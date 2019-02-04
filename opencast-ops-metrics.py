
import re
import csv
import boto3
import shlex
import click
import arrow
from syslog import syslog, openlog
import jmespath
from urllib.parse import urlparse, urljoin
from io import StringIO
import requests
from requests.auth import HTTPDigestAuth
from datetime import datetime
from subprocess import Popen, PIPE
import xml.etree.ElementTree as ET

# prefix syslog messages
openlog("opencast-ops-metrics")

XML_NAMESPACES = {
    'track': 'http://mediapackage.opencastproject.org',
    'oc': 'http://serviceregistry.opencastproject.org'
}
OC_JOB_STATUS_RUNNING = 2


@click.group()
@click.option('--profile')
@click.option('--stack-id')
@click.pass_context
def cli(ctx, profile, stack_id):

    if profile is not None:
        boto3.setup_default_session(profile_name=profile)
        syslog("setting aws profile to {}".format(profile))

    if stack_id is None:
        raise click.UsageError("--stack-id is required")

    opsworks = boto3.client('opsworks')
    stacks = opsworks.describe_stacks(StackIds=[stack_id])
    stack_name = stacks["Stacks"][0]["Name"]

    ctx.obj["stack_id"] = stack_id
    ctx.obj["stack_name"] = stack_name

    cloudwatch = boto3.client('cloudwatch')
    def put_metric_data(namespace, metric_data):
        syslog("Sending {} metric data points to {}".format(
            len(metric_data), namespace
        ))
#        resp = cloudwatch.put_metric_data(
#            Namespace=namespace,
#            MetricData=metric_data
#        )
        return
    ctx.obj["put_metric_data"] = put_metric_data


@cli.group()
def ops():
    pass


@ops.command(name='active')
@click.option('--metric-namespace', default='Opencast')
@click.pass_context
def active_ops(ctx, metric_namespace):

    stack_name = ctx.obj["stack_name"]
    syslog("publishing active operation metrics for '{}'".format(stack_name))

    sql = get_active_ops_query()
    op_data = exec_query(sql)

    # maps private dns => node name
    host_map = get_host_map(stack_name)

    metric_data = []
    for op in op_data:
        op_type = op['operation']
        host = urlparse(op['host']).netloc
        count = int(op['cnt'])
        node_name = host_map.get(host, 'unknown')

        dimensions = [
            { "Name": "OperationType", "Value": op_type },
            { "Name": "ProcessingHost", "Value": node_name },
            { "Name": "OpsworksStack", "Value": stack_name }
        ]

        dp = {
            "MetricName": "ActiveJobs",
            "Value": count,
            "Unit": "Count",
            "Timestamp": arrow.utcnow().timestamp,
            "Dimensions": dimensions
        }
        metric_data.append(dp)

        if len(metric_data) >= 10:
            ctx.obj["put_metric_data"](metric_namespace, metric_data)
            metric_data = []

    if len(metric_data):
        ctx.obj["put_metric_data"](metric_namespace, metric_data)



@ops.command(name='runtimes')
@click.option('--interval-seconds', default=120)
@click.option('--metric-namespace', default='Opencast')
@click.pass_context
def op_runtimes(ctx, interval_seconds, metric_namespace):

    stack_name = ctx.obj["stack_name"]
    syslog("publishing operation runtime metrics for past {} seconds for '{}'" \
           .format(interval_seconds, stack_name))

    sql = get_runtimes_query(interval_seconds)
    op_data = exec_query(sql)

    metric_data = []
    for op in op_data:

        created = arrow.get(op['date_created']).replace(tzinfo='US/Eastern')
        started = arrow.get(op['date_started']).replace(tzinfo='US/Eastern')
        completed = arrow.get(op['date_completed']).replace(tzinfo='US/Eastern')

        runtime = (completed - started).seconds
        queuetime = (started - created).seconds

        op_type = op['operation']
        payload = op['payload']

        dimensions = [
            { "Name": "OperationType", "Value": op_type },
            { "Name": "OpsworksStack", "Value": stack_name }
        ]

        metric_data.append({
            "MetricName": "OperationRuntime",
            "Value": runtime,
            "Unit": "Seconds",
            "Timestamp": completed.to('UTC').timestamp,
            "Dimensions": dimensions
        })

        if payload.startswith('<?xml'):
            duration = extract_duration_from_payload(payload)
            if duration:
                perf_ratio = round(runtime / duration, 2)

                dp = {
                    "MetricName": "OperationPerfRatio",
                    "Value": perf_ratio,
                    "Unit": 'None',
                    "Timestamp": completed.to('UTC').timestamp,
                    "Dimensions": dimensions
                }
                metric_data.append(dp)

        if len(metric_data) >= 10:
            ctx.obj["put_metric_data"](metric_namespace, metric_data)
            metric_data = []

    if len(metric_data):
        ctx.obj["put_metric_data"](metric_namespace, metric_data)


@cli.group()
def workflows():
    pass


@cli.group()
def job_load():
    pass


@job_load.command(name='used')
@click.option('--admin-host')
@click.option('--api-user')
@click.option('--api-pass')
@click.pass_context
def percent_used(ctx, admin_host, api_user, api_pass):

    stack_id = ctx.obj["stack_id"]
    stack_name = ctx.obj["stack_name"]
    syslog("Calculating percent job_load usage for '{}'".format(stack_name))

    workers_host_map = get_host_map(stack_name, state="running", hostname_prefix="workers")
    layer_id = get_workers_layer_id(stack_id)

    syslog("{} workers running".format(len(workers_host_map)))

    if not len(workers_host_map):
        syslog("No workers running!?")

    auth = HTTPDigestAuth(api_user, api_pass)

    max_loads = get_load_factors('services/maxload', admin_host, auth)
    max_loads = {x: y for x, y in max_loads.items() if x in workers_host_map}

    current_loads = get_load_factors('services/currentload', admin_host, auth)
    current_loads = {x: y for x, y in current_loads.items() if x in workers_host_map}

    max_load = sum(max_loads.values())
    current_load = sum(current_loads.values())

    job_load_pct = round( (current_load / max_load) * 100, 2)
    syslog(
        "WorkersJobLoadPercentUsed: current: {}, max: {}, pct: {}" \
            .format(current_load, max_load, job_load_pct))

    ctx.obj["put_metric_data"]('AWS/OpsworksCustom', [
        {
            "MetricName": "WorkersJobLoadPercentUsed",
            "Value": job_load_pct,
            "Unit": 'Percent',
            "Dimensions": [
                {
                    "Name": "LayerId",
                    "Value": layer_id
                }
            ]

        }
    ])

@job_load.command()
@click.option('--admin-host')
@click.option('--api-user')
@click.option('--api-pass')
@click.pass_context
def max_available(ctx, admin_host, api_user, api_pass):

    stack_id = ctx.obj["stack_id"]
    stack_name = ctx.obj["stack_name"]
    syslog("Calculating max available load for '{}'".format(stack_name))

    workers_host_map = get_host_map(stack_name, state="running", hostname_prefix="workers")
    layer_id = get_workers_layer_id(stack_id)

    syslog("{} workers running".format(len(workers_host_map)))

    if not len(workers_host_map):
        syslog("No workers running!?")

    auth = HTTPDigestAuth(api_user, api_pass)

    max_loads = get_load_factors('services/maxload', admin_host, auth)
    max_loads = {x: y for x, y in max_loads.items() if x in workers_host_map}

    current_loads = get_load_factors('services/currentload', admin_host, auth)
    current_loads = {x: y for x, y in current_loads.items() if x in workers_host_map}


    # get a tuple of each host's max/current load
    available_loads = {
        x: max_loads[x] - current_loads[x] for x in max_loads.keys()
    }
    syslog("available loads: {}".format(available_loads))

    # sort by the difference descending and take the first one
    current_max = sorted(available_loads.values(), reverse=True)[0]
    syslog("Max available job_load: {}".format(current_max))

    ctx.obj["put_metric_data"]('AWS/OpsworksCustom', [
        {
            "MetricName": "WorkersJobLoadMaxAavailable",
            "Value": current_max,
            "Unit": 'Percent',
            "Dimensions": [
                {
                    "Name": "LayerId",
                    "Value": layer_id
                }
            ]

        }
    ])


def get_workers_layer_id(stack_id):

    opsworks = boto3.client('opsworks')
    layers = opsworks.describe_layers(StackId=stack_id)
    workers_layer = next(x for x in layers["Layers"] if x["Name"] == "Workers")
    return workers_layer["LayerId"]


def get_load_factors(endpoint, admin_host, auth):
    endpoint_url = urljoin("http://" + admin_host, endpoint)
    headers = {'X-REQUESTED-AUTH': 'Digest'}
    resp = requests.get(endpoint_url, auth=auth, headers=headers)
    xml_root = ET.fromstring(resp.content)
    nodes = xml_root.findall('.//oc:node', namespaces=XML_NAMESPACES)
    load_factors = {}
    for node in nodes:
        host = urlparse(node.attrib["host"]).netloc
        load_factors[host] = float(node.attrib['loadFactor'])
    return load_factors


def get_host_map(stack_name, state=None, hostname_prefix=None):

    ec2 = boto3.client('ec2')
    instances = ec2.describe_instances(
        Filters=[{'Name': 'tag:opsworks:stack', 'Values': [stack_name]}]
    )
    jp_query = ("Reservations[].Instances[].[PrivateDnsName, State.Name, "
                "Tags[?Key=='opsworks:instance'] | [0].Value]")
    private_dns_to_hostname = jmespath.search(jp_query, instances)

    if state is not None:
        private_dns_to_hostname = [
            x for x in private_dns_to_hostname
            if x[1] == state
        ]

    if hostname_prefix is not None:
        private_dns_to_hostname = [
            x for x in private_dns_to_hostname
            if x[2].startswith(hostname_prefix)
        ]

    return dict([(x[0], x[2]) for x in private_dns_to_hostname])


def get_runtimes_query(interval_seconds):

    now = datetime.now().isoformat()
    query = """
        SELECT
            id, status, payload, operation, date_created, 
            date_started, date_completed
        FROM oc_job 
        WHERE
            date_completed BETWEEN DATE_SUB("{}", INTERVAL {} SECOND) AND "{}" 
            AND operation NOT LIKE "START_%"
        """.format(now, interval_seconds, now)

    return re.compile(r'\s+').sub(' ', query.strip())


def get_active_ops_query():
    query = """
        SELECT
            ocj.operation, ochr.host, count(*) as cnt
        FROM oc_job ocj
        JOIN oc_service_registration ocsr ON ocsr.id = ocj.processor_service
        JOIN oc_host_registration ochr ON ochr.id = ocsr.host_registration
        WHERE
            ocj.status = {}
            AND ocj.operation NOT LIKE "START_%"
        GROUP BY
            ocj.operation,
            ochr.host
    """.format(OC_JOB_STATUS_RUNNING)

    return re.compile(r'\s+').sub(' ', query.strip())


def exec_query(sql):

    cmd = "/usr/bin/mysql -B -e '{}' {}".format(sql, "opencast")
    split_cmd = shlex.split(cmd)
    subproc = Popen(split_cmd, stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = subproc.communicate()
    err = stderr.decode("utf-8")
    tsv_data = StringIO(stdout.decode("utf-8"))
    return csv.DictReader(tsv_data, delimiter="\t")


def extract_duration_from_payload(payload):
    try:
        # some payloads have two (?!--stack-id 2e8db1c3-5bad-4f91-9a2b-905e36617be8 ?) xml docs separated by '###';
        # duration should be the same, so just take the first one
        for doc in payload.split('###'):
            root = ET.fromstring(doc.encode())
            duration = root.find('.//track:duration', namespaces=XML_NAMESPACES)
            if hasattr(duration, 'text'):
                return int(duration.text) / 1000 # orig value is in millisec
        return None
    except Exception as e:
        print("something went wrong: {}, {}".format(e, payload))


if __name__ == '__main__':
    cli(obj={})
