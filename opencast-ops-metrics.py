#!/usr/bin/env python

import re
import csv
import boto3
import shlex
import click
import arrow
from syslog import syslog, openlog, LOG_ERR
import jmespath
from urllib.parse import urlparse, urljoin
from io import StringIO
import requests
from requests.auth import HTTPDigestAuth
from datetime import datetime
from subprocess import Popen, PIPE, TimeoutExpired
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
@click.pass_obj
def cli(ctx, profile):

    if profile is not None:
        boto3.setup_default_session(profile_name=profile)
        syslog("setting aws profile to {}".format(profile))

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
    ctx["put_metric_data"] = put_metric_data


@cli.group()
def ops():
    pass


@ops.command(name='running')
@click.option('--stack-name')
@click.option('--metric-namespace', default='Opencast')
@click.pass_obj
def running_ops(ctx, stack_name, metric_namespace):

    syslog("publishing running operation metrics for '{}'".format(stack_name))

    sql = get_running_ops_query()
    op_data = exec_query(sql)

    # maps private dns => node name
    worker_hosts = get_worker_host_map(stack_name)["all"]

    metric_data = []
    for op in op_data:
        op_type = op['operation']
        host = urlparse(op['host']).netloc
        count = int(op['cnt'])
        node_name = worker_hosts.get(host, 'unknown')

        dimensions = [
            { "Name": "OperationType", "Value": op_type },
            #{ "Name": "ProcessingHost", "Value": node_name },
            { "Name": "OpsworksStack", "Value": stack_name }
        ]

        dp = {
            "MetricName": "RunningOperations",
            "Value": count,
            "Unit": "Count",
            "Timestamp": arrow.utcnow().timestamp,
            "Dimensions": dimensions
        }
        metric_data.append(dp)

        if len(metric_data) >= 10:
            ctx["put_metric_data"](metric_namespace, metric_data)
            metric_data = []

    if len(metric_data):
        ctx["put_metric_data"](metric_namespace, metric_data)



@ops.command(name='runtimes')
@click.option('--stack-name')
@click.option('--interval-seconds', default=120)
@click.option('--metric-namespace', default='Opencast')
@click.pass_obj
def op_runtimes(ctx, stack_name, interval_seconds, metric_namespace):

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
            ctx["put_metric_data"](metric_namespace, metric_data)
            metric_data = []

    if len(metric_data):
        ctx["put_metric_data"](metric_namespace, metric_data)


@cli.group()
def job_load():
    pass


@job_load.command(name='used')
@click.option('--stack-name')
@click.option('--admin-host')
@click.option('--api-user')
@click.option('--api-pass')
@click.option('--metric-namespace', default='Opencast')
@click.pass_obj
def percent_used(ctx, stack_name, admin_host, api_user, api_pass,
                 metric_namespace):

    stack_id = get_stack_id(stack_name)
    syslog("Calculating percent job_load usage for '{}'".format(stack_name))

    worker_hosts = get_worker_host_map(stack_name)
    running_worker_hosts = worker_hosts["running"]
    all_worker_hosts = worker_hosts["all"]
    layer_id = get_workers_layer_id(stack_id)

    syslog("{} workers running".format(len(running_worker_hosts)))

    if not len(running_worker_hosts):
        syslog("No workers running!?")

    digest_auth = get_digest_auth(api_user, api_pass)

    max_loads = get_load_factors('services/maxload', admin_host, digest_auth)
    max_loads_running = {x: y for x, y in max_loads.items() if x in running_worker_hosts}
    max_loads_all = {x: y for x, y in max_loads.items() if x in all_worker_hosts}

    current_loads = get_load_factors('services/currentload', admin_host, digest_auth)
    current_loads = {x: y for x, y in current_loads.items() if x in running_worker_hosts}

    running_max_load = sum(max_loads_running.values())
    total_max_load = sum(max_loads_all.values())
    current_load = sum(current_loads.values())

    job_load_pct = round( (current_load / running_max_load) * 100, 2)
    job_load_total_pct = round( (current_load / total_max_load) * 100, 2)

    syslog(
        "WorkersJobLoadPercentUsed: current: {}, max_running: {}, max_total: "
        "{}, pct_running: {}, pct_total: {}" \
            .format(current_load,
                    running_max_load,
                    total_max_load,
                    job_load_pct,
                    job_load_total_pct))

    dimensions = [{ "Name": "OpsworksStack", "Value": stack_name }]
    metric_data = [
        {
            "MetricName": "WorkersJobLoadPercentUsed",
            "Value": job_load_pct,
            "Unit": 'Percent',
            "Dimensions": dimensions
        },
        {
            "MetricName": "WorkersJobLoadPercentTotalUsed",
            "Value": job_load_total_pct,
            "Unit": 'Percent',
            "Dimensions": dimensions
        }
    ]
    ctx["put_metric_data"](metric_namespace, metric_data)


@job_load.command()
@click.option('--stack-name')
@click.option('--admin-host')
@click.option('--api-user')
@click.option('--api-pass')
@click.option('--metric-namespace', default='Opencast')
@click.pass_obj
def max_available(ctx, stack_name, admin_host, api_user, api_pass,
                  metric_namespace):

    stack_id = get_stack_id(stack_name)
    syslog("Calculating max available load for '{}'".format(stack_name))

    worker_hosts = get_worker_host_map(stack_name)
    running_worker_hosts = worker_hosts["running"]
    layer_id = get_workers_layer_id(stack_id)

    syslog("{} workers running".format(len(running_worker_hosts)))

    if not len(running_worker_hosts):
        syslog("No workers running!?")

    digest_auth = get_digest_auth(api_user, api_pass)

    max_loads = get_load_factors('services/maxload', admin_host, digest_auth)
    max_loads = {x: y for x, y in max_loads.items() if x in running_worker_hosts}

    current_loads = get_load_factors('services/currentload', admin_host, digest_auth)
    current_loads = {x: y for x, y in current_loads.items() if x in running_worker_hosts}

    # get a tuple of each host's max/current load
    available_loads = {
        x: max_loads[x] - current_loads[x] for x in max_loads.keys()
    }
    syslog("available loads: {}".format(available_loads))

    # sort by the difference descending and take the first one
    current_max = sorted(available_loads.values(), reverse=True)[0]
    syslog("Max available job_load: {}".format(current_max))

    dimensions = [{ "Name": "OpsworksStack", "Value": stack_name }]
    ctx["put_metric_data"](metric_namespace, [
        {
            "MetricName": "WorkersJobLoadMaxAavailable",
            "Value": current_max,
            "Unit": 'Percent',
            "Dimensions": dimensions
        }
    ])


@cli.group()
def workflows():
    pass


@workflows.command(name='running')
@click.option('--stack-name')
@click.option('--admin-host')
@click.option('--api-user')
@click.option('--api-pass')
@click.option('--metric-namespace', default='Opencast')
@click.pass_obj
def running_workflows(ctx, stack_name, admin_host, api_user, api_pass,
                      metric_namespace):

    stack_id = get_stack_id(stack_name)
    digest_auth = get_digest_auth(api_user, api_pass)
    syslog("Fetching count of running workflows")

    endpoint_url = urljoin("http://" + admin_host, 'admin-ng/job/tasks.json')
    params = {'limit': 999, 'status': 'RUNNING'}
    headers = {'X-REQUESTED-AUTH': 'Digest'}
    resp = requests.get(endpoint_url, params=params, auth=digest_auth,
                        headers=headers)
    tasks = resp.json()
    workflow_count = tasks["count"]

    syslog("Running workflows: {}".format(workflow_count))

    dimensions = [{ "Name": "OpsworksStack", "Value": stack_name }]
    ctx["put_metric_data"](metric_namespace, [
        {
            "MetricName": "RunningWorkflows",
            "Value": workflow_count,
            "Unit": 'Count',
            "Dimensions": dimensions
        }
    ])


@workflows.command(name='runtimes')
@click.option('--stack-name')
@click.option('--admin-host')
@click.option('--api-user')
@click.option('--api-pass')
@click.option('--end-date')
@click.option('--days-interval', default=1)
@click.option('--metric-namespace', default='Opencast')
@click.option('--mp')
@click.pass_obj
def workflow_runtimes(ctx, stack_name, admin_host, api_user, api_pass,
                      end_date, days_interval, metric_namespace, mp):

    stack_id = get_stack_id(stack_name)
    digest_auth = get_digest_auth(api_user, api_pass)

    if end_date is None:
        end_date = arrow.now()
    else:
        end_date = arrow.get(end_date)

    start_date = end_date.replace(days=-days_interval)

    syslog("publishing workflow runtime metrics")

    endpoint_url = urljoin("http://" + admin_host, 'workflow/instances.json')
    headers = {'X-REQUESTED-AUTH': 'Digest'}

    params = {
        'count': 20,
        'startPage': 0,
        # opencast is picky af about these date strings smh
        'fromdate': start_date.format('YYYY-MM-DDTHH:mm:ss') + "Z",
        'todate': end_date.format('YYYY-MM-DDTHH:mm:ss') + "Z",
        'state': 'SUCCEEDED'
    }

    if mp is not None:
        params["mp"] = mp

    workflows = []
    while True:
        resp = requests.get(endpoint_url, params=params, auth=digest_auth,
                            headers=headers)
        wf_data = resp.json()["workflows"]
        if "workflow" not in wf_data:
            break
        try:
            # "workflow" will be an array if > 1
            workflows.extend(wf_data["workflow"])
        except TypeError:
            # or a single object if there's only 1
            workflows.append(wf_data["workflow"])
        syslog("Collected {} workflows".format(len(workflows)))
        params["startPage"] += 1

    metric_data = []
    for wf in workflows:
        ops = wf["operations"]["operation"]
        wf_type = wf["template"]
        track_duration = get_track_duration_from_wf(wf)
        wf_duration, wf_completed_ts = get_wf_duration_completed(ops)
        wf_completed = arrow.get(wf_completed_ts).replace(tzinfo='US/Eastern')

        dimensions = [
            { "Name": "WorkflowType", "Value": wf_type },
            { "Name": "OpsworksStack", "Value": stack_name }
        ]

        dp = {
            "MetricName": "WorkflowDuration",
            "Value": wf_duration,
            "Unit": "Count",
            "Timestamp": wf_completed.to('UTC').timestamp,
            "Dimensions": dimensions
        }
        metric_data.append(dp)

        if track_duration is not None:
            perf_ratio = round(wf_duration / track_duration, 2)

            dp = {
                "MetricName": "WorkflowPerfRatio",
                "Value": perf_ratio,
                "Unit": "None",
                "Timestamp": wf_completed.to('UTC').timestamp,
                "Dimensions": dimensions
            }
            metric_data.append(dp)

        if len(metric_data) >= 10:
            ctx["put_metric_data"](metric_namespace, metric_data)
            metric_data = []

    if len(metric_data):
        ctx["put_metric_data"](metric_namespace, metric_data)


def get_track_duration_from_wf(wf):
    try:
        pubs = wf["mediapackage"]["publications"]["publication"]
    except (KeyError, TypeError):
        return None
    if isinstance(pubs, dict):
        pubs = [pubs]
    for pub in pubs:
        media = pub["media"]
        if not media:
            continue
        tracks = media["track"]
        if isinstance(tracks, dict):
            tracks = [tracks]
        for track in tracks:
            if "duration" in track:
                return track["duration"] / 1000
    return None


def get_wf_duration_completed(ops):
    wf_start = ops[0]["started"] / 1000
    wf_end = ops[-1]["completed"] / 1000
    wf_duration = (wf_end - wf_start)
    return wf_duration, wf_end


def get_stack_id(stack_name):
    opsworks = boto3.client('opsworks')
    stacks = opsworks.describe_stacks()
    jp_query = "Stacks[?Name=='{}'] | [0].StackId".format(stack_name)
    stack_id = jmespath.search(jp_query, stacks)
    return stack_id


def get_digest_auth(api_user, api_pass):
    return HTTPDigestAuth(api_user, api_pass)


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


def get_worker_host_map(stack_name):

    ec2 = boto3.client('ec2')
    instances = ec2.describe_instances(
        Filters=[{'Name': 'tag:opsworks:stack', 'Values': [stack_name]}]
    )
    jp_query = ("Reservations[].Instances[].[PrivateDnsName, State.Name, "
                "Tags[?Key=='opsworks:instance'] | [0].Value]")
    private_dns_to_hostname = jmespath.search(jp_query, instances)

    worker_hosts = [
        x for x in private_dns_to_hostname
        if x[2].startswith("workers")
    ]

    running_hosts = [
        x for x in worker_hosts
        if x[1] == "running"
    ]

    return {
        "running": dict([(x[0], x[2]) for x in running_hosts]),
        "all": dict([(x[0], x[2]) for x in worker_hosts]),
    }


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


def get_running_ops_query():
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
    try:
        (stdout, stderr) = subproc.communicate(timeout=5)
    except TimeoutExpired as e:
        syslog(LOG_ERR, str(e))
        raise
    err = stderr.decode("utf-8")
    if err:
        syslog(LOG_ERR, err)
        raise RuntimeError(err)
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
