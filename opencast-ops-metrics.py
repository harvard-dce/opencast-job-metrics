
import re
import csv
import boto3
import shlex
import click
import arrow
from syslog import syslog, openlog
import jmespath
from urllib.parse import urlparse
from io import StringIO
from datetime import datetime
from subprocess import Popen, PIPE
import xml.etree.ElementTree as ET

# prefix syslog messages
openlog("opencast-ops-metrics")

PAYLOAD_XML_NS = {'track': 'http://mediapackage.opencastproject.org'}
OC_JOB_STATUS_RUNNING = 2


@click.group()
@click.option('--profile')
@click.pass_context
def cli(ctx, profile):
    if profile is not None:
        boto3.setup_default_session(profile_name=profile)
        syslog("setting aws profile to {}".format(profile))

    cloudwatch = boto3.client('cloudwatch')
    def put_metric_data(namespace, metric_data):
        syslog("Sending {} metric data points to {}".format(
            len(metric_data), namespace
        ))
#        cloudwatch.put_metric_data(
#            Namespace=namespace,
#            MetricData=metric_data
#        )
    ctx.obj["put_metric_data"] = put_metric_data


@cli.group()
def ops():
    pass


@cli.group()
def workflows():
    pass


@ops.command(name='active')
@click.argument('stack_name')
@click.option('--metric-namespace', default='Opencast')
@click.pass_context
def active_ops(ctx, stack_name, metric_namespace):

    syslog("publishing active operation metrics for '{}'".format(stack_name))

    sql = get_active_ops_query()
    op_data = exec_query(sql)
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

        metric_data.append({
            "MetricName": "ActiveJobs",
            "Value": count,
            "Unit": "Count",
            "Timestamp": arrow.utcnow().datetime,
            "Dimensions": dimensions
        })

        if len(metric_data) >= 10:
            ctx.obj["put_metric_data"](metric_namespace, metric_data)
            metric_data = []

    if len(metric_data):
        ctx.obj["put_metric_data"](metric_namespace, metric_data)



@ops.command(name='runtimes')
@click.argument('stack_name')
@click.option('--interval-seconds', default=120)
@click.option('--metric-namespace', default='Opencast')
@click.pass_context
def op_runtimes(ctx, stack_name, interval_seconds, metric_namespace):

    syslog("publishing operation runtime metrics for past {} seconds for '{}'" \
           .format(interval_seconds, stack_name))

    sql = get_runtimes_query(interval_seconds)
    op_data = exec_query(sql)

    metric_data = []
    for op in op_data:

        created = arrow.get(op['date_created'])
        started = arrow.get(op['date_started'])
        completed = arrow.get(op['date_completed'])

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
            "Timestamp": completed.to('UTC').datetime,
            "Dimensions": dimensions
        })

        if payload.startswith('<?xml'):
            duration = extract_duration_from_payload(payload)
            if duration:
                perf_ratio = round(runtime / duration, 2)

                metric_data.append({
                    "MetricName": "OperationPerfRatio",
                    "Value": perf_ratio,
                    "Unit": 'None',
                    "Timestamp": completed.to('UTC').datetime,
                    "Dimensions": dimensions
                })

        if len(metric_data) >= 10:
            ctx.obj["put_metric_data"](metric_namespace, metric_data)
            metric_data = []

    if len(metric_data):
        ctx.obj["put_metric_data"](metric_namespace, metric_data)


def get_host_map(stack_name):

    ec2 = boto3.client('ec2')
    instances = ec2.describe_instances(
        Filters=[{'Name': 'tag:opsworks:stack', 'Values': [stack_name]}]
    )
    jp_query = ("Reservations[].Instances[].[PrivateDnsName, "
                "Tags[?Key=='opsworks:instance'] | [0].Value]")
    hostnames_vs_names = jmespath.search(jp_query, instances)
    return dict(hostnames_vs_names)


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
        # some payloads have two (?!?) xml docs separated by '###';
        # duration should be the same, so just take the first one
        for doc in payload.split('###'):
            root = ET.fromstring(doc.encode())
            duration = root.find('.//track:duration', namespaces=PAYLOAD_XML_NS)
            if hasattr(duration, 'text'):
                return int(duration.text) / 1000 # orig value is in millisec
        return None
    except Exception as e:
        print("something went wrong: {}, {}".format(e, payload))


if __name__ == '__main__':
    cli(obj={})
