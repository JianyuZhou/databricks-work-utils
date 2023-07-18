#!/usr/bin/env python
from argparse import ArgumentParser
import json, os, sys, subprocess
import pandas as pd

# find all contexts
def run_cmd(cmd_str, split_time = -1, timeout = 30):
    try:
        stdout = subprocess.run(cmd_str.split(' ', split_time), capture_output = True, timeout = timeout).stdout.decode('utf-8', errors='replace')
    except Exception as e:
        print(f"encounter exception when running command: {cmd_str}, error: {e}")
        raise e
    return stdout
    
# find manage-catalog pod name under a specific kubernetes context and namespace
def get_managed_catalog_pod_name(context, namespace):
    pod_name = None
    try:
        result_str = run_cmd(f'bin/kubecfg get pods --context {context} --namespace {namespace}')
        # print(f'cd {os.path.expanduser("~")}/universe && bin/kubecfg get pods --context {context} --namespace {namespace}')
        records = result_str.split('\n')
        pods = [record for record in records if record.find('managed-catalog') != -1]
        if len(pods) > 0:
            pod_name = pods[0].split()[0] # return the first managed-catalog pod is enough as only need to go in to one pod to run sql query, other pods are just backup
    except Exception as e:
        print(f"unable to get pod name in {context}/{namespace}")
    finally:
        return pod_name


def get_all_contexts(env = 'dev'):
    result_str = run_cmd("kubectl config get-contexts")
    records = result_str.split('\n')
    return [record.split()[0] for record in records if record.find(env) != -1]

def run_sql_in_pod(sql_str, context, namespace, pod_name):
    result = None
    try:
        cmd_str = f"kubectl exec -it --context={context} --namespace={namespace} {pod_name} -- ./scripts/mysql_console -e {sql_str}"
        result = run_cmd(cmd_str, 9)
    except Exception as e:
        print(f"encounter error when running cmd: {cmd_str}")
    return result

def convert_sql_result_to_df(sql_result:str):
    '''
    example sql query result returned is a tabular string like:
    +----------------+------+----------------------------------+-------------------------+-------------------------+
    | current_date() | name | hex(metastore_id)                | created_at              | updated_at              |
    +----------------+------+----------------------------------+-------------------------+-------------------------+
    | 2023-06-03     | main | C75570844B1346BEB4F06710A14DF4DF | 2022-05-07 07:25:08.095 | 2022-05-07 07:25:08.095 |
    | 2023-06-03     | main | C81987DAC18B43ECB69F5927B81380B0 | 2022-05-07 07:25:37.371 | 2022-05-07 07:25:37.371 |
    | 2023-06-03     | main | D8F8DA540C1943D7BA83C7BB6440B610 | 2022-05-07 07:24:31.487 | 2022-05-07 07:24:31.487 |
    | 2023-06-03     | main | C326D827A8AB439891CFC68DBE8610C5 | 2022-04-08 00:11:46.650 | 2022-04-08 00:11:46.650 |
    | 2023-06-03     | main | F6431E4D00964BEBA76945A69738681E | 2022-04-08 00:12:19.283 | 2022-04-08 00:12:19.283 |
    | 2023-06-03     | main | F95DF05B25C74374BBA1280C596CB7F3 | 2022-04-08 00:12:35.203 | 2022-04-08 00:12:35.203 |
    | 2023-06-03     | main | 07E958DDC08C46C6A9BB593E838EFE7C | 2023-01-14 02:20:31.418 | 2023-01-14 02:20:31.418 |
    | 2023-06-03     | main | 4CE47E47BA6D403288C55E9E7FED80D5 | 2023-01-18 20:16:58.272 | 2023-01-18 20:16:58.272 |
    | 2023-06-03     | main | 6514DFC8D3D4447F9D78F4BF457479AC | 2022-05-15 07:54:22.654 | 2022-05-15 07:54:22.654 |
    | 2023-06-03     | main | 82C4940829914A3DA6ACB268336A25F5 | 2022-05-27 00:05:54.642 | 2022-05-27 00:05:54.642 |
    +----------------+------+----------------------------------+-------------------------+-------------------------+
    '''
    try:
        lines = sql_result.strip().splitlines()
        # Filter out any lines that start with '+'.
        lines = [line.strip('|') for line in lines if line.startswith('|')]
        # Split each line into columns and strip leading/trailing spaces.
        data = [line.split('|') for line in lines]
        # The first line should be the column headers.
        columns = [col.strip() for col in data[0]]
        # The rest of the lines should be the data.
        data = [[col.strip() for col in row] for row in data[1:]]
        # Create a DataFrame.
        df = pd.DataFrame(data, columns=columns)
    except Exception as e:
        raise Exception(f"sql_result: {sql_result}, error: {e}")
    return df

if __name__ == "__main__":
    parser = ArgumentParser(usage="qatools")
    parser.add_argument(
        "-n",
        "--namespace",
        default = "managed-catalog", # this is the prod namespace name for UC, replace it with your own test shard name if you want to run query in you test shard
        help = "optional, kubernetes namespace/shard name")

    parser.add_argument(
        "-c",
        "--context",
        default = "dev-aws-us-west-2",
        help = "either a specific kubernetes context name, or a context prefix, if it's prefix, it will run the sql in all contexts with this prefix"
    )
    parser.add_argument(
        "-s",
        "--sql_file",
        help = "the sql file name that contains sql query want to run"
    )
    parser.add_argument(
        "-o",
        "--output",
        default = os.path.join(os.path.expanduser('~'), "query_result.csv"),
        help = "the output file name, should not exist"
    )

    args = parser.parse_args()
    if not args.sql_file:
        print("sql_file must be provided")
        sys.exit()

    # convert all path variables to absolute path
    args.sql_file = os.path.abspath(args.sql_file)
    args.output = os.path.abspath(args.output)

    # change dir to ~/universe as bin/kubecfg has to be run at this folder
    os.chdir(f"{os.path.expanduser('~')}/universe")
    # read sql script
    with open(args.sql_file, 'r') as fp:
        sql_str = fp.read()

    contexts = get_all_contexts(args.context)

    for context in contexts:
        print(f"running sql query in {args.sql_file} in {context}")
        pod_name = get_managed_catalog_pod_name(context, args.namespace)
        if pod_name:
            print(f"entering pod {pod_name}")
            result = run_sql_in_pod(sql_str, context, args.namespace, pod_name)
            if result:
                try:
                    df = convert_sql_result_to_df(result)
                    df['context'] = context
                    print(f"writing query result to {args.output}")
                    if os.path.isfile(args.output):
                        df.to_csv(args.output, mode='a', header=False, index=False)
                    else:
                        df.to_csv(args.output, index=False)
                except Exception as e:
                    print(e)
    print("Done!")
