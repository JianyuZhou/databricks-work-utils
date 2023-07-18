#!/usr/bin/env python
import os
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from mysql_orchestrator import get_all_contexts, run_cmd

def get_kube_access(context, write = False):
    if write:
        run_cmd(f"bin/get-kube-access {context} -write", timeout = None)
    else:
        run_cmd(f"bin/get-kube-access {context}", timeout = None)

if __name__ == "__main__":
    parser = ArgumentParser(usage="qatools")
    parser.add_argument(
        "-c",
        "--context",
        default = "dev-aws-us-west-2",
        help = "either a specific kubernetes context name, or a context prefix, if it's prefix, it will run the sql in all contexts with this prefix"
    )
    parser.add_argument(
        "-w",
        "--write",
        action='store_true',
        help = "get write access for the kube context"
    )
    args = parser.parse_args()
    os.chdir(f"{os.path.expanduser('~')}/universe")
    
contexts = get_all_contexts(args.context)
futures = []
with ThreadPoolExecutor() as executor:
    for context in contexts:
        print(context)
        future = executor.submit(get_kube_access, context, args.write)
        futures.append(future)