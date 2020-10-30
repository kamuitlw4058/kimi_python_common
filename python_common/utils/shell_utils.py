__author__ = 'kimi'
__email__ = '199524943@qq.com'


import os

import subprocess
import shutil

from python_common.utils.logger import getLogger

logger = getLogger(__name__)

def run_cli(cmd,env=None,err_fail=False):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8',env=env)
    out, err = proc.communicate()
    if proc.returncode != 0 or (err_fail and err):
        raise RuntimeError(f'{cmd}\nreturn code = {proc.returncode}\nstderr = {err}')
    return out

def __handler_log(log_line, log_handler,verbose=False):
    log_line = log_line.decode().strip()
    if log_handler is not None:
        log_handler(log_line)
    if verbose:
        logger.info(log_line)
    return log_line


def execute(shell_cmd, verbose=False,stderr2stdout=True, log_handler = None):
    logs = []
    stdout = subprocess.PIPE
    stderr = subprocess.STDOUT

    if not stderr2stdout:
        stderr =   subprocess.PIPE
    p = subprocess.Popen(shell_cmd, shell=True, stdout=stdout, stderr=stderr)
    status = None
    while status is None:
        status = p.poll()
        if not stderr2stdout:
            log_line = p.stderr.readline()
            handled_line = __handler_log(log_line, log_handler, verbose)
            logs.append(handled_line)

        log_line = p.stdout.readline()
        handled_line = __handler_log(log_line, log_handler,verbose)
        logs.append(handled_line)

    if not stderr2stdout:
        log_lines = p.stderr.readlines()
        for log_line in log_lines:
            handled_line = __handler_log(log_line, log_handler,verbose)
            logs.append(handled_line)


    log_lines = p.stdout.readlines()
    for log_line in log_lines:
        handled_line = __handler_log(log_line, log_handler,verbose)
        logs.append(handled_line)

    logs_str = ""
    for log in logs:
        logs_str += log + "\n"
    return status, logs_str


def cat(file_path, verbose=False):
    with open(file_path, 'r') as f:
        logs = f.read()
        if verbose:
            logger.info(logs)
    return logs


def write(file_path, context):
    with open(file_path, 'w') as f:
        f.write(context)
