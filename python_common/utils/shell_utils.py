__author__ = 'kimi'
__email__ = '199524943@qq.com'



import os

import subprocess
import shutil



def run_cli(cmd,env=None,err_fail=False):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8',env=env)
    out, err = proc.communicate()
    if proc.returncode != 0 or (err_fail and err):
        raise RuntimeError(f'{cmd}\nreturn code = {proc.returncode}\nstderr = {err}')
    return out