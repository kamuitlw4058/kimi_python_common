#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'kimi'
__email__ = '199524943@qq.com'

import os
import shutil

from ..utils.shell_utils import run_cli

class HDFS:
    
    def __init__(self,env=None):
        self.env = env

    def ls(self, hdfs_dir):
        try:
            cmd = f'hadoop fs -ls {hdfs_dir}'
            out = run_cli(cmd,self.env)
            res = []
            for l in out.split('\n'):
                n = l.split(' ')[-1]
                if n.startswith(hdfs_dir):
                    res.append(n)
            return res
            # return self._fs.ls(hdfs_dir)
        except Exception as e:
            return []

    def get_dir(self, src_hdfs_dir, dst_local_dir):
        if os.path.exists(dst_local_dir):
            shutil.rmtree(dst_local_dir)
        os.makedirs(dst_local_dir)

        for hdfs_file in self.ls(src_hdfs_dir):
            local_file = os.path.join(dst_local_dir, os.path.basename(hdfs_file))
            self.get(hdfs_file, local_file)

    def get(self, src_hdfs_file, dst_local_file):
        run_cli(f'hadoop fs -get {src_hdfs_file} {dst_local_file}',self.env)
        # f = open(dst_local_file, 'wb')
        # self._fs.download(src_hdfs_file, f)
        # f.close()

    def put(self, src_local_file, dst_hdfs_file):
        # f = open(src_local_file, 'rb')
        # self._fs.upload(dst_hdfs_file, f)
        # f.close()
        run_cli(f'hadoop fs -put -f {src_local_file} {dst_hdfs_file}',self,env)

    def mkdir(self, hdfs_dir):
        run_cli(f'hadoop fs -mkdir -p {hdfs_dir}',self.env)

    def exists(self, hdfs_path):
        # return self._fs.exists(hdfs_path)
        try:
            ret = self.ls(hdfs_path)
            if len(ret) >0:
                return True
            else:
                return False
        except Exception as e:
            print("hdfs exists exception:" +  str(e))
            return False

    def rm(self, hdfs_path):
        # self._fs.rm(hdfs_path, recursive=recursive)
        try:
            run_cli(f'hadoop fs -rm -r {hdfs_path}',self.env)
        except Exception as e:
            pass

    def download_checkpoint(self, hdfs_dir, local_dir):
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
        os.makedirs(local_dir)

        ckpt_dir = self._find_ckpt_dir(hdfs_dir)
        for i in self.ls(ckpt_dir):
            basename = os.path.basename(i)
            self.get(i, os.path.join(local_dir, basename))

    def _find_ckpt_dir(self, hdfs_model_dir, ckpt_prefix='model.ckpt'):
        for container in self.ls(hdfs_model_dir):
            basename = os.path.basename(container)
            if basename.startswith('container_'):
                # check each container dir, checkpoint file must in one of them
                for i in self.ls(container):
                    f = os.path.basename(i)
                    if f.startswith(ckpt_prefix):
                        return container


