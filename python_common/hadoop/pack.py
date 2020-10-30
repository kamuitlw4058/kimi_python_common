#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'kimi'
__email__ = 'wangcenhan@xiaoniuhy.com'

import os
import zipfile

from ..utils.logger import getLogger
logger = getLogger(__name__)

from ..hadoop.hdfs import HDFS


def zip_dir(src_dir, dst_name, new_root_name=None, ignore_dir=None,ignore_root_dir= None):
    if ignore_dir is None:
        ignore_dir = []
    ignore_dir.extend(['__pycache__'])

    if ignore_root_dir is None:
        ignore_root_dir = []
    ignore_root_dir.extend( ['.git'])

    zipf = zipfile.ZipFile(dst_name, 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(src_dir):
        if not files:
            continue
        if os.path.basename(root) in ignore_dir:
            continue

        relpath = os.path.relpath(root, src_dir)
        ignore_root_dir_match = False
        for i in ignore_root_dir:
            if str(relpath).startswith(i):
                ignore_root_dir_match = True
        if ignore_root_dir_match:
            continue

        for file in files:
            arcname = os.path.join(relpath, file)
            if arcname.startswith('./'):
                arcname = arcname[2:]

            if arcname == dst_name:
                continue
            if new_root_name:
                arcname = os.path.join(new_root_name, arcname)
            zipf.write(os.path.join(root, file), arcname)
    zipf.close()

def pack_and_upload(upload_path,overwrite = True,package_name= 'trainer_package',package_suffix='zip',hdfs=None,subdir=None, ignore_dir=None,ignore_root_dir=None):
    if subdir is None:
        path = os.getcwd()
    else:
        path = os.path.join(os.getcwd(),subdir)
        
    zip_path = f'{package_name}.{package_suffix}'
    zip_dir(path, zip_path,ignore_dir=ignore_dir,ignore_root_dir=ignore_root_dir)
    logger.info(f'build pack from {path} to {zip_path}')

    if hdfs is None:
        hdfs = HDFS()

    if hdfs.exists(upload_path):
        if not overwrite:
            logger.info(f'{zip_path} is exist! {upload_path}')
            return
        hdfs.rm(upload_path)
    hdfs.mkdir(os.path.dirname(upload_path))

    hdfs.put(zip_path, upload_path)

    logger.info(f'success upload {zip_path} to {upload_path}')
