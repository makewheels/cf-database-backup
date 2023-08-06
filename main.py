# https://gitee.com/publishcode/cf-database-backup/hooks/1321604/edit

import logging
import os
import pymongo
import oss2
import tarfile
import datetime


def start(event, context):
    print('efafawfwef')
    logger = logging.getLogger()
    logger.info('hello world')
    return 'hello world'
