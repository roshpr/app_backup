from bottle import request
import logging
from agent import constants
import json
import os
import functools
from agent.utils import fileutils, utils, nfs
from agent.appcontrollers.app_manager import AppManager
from device_info import *
log = logging.getLogger('BackupAgent')

app_mgr = None


def loop_apps(func):
    @functools.wraps(func)
    def func_wrapper(appname=None):
        req_data = json.loads(json.dumps(request.json))
        if os.getenv('DEBUG', '0') == '1':
            req_data = app_mgr.debug_data
        response = func(req_data)
        return response

    return func_wrapper


def initialize_apps():
    global app_mgr
    app_mgr = AppManager()
    app_mgr.initialize()


@loop_apps
def pre_backup(req_data=None):
    log.info('target pre_backup')
    try:
        nfs.unmount_nfs_share(constants.BACK_UP_DIR)
    except Exception as ex:
        log.warn('Unmount nfs share failed with %s'% str(ex))

    nfs.mount_nfs_share(req_data['server'], req_data['nfs_share'], constants.BACK_UP_DIR)

    return app_mgr.call_apps('pre_backup', req_data)


@loop_apps
def backup(req_data=None):
    log.info('target backup')
    return app_mgr.call_apps('backup', req_data)


@loop_apps
def post_backup(req_data=None):
    log.info('target post_backup')

    resp = app_mgr.call_apps('post_backup', req_data)

    try:
        nfs.unmount_nfs_share('/backup')
    except:
        pass

    return resp


@loop_apps
def pre_restore(req_data=None):
    log.info('target pre_restore')
    try:
        nfs.unmount_nfs_share(constants.BACK_UP_DIR)
    except Exception as ex:
        log.warn('Unmount nfs share failed with %s' % str(ex))

    nfs.mount_nfs_share(req_data['server'], req_data['nfs_share'], constants.BACK_UP_DIR)

    return app_mgr.call_apps('pre_restore', req_data)


@loop_apps
def restore(req_data=None):
    log.info('target restore')
    return app_mgr.call_apps('restore', req_data)


@loop_apps
def post_restore(req_data=None):
    log.info('target post_restore')
    return app_mgr.call_apps('post_restore', req_data)

def describe_node():
    log.info('target describe node')
    device_data = generate_node_data()
    log.info('Apps in system %s', device_data)
    device_app_data = app_mgr.fetch_apps_info(device_data)
    # debug_data = fileutils.get_yaml_config('appcontrollers/describenode.yaml')
    return device_app_data


def health():
    log.info('Get system health')
    debug_data = fileutils.get_yaml_config('data/health.yaml')
    return debug_data