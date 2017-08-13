from agent.utils import processutils, fileutils, utils
import yaml
import socket
import datetime
import logging
import re
log = logging.getLogger('BackupAgent')

GET_ROLES = 'sudo salt-call --local grains.get roles'
GET_IP = 'hostname -I'
APPS_SUPPORTING_ROLE = ('etcd', 'elasticsearch')


def generate_node_data():
    '''
    Aggregates node system & application related data
    Returns: node related data
    '''
    app_data = generate_app_data()
    system_data = generate_system_data()
    node_info = dict()
    node_info['components'] = {}
    node_info['system_info'] = system_data

    return node_info


def generate_system_data():
    '''
    hostname: host1
    ip: 127.0.0.1
    time: current time
    data: arbitary data that client wants to store
    '''

    system_info = dict()
    system_info['hostname'] = socket.gethostname()
    ret, ip = processutils.execute_long_process(GET_IP)
    system_info['ip'] = ip
    system_info['time'] = utils.totimestamp(datetime.datetime.now())
    return system_info


def generate_app_data():
    roles = get_roles()
    return roles


def get_roles():
    '''
    Fetches application running in system using salt grains info
    Returns: running apps
    '''
    raw_roles = fetch_salt_roles()
    # commented: this will destroy lists in yaml
    stripped_roles = re.sub('--+', '', raw_roles)
    # stripped_roles = raw_roles.replace('-','')
    plugin_apps = fileutils.get_available_apps()
    salt_roles = yaml.load(stripped_roles)
    roles = list()
    for app in salt_roles['local']:
        if app in plugin_apps:
            roles.append(app)
    return roles


def fetch_salt_roles():
    ret_code, resp = processutils.execute_long_process(GET_ROLES)
    return resp
