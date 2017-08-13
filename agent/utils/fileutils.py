import yaml
import os
import logging
log = logging.getLogger('BackupAgent')
APPS_PATH = "agent/appcontrollers/apps"


def get_yaml_config(file):
    data_file = open(file, 'r')
    debug_data = yaml.load(data_file)
    data_file.close()
    return debug_data


def get_available_apps():
    lst = os.listdir(APPS_PATH)
    apps = list()
    for fil in lst:
        if fil.find("__") == -1 and fil.find(".pyc") == -1:
            pos = fil.find(".py")
            apps.append(fil[:pos])
    log.info('Discovered Apps %s' % str(apps))
    return apps
