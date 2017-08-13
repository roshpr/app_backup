from agent.appcontrollers.targets import *
import logging

log = logging.getLogger('BackupAgent')


def register_routes(rout):
    log.info('register_routes')
    initialize_apps()
    rout('/prebackup', 'POST', pre_backup)
    rout('/prerestore', 'POST', pre_restore)
    rout('/postbackup', 'POST', post_backup)
    rout('/postrestore', 'POST', post_restore)
    rout('/backup', 'POST', backup)
    rout('/restore', 'POST', restore)
    rout('/describenode', 'GET', describe_node)
    rout('/health', 'GET', health)