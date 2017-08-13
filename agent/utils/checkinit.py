from agent.utils import utils, nfs
import os
import logging
log = logging.getLogger('BackupAgent')

def install_nfs_server():
    log.info('Installing NFS server')
    #check if nfs is installed
    nfs.install_nfs_server()


def is_nfs_server_installed():
    log.info('Check NFS server is installed')
    command = ['dpkg', '-l', 'nfs-kernel-server']
    status, code, output = utils.run_command(command=command, output=True, ignore=True)
    if not status:
        return False
    if "ii  nfs-kernel-server" in output:
        return True
    return False


def is_user_root():
    log.info('Check the user running the application')
    uid = os.geteuid()
    log.debug('Current user uid: %d', uid)
    if uid == 0:
        return True
    else:
        return False
