import utils
import logging
from pathlib import Path


def is_nfs_server_installed():
    command = ['dpkg', '-l', 'nfs-kernel-server']
    status, code, output = utils.run_command(command=command, output=True, ignore=True)
    if not status:
        return False
    if "ii  nfs-kernel-server" in output:
        return True
    return False


def install_nfs_server():
    #check if nfs is installed
    if not is_nfs_server_installed():
        logging.warn('Installing nfs-kernel-server, this may take lot of time, please be patient')
        utils.run_command(command=['sudo', 'apt-get', 'update', '-y'])
        utils.run_command(command=['sudo', 'apt-get', 'install', '-y', 'nfs-kernel-server'])
    else:
        return True

    if not is_nfs_server_installed():
        raise Exception("Could not install nfs-kernel-server, please check manually")

def start_nfs_server():
    command = ['sudo', 'service', 'nfs-kernel-server', 'start']
    utils.run_command(command=command)

def export_nfs_share(path, hosts=['*'], options="rw,sync,no_root_squash,no_subtree_check"):
    for host in hosts:
        export_cmd = ['sudo', 'exportfs', '-o', options, ':'.join([host, path])]
        utils.run_command(command=export_cmd)

def unexport_nfs_share(path, hosts=['*']):
    for host in hosts:
        unexport_cmd = ['sudo', 'exportfs', '-u', ':'.join([host, path])]
        utils.run_command(command=unexport_cmd)

def mount_nfs_share(server, path, mountpoint):

    mountsource = ':'.join([server, path])
    status, code, output = utils.run_command(command=['mount', '-l', '-t', 'nfs', '-t', 'nfs4'], retry_sudo=True)
    if mountsource + " on " + mountpoint in output:
        return
    elif "on " + mountpoint in output:
        raise Exception("Mount point %s is already in use"%(mountpoint))

    if not Path(mountpoint).exists():
        utils.run_command(command=['mkdir', '-p', mountpoint], retry_sudo=True)

    mount_cmd = ['sudo', 'mount', mountsource, mountpoint]
    utils.run_command(command=mount_cmd)

    chmod_cmd = ['sudo', 'chmod', 'a+w', mountpoint]
    utils.run_command(command=chmod_cmd)

def unmount_nfs_share(mountpoint):
    status, code, output = utils.run_command(command=['mount', '-l', '-t', 'nfs,nfs4'], output=True, retry_sudo=True)
    if "on " + mountpoint not in output:
        return

    umount_cmd = ['sudo', 'umount', '-fl', mountpoint]
    utils.run_command(command=umount_cmd, retry_sudo=True)

def delete_nfs_share(path):
    pattern = r'\|^' + path + '.*$|d'
    utils.run_command(command=['sudo', 'sed', '-i', pattern, '/etc/exports'])
    export_cmd = ['sudo', 'exportfs', '-ra']
    utils.run_command(command=export_cmd)

def add_nfs_share(path, hosts=['*'], options="rw,sync,no_root_squash,no_subtree_check"):
    delete_nfs_share(path)

    hostoptions = ''
    for host in hosts:
        hostoptions += host+'('+options+') '

    exentry = path + '  ' + hostoptions + '\n'

    import os
    pread, pwrite = os.pipe()
    os.write(pwrite, exentry)
    os.close(pwrite)

    utils.run_command(command=['sudo', 'tee', '-a', '/etc/exports'], stdin=pread)
    export_cmd = ['sudo', 'exportfs', '-ra']
    utils.run_command(command=export_cmd)



