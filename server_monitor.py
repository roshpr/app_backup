#!/usr/bin/python
import time
import logging
import sys
import subprocess
import listener_monitor_server as lms
import argparse

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger('DBSwitchOver')
CONNECT_TO_PORT = 'netcat -z -v {0} {1}'
MYSQL_PORT = 3306
SSH_TUNNEL_CMD = 'ssh root@{0} {1}'
# master_ip={{ master_ip }}
# slave_ips=[{{ slave_ips }}]
master_ip = ''
slave_ip = []
# mysql_pass = {{ mysql_pass }}
# no_minutes_to_wait_b4_reset_master = {{ time_to_change_master }}
# no_of_retries = {{ no_of_retries }}

mysql_pass = 'passw0rd'
no_minutes_to_wait_b4_reset_master = 5
no_of_retries = 5


def exec_command(command, output=True, shell=True, ignore=False, retry_sudo=False,  **kwargs):

    try:
        logging.info('Running command: %s' % " ".join(command) if type(command) is list else command)
        if output:
            output = subprocess.check_output(command, shell=shell, stderr=subprocess.STDOUT, **kwargs)
            return (True, 0, output)
        else:
            retcode = subprocess.call(command, shell=shell, stderr=subprocess.STDOUT, **kwargs)
            return (True, retcode, None)

    except subprocess.CalledProcessError as ex:
        if retry_sudo and 'sudo' not in command and ( "Permission denied" in ex.output or "only root can do that" in ex.output):
            if type(command) is list:
                sudo_cmd = ['sudo'] + command
            else:
                sudo_cmd = 'sudo ' + command
            logging.info('Retrying command: %s' % " ".join(sudo_cmd) if type(sudo_cmd) is list else sudo_cmd)
            return exec_command(sudo_cmd, output, shell=shell, ignore=ignore, retry_sudo=False, **kwargs)


        cmd = ex.cmd
        if type(cmd) is list:
            cmd = " ".join(cmd)

        logging.error('Failed to run command: %s details:%s,%s' % (cmd, ex.returncode, ex.output))

        if ignore:
            return (False, ex.returncode, ex.output)
        else:
            raise Exception('Failed to run command: %s' % cmd, ex.returncode, ex.output)

log.info('Listener scheduling')
#lms.invoke_listener()


master_reset_time_retry_count = 0
master_reset_cmd_retry_count = 0
master_list = list()
master_list.append(master_ip)


def spawn_ssh_cmd(cmd, node_ip):
    log.info('Send commands via ssh')
    cmd_thru_ssh = SSH_TUNNEL_CMD.format(node_ip, cmd)
    exec_command(cmd_thru_ssh)

def mariadb_monitor():
    while True:
        time.sleep(60)
        status, ret_code, output = exec_command(CONNECT_TO_PORT.format(master_ip, MYSQL_PORT), shell=True)
        if ret_code == 0:
            continue
        print 1


def parse_options(args):
    parser = argparse.ArgumentParser(description='Use cautiously. Only admins to use this MariaDB switch over.')

    subparsers = parser.add_subparsers(help='MariaDB Switchover', dest='command')

    add_p = subparsers.add_parser('start', help='Start listening')
    # add_p.add_argument("--mhost", help='MariaDB Host',
    #                    metavar='<ip>', required=True)
    # add_p.add_argument("--mpass", help='MariaDB passwd',
    #                    metavar='<ip>', required=True)

    args = parser.parse_args()
    return args


if __name__ == "__main__":

    exit(0)
    options = parse_options(sys.argv[1:])

    if options.command == 'start':
        mariadb_monitor()