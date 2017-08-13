import json
import requests
import argparse
import subprocess
import logging
import sys
import socket
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger('DBSwitchOver')

PORT = '2379'
ETCD_BASE_URL = "http://{0}:{1}/v2/keys"
KEY_RESOURCE = "/lb/services/mariadb/backends/mariadbbackend"

MYSQL_CONNECT = 'mysql -h {0} -u root -p{1} -e "{2}";'


def execute_process(command):
    '''
    execute a long running command
    :param command:
    :return:
    '''
    try:
        log.debug('Executing %s', command)
        retcode = subprocess.call(command, shell=True)
        if retcode < 0:
            log.error('Subprocess failed with return code {0}'.format(retcode))
        return retcode
    except OSError as e:
        log.exception('Command %s failed', command)
        return -1


def change_slave_to_master(mysql_host, mysql_pass):
    log.info('MariaDB slave switch over')

    STOP_SLAVE = MYSQL_CONNECT.format(mysql_host, mysql_pass, "STOP SLAVE")
    retcode = execute_process(STOP_SLAVE)
    if retcode == 0:
        RESET_SLAVE = MYSQL_CONNECT.format(mysql_host, mysql_pass, "RESET SLAVE ALL")
        retcode = execute_process(RESET_SLAVE)
        if retcode == 0:
            SLAVE_STATUS = MYSQL_CONNECT.format(mysql_host, mysql_pass, "SHOW SLAVE STATUS")
            retcode = execute_process(SLAVE_STATUS)
            if retcode == 0:
                log.info('MariaDB master switchover completed')
                secondary_slave_choice = raw_input("Do you have a secondary mariadb slave? [y/n]:")
                if secondary_slave_choice.lower() == 'y':
                    secondary_slave_ip = raw_input("Enter secondary marisdb slave IP :")
                    if not secondary_slave_ip:
                        print 'Assuming there is no secondary server. Continuing'
                    else:
                        if check_valid_ip(secondary_slave_ip):
                            log.info('Processing secondary db server %s', secondary_slave_ip)
                            ret = process_secondary_dbserver(mysql_host, secondary_slave_ip, mysql_pass)
                            if not ret:
                                log.error('##############################################')
                                log.error('Secondary DB Slave master switch over failed')
                                log.error('Configure secondary slave master switch over manually')
                                return True
                        else:
                            print 'Invalid IP, asssuming there is no secondary server. Continuing'
                            return True
                else:
                    log.info('No Secondary slave to be configured')
                    return True
            else:
                log.warn('Show slave status failed. Check the DB Status manually.')
                return True
        else:
            log.error('Master reset failed')
            return False
    else:
        log.error('Stopping slave config failed.')
        return False

    return True

def process_secondary_dbserver(master_dbhost, slave_ip, mysql_pass):
    log.info('Configuring slave %s to master %s', slave_ip, master_dbhost)
    stop_slave = MYSQL_CONNECT.format(slave_ip, mysql_pass, "STOP SLAVE")
    retcode = execute_process(stop_slave)
    if retcode == 0:
        RESET_SLAVE = MYSQL_CONNECT.format(slave_ip, mysql_pass, "RESET SLAVE ALL")
        retcode = execute_process(RESET_SLAVE)
        if retcode == 0:
            change_master = "CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='root', MASTER_PASSWORD='%s'" % \
                            (master_dbhost, mysql_pass)
            set_master_on_slave = MYSQL_CONNECT.format(slave_ip, mysql_pass, change_master)
            retcode = execute_process(set_master_on_slave)
            if retcode == 0:
                start_slave = MYSQL_CONNECT.format(slave_ip, mysql_pass, "START SLAVE")
                retcode = execute_process(start_slave)
                if retcode == 0:
                    log.info('Slave switch to new master completed for slave host %s', slave_ip)
                    return True
                else:
                    log.error('Starting secondary slave failed')
                    return False
            else:
                log.error('Changing master host in secondary server failed')
                return False
        else:
            log.error('Resetting secondary slave failed')
            return False
    else:
        log.error('Stopping slave replication failed')
        return False


def check_valid_ip(addr):
    try:
        socket.inet_aton(addr)
        return True
    except socket.error:
        return False


def etcd_config_change(haproxy_host, mysql_slave_host):
    log.info('HAproxy master switch over')
    etcd_config = '{"beg_urls": null, "options": [], "servers": [{"ip": "%s", "port": 3306}]}' % (mysql_slave_host)
    #resource = '/testha'
    etcd_response = requests.put(ETCD_BASE_URL.format(haproxy_host, PORT) + KEY_RESOURCE, data=etcd_config)

    node_status = etcd_response.status_code
    if node_status and node_status == 200:
        log.info('HAProxy master switchover completed. {0}'.format(node_status))
    elif node_status:
        log.error('HAProxy mariadb switch over failed: {0}'.format(node_status))
    else:
        log.error('HAProxy mariadb switch over failed')


def slave_switch(haproxy_host, mysql_slave_host, mysql_admin_pass):
    log.info('Starting master switch over')
    try:
        ret_result = change_slave_to_master(mysql_slave_host, mysql_admin_pass)
        if ret_result:
            etcd_config_change(haproxy_host, mysql_slave_host)
            log.info('Switch over completed in MariaDB & HAProxy')
        else:
            log.error("########################################")
            log.error("Master switch over failed")
    except Exception:
        log.exception('Master switch over failed')


def parse_options(args):
    parser = argparse.ArgumentParser(description='Use cautiously. Only admins to use this MariaDB switch over.')

    subparsers = parser.add_subparsers(help='MariaDB Switchover', dest='command')

    add_p = subparsers.add_parser('options', help='Switch master')
    add_p.add_argument("--mhost", help='MariaDB Host',
                       metavar='<ip>', required=True)
    add_p.add_argument("--mpass", help='MariaDB passwd',
                       metavar='<ip>', required=True)
    add_p.add_argument("--hhost", help='HAProxy Host',
                       metavar='<ip>', required=True)

    args = parser.parse_args()
    return args

if __name__ == "__main__":
    options = parse_options(sys.argv[1:])
    if options.command == 'options':
        mhost = options.mhost
        mpass = options.mpass
        hhost = options.hhost
        slave_switch(hhost, mhost, mpass)
