import yaml
import argparse, sys
import logging
from prettytable import PrettyTable
log = logging.getLogger('BackupManager')


def process_backup_status(backup_name, snapshotname, clusters=('central')):
    backup_path = '/backups'
    for cluster in clusters:
        file_path = '{0}/{1}/{2}/backup_{3}.yaml'.format(backup_path, backup_name, snapshotname, cluster)
        log.info('Status file path %s', file_path)

        status_file = open(file_path, 'r')
        status_data = status_file.read()
        status_dict = yaml.load(status_data)

        if 'components' in status_dict:
            table = PrettyTable(['Application', 'Role', 'Supports', 'Pre Backup Status', 'Backup Status'])
            table.align["Application"] = "l"
            apps = status_dict['components']
            for app in apps:
                table.add_row([app.title(),
                               apps[app]['role'],
                               apps[app]['supports'],
                               apps[app]['pre_backup'],
                               apps[app]['backup']])
            print('################## %s Backup Status ##################' % cluster.title())
            print table
        file_path = '{0}/{1}/{2}/restore_{3}.yaml'.format(backup_path, backup_name, snapshotname, cluster)
        log.info('Status file path %s', file_path)

        status_file = open(file_path, 'r')
        status_data = status_file.read()
        status_dict = yaml.load(status_data)

        if 'components' in status_dict:
            table = PrettyTable(['Application', 'Role', 'Supports', 'Pre Restore Status', 'Restore Status',
                                 'Post Restore Status'])
            table.align["Application"] = "l"
            apps = status_dict['components']
            for app in apps:
                table.add_row([app.title(),
                               apps[app]['role'],
                               apps[app]['supports'],
                               apps[app]['pre_restore'],
                               apps[app]['restore'] if 'restore' in apps[app] else 'NA',
                               apps[app]['post_restore']])
            print('################## %s Restore Status ##################' % cluster.title())
            print table


def parse_options(args):
    parser = argparse.ArgumentParser(description='Fetch the status')
    subparsers = parser.add_subparsers(help='Add sub commands', dest='command')
    add_p = subparsers.add_parser('status', help='fetch status')
    add_p.add_argument("--backupname", help='Mandatory: Name of the backup',
                       metavar='daily', required=True)
    add_p.add_argument("--snapshotname", help='Mandatory: snapshotname usually time',
                       metavar='2017-10-20:10-20-10', required=True)
    add_p.add_argument("--clusters", help='Mandatory: name of different regions',
                           metavar='central,regional', required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    options = parse_options(sys.argv[1:])
    if options.command == 'status':
        process_backup_status(options.backupname, options.snapshotname, options.clusters.split(','))
    #process_backup_status(backup_name, isnapshot_name, [list_of_regions])
