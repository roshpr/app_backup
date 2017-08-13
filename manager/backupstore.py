import re
import os
import utils
from natsort import natsorted
import fnmatch
import logging
import datetime
from pathlib import Path
import nfs
import yaml
from shutil import copyfile


TIMESTAMP_PATTERN = re.compile(r'''
    # Required components.
    (?P<year>\d{4} ) \D?
    (?P<month>\d{2}) \D?
    (?P<day>\d{2}  ) \D?
    (?:
        # Optional components.
        (?P<hour>\d{2}  ) \D?
        (?P<minute>\d{2}) \D?
        (?P<second>\d{2})?
    )?
''', re.VERBOSE)

class BackupStore(object):
    def __init__(self, path, exclude_list=[], include_list=None):
        self.path = str(Path(os.path.expanduser(path)).absolute().resolve())
        self.backupdir = os.path.join(self.path, 'current')
        self.exclude_list = exclude_list
        self.include_list = include_list

        utils.ensure_directory(self.path)
        utils.ensure_directory(self.backupdir)

    def pre_backup(self, hosts=['*']):
        # create required paths and nfs share
        nfs.install_nfs_server()
        nfs.add_nfs_share(self.backupdir, hosts=hosts)
        nfs.start_nfs_server()

    def post_backup(self):
        nfs.delete_nfs_share(self.backupdir)

    def pre_restore(self, snapshot_name, backup_name=None, hosts=['*']):
        snapshot = self.collect_snapshot(snapshot_name, backup_name)
        nfs.install_nfs_server()
        nfs.add_nfs_share(snapshot.path, hosts=hosts)
        nfs.start_nfs_server()
        return snapshot

    def post_backup(self, snapshot):
        nfs.delete_nfs_share(snapshot.path)
        return snapshot

    def collect_snapshot(self, snapshot_name, backup_name=None):
        backup_dir = os.path.expanduser(os.path.join(self.path, backup_name) if backup_name else self.path)
        logging.info("Collecting backup %s form %s ..", snapshot_name, backup_dir)
        snapshot_path = Path(os.path.join(backup_dir, snapshot_name))
        if not snapshot_path.exists() or not snapshot_path.is_dir():
            raise Exception('Cannot find snapshot directory %s' % str(snapshot_path))

        match = TIMESTAMP_PATTERN.search(snapshot_name)
        return Snapshot(
                backup_name=backup_name,
                snapshot_name=snapshot_name,
                path=str(snapshot_path),
                timestamp=datetime.datetime(*(int(group, 10) for group in match.groups('0'))),
              )


    def collect_snapshots(self, backup_name):
        backups = []
        backup_dir = os.path.expanduser(os.path.join(self.path, backup_name) if backup_name else self.path)
        logging.info("Scanning %s for backups ..", backup_dir)

        subfolders = [f.name for f in Path(backup_dir).iterdir()]
        #subfolders = filter(lambda x: os.path.isdir(os.path.join(backup_dir, x)), os.listdir(backup_dir))
        #subfolders = [f.name for f in os.scandir(backup_dir) if f.is_dir()]

        for entry in natsorted(subfolders):
          match = TIMESTAMP_PATTERN.search(entry)

          if match:
            if self.exclude_list and any(fnmatch.fnmatch(entry, p) for p in self.exclude_list):
              logging.verbose("Excluded %r (it matched the exclude list).", entry)
            elif self.include_list and not any(fnmatch.fnmatch(entry, p) for p in self.include_list):
              logging.verbose("Excluded %r (it didn't match the include list).", entry)
            else:
              backups.append(Snapshot(
                backup_name=backup_name,
                snapshot_name=entry,
                path=os.path.join(backup_dir, entry),
                timestamp=datetime.datetime(*(int(group, 10) for group in match.groups('0'))),
              ))
          else:
            logging.debug("Failed to match time stamp in filename: %s", entry)
        if backups:
          logging.info("Found %i timestamped backups in %s.", len(backups), backup_dir)

        return sorted(backups, key=lambda b: (b.timestamp, b.backup_name) )

    def create_snapshot(self, backup_name, timestamp=None, target=None):
        backup_dir = os.path.expanduser(os.path.join(self.path, backup_name) if backup_name else self.path)
        utils.ensure_directory(backup_dir)
        if timestamp is not None:
            backup_datetime = datetime.datetime.fromtimestamp(timestamp)
        else:
            backup_datetime = datetime.datetime.now()

        target_name = target if target is not None else backup_datetime.strftime('%Y-%m-%dT%H:%M:%S')
        snapshot_dir = Path(os.path.join(backup_dir, target_name))
        source_dir = Path(self.backupdir)
        if not source_dir.is_dir():
            raise Exception('Source directory %s does not exist' % str(source_dir))
        if snapshot_dir.exists():
            raise Exception('Destination directory %s already exist' % str(snapshot_dir))

        #os.makedirs(snapshot_dir)
        cp_command = [
            'cp', '--archive', '--link',
            str(source_dir),
            str(snapshot_dir),
        ]

        logging.info("Creating snapshot: %s", str(snapshot_dir))
        status, code, output = utils.run_command(command=cp_command)

        if not status:
            logging.error("Failed to create snapshot %s", str(snapshot_dir))
            raise Exception('Create snapshot error %s' % (code), output)

        return str(snapshot_dir)

    def rotate_snapshots(self, backup_name, keep):
        sorted_backups = self.collect_snapshots(backup_name)
        if not sorted_backups:
            logging.info("No backups found in %s.", os.path.join(self.path, backup_name))
            return

        oldest_backup = sorted_backups[0]
        newest_backup = sorted_backups[-1]

        backups_to_delete = len(sorted_backups) - keep
        if backups_to_delete > 0:
            for index in range(0, int(backups_to_delete)):
                backup = sorted_backups[index]
                logging.info('Deleting snapshot path %s' % backup.path)
                status, code, output = utils.run_command(command=['rm', '--force', '--recursive', backup.path])
                if not status:
                    raise Exception('Failed to delete snapshot %s' % backup.path, output)


        return sorted_backups[:backups_to_delete]


class Snapshot(object):

    """
    :class:`Backup` objects represent a rotation subject.
    In addition to the :attr:`pathname`, :attr:`timestamp` and :attr:`week`
    properties :class:`Backup` objects support all of the attributes of
    :class:`~datetime.datetime` objects by deferring attribute access for
    unknown attributes to :attr:`timestamp`.
    """

    key_properties = 'name', 'timestamp', 'pathname'
    def __init__(self, backup_name, snapshot_name, path, timestamp):
        self.backup_name = backup_name
        self.snapshot_name = snapshot_name
        self.path = path
        self.timestamp = timestamp

def display_status(path):
    from prettytable import PrettyTable
    from prettytable import ALL
    path = Path(path)
    if not path.exists():
        raise Exception("Path %s does not exist"%str(path))

    files = []
    if path.is_dir():
        backupfile = path.joinpath('backup.yaml')
        if backupfile.exists():
            files.append(backupfile)
        restorefile = path.joinpath('restore.yaml')
        if restorefile.exists():
            files.append(restorefile)
    else:
        files.append(path)


    for statusfile in files:
        if statusfile.name == 'backup.yaml':
            mode = 'backup'
            title = 'Backup Status'
        else:
            mode = 'restore'
            title = 'Restore Status'

        logging.info('Loading file %s', str(statusfile))
        status_yaml = yaml.load(statusfile.open('r'))

        if mode == 'backup':
	    table_header = ['Cluster', 'Node', 'Component', 'Role', 'Supports', 'Pre Backup', 'Backup', 'Post Backup']
            table = PrettyTable(table_header)
        else:
            table_header = ['Cluster', 'Node', 'Component', 'Role', 'Supports', 'Pre Restore', 'Restore', 'Post Restore']
            table = PrettyTable(table_header)

        table.align["Cluster"] = "l"
        table.border = True
	table.hrules = ALL

	html_table_data = []
        nodes = status_yaml.get('nodes', {})
        for node_name, node_data in nodes.iteritems():
            for comp_name, comp_data in node_data.get('components', {}).iteritems():
                cluster = node_data.get('config',{}).get('cluster', '')
                host = '%s:%s'%( node_name, node_data.get('config').get('host', ''))
                component = comp_name
                role = comp_data.get('role')
                supports = ', '.join(comp_data.get('supports', []))
                pre_status = comp_data.get('pre_backup', comp_data.get('pre_restore', ''))
                exec_status = comp_data.get('backup', comp_data.get('restore', ''))
                post_status = comp_data.get('post_backup', comp_data.get('post_restore', ''))

                table.add_row([cluster, host, component, role, supports, pre_status, exec_status, post_status])
		html_table_data.append([cluster, host, component, role, supports, pre_status, exec_status, post_status])
		
	htmlcode = table.get_html_string()
	htmlcode_patched_table = htmlcode.replace('<table>', '<table class="responstable">') 
	headcode = '<head><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/normalize/5.0.0/normalize.min.css"><link rel="stylesheet" href="style.css"></head><body>'
	import time
	status_file = ''
	if mode == 'backup':
	    header = '<h1>Backup status: {0}</h1>'.format(time.strftime("%c"))
	else:
	    header = '<h1>Restore status: {0}</h1>'.format(time.strftime("%c"))

	status_file = '/{0}_status.html'.format(mode)
	htmlcode_final = '{0}{1}{2}</body>'.format(headcode, header, htmlcode_patched_table)
	status_dir = '/var/www/html'
	utils.ensure_directory(status_dir)
	copyfile('style.css', status_dir+'/style.css')
	file_html = open(status_dir+status_file,'w') 
	file_html.write(htmlcode_final)
 	file_html.close()
        print('################## %s ##################'% (title) )
        print table
        print('###################################################')

if __name__ == "__main__":
  #args = sys.argv[1:]
  logging.basicConfig(level=logging.DEBUG)
  display_status('/backups/current')
  #bs = BackupStore('~/csp/csp-application-deployment/backup/examples/backups')
  #snapshot_dir = bs.create_snapshot('a')
  #backups = bs.collect_snapshots('a')
  #bs.rotate_snapshots('a', 3)
  #print backups
