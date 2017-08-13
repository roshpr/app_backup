import string
import tempfile
import os
import getopt
import sys
import errno
import logging
from multiprocessing.pool import ThreadPool
import yaml
import pystache
from masternode import BackupNode
from backupstore import BackupStore, Snapshot, display_status
import time, datetime
import psutil
import natsort
import utils

DEFAULT_NAME = 'backup'
DEFAULT_KEEP = 1
DEFAULT_SERVERS = []
DEFAULT_USER = 'root'
DEFAULT_NUM_THREADS = psutil.cpu_count()


class BackupManager:
  '''
  directory structure for the backup
  self.store/latest => always do rsync to this folder
  self.store/latest/node => all backup data for this node
  self.store/latest/node/component => all the backup data for this node and a component
  '''
  def __init__(self, config, **kwargs):
    self.name = config['name']
    self.keep = config['keep']
    self.server = config['server']
    self.node_port = config.get('node_port')
    self.node_protocol = config.get('node_protocol')
    self.store = BackupStore(config['store'])

    self.user = config['user']
    self.components = config['components']
    self.snapshot_time = kwargs.get('snapshot_time', datetime.datetime.utcnow())
    self.snapshot_name = kwargs.get('snapshot_name', self.snapshot_time.strftime('%Y-%m-%dT%H:%M:%S'))
    self.nodes = {}


    for node_config in config['nodes']:
      #node_store = os.path.join(self.workdir, name)
      node = BackupNode(self.store, self.components,
                        port=self.node_port,
                        protocol=self.node_protocol,
                        config=node_config,
                        backup_name=self.name,
                        snapshot_time=self.snapshot_time,
                        snapshot_name=self.snapshot_name)
      self.nodes[node.node_name] = node

  def get_status(self):
    status_data = {
      'name': self.name,
      'keep': self.keep,
      'server': self.server,
      'node_port': self.node_port,
      'node_protocol': self.node_protocol,
      'user': self.user,
      'snapshot_time': utils.totimestamp(self.snapshot_time),
      'snapshot_name': self.snapshot_name,
      'components': self.components
    }
    nodes = {}
    status_data['nodes'] = nodes
    for node_name, node in self.nodes.items():
      nodes[node_name] = node.get_status()

    return status_data


  def dump_status(self, prefix):
      dump_path = os.path.join(self.store.backupdir, prefix + '.yaml')
      status_data = self.get_status()

      try:
        os.remove(dump_path)
      except:
        pass

      with open(dump_path, 'w') as outfile:
          yaml.safe_dump(status_data, outfile, indent=2)

  def collect_node_details(self):
    pool = ThreadPool(processes=len(self.nodes))
    results = {}
    for node in self.nodes.values():
      async_result = pool.apply_async(node.get_node_info)
      results[node.node_name] = async_result

    for node_name, async_result in results.items():
      status, mesg, data = async_result.get()
      if not status:
        #TODO: check flag to allow partial backups and continue
        raise Exception("Failed to collect node details form %s, reason: %s" % (node_name, mesg))

  def pre_backup(self, force=False):
    '''
    Prepare the node for backup and keep the data ready to be pulled
    Things like stopping or flushing application buffers and taking LVM snapshot
    :return:
    '''
    self.store.pre_backup()
    pool = ThreadPool(processes=len(self.nodes))
    results = {}
    for node in self.nodes.values():
      async_result = pool.apply_async(node.pre_backup, (force,))
      results[node.node_name] = async_result

    for node_name, async_result in results.items():
      status, mesg, detail = async_result.get()
      if not status:
        # TODO: check flag to allow partial backups and continue
        self.post_backup()
        raise Exception("Failed to run pre_backup for %s, reason: %s" % (node_name, mesg))


  def backup(self, force=False):
    '''
    Pull backup data from all nodes one prepare_backup is completed
    :return:
    '''
    pool = ThreadPool(processes=int(DEFAULT_NUM_THREADS))
    results = {}
    for node_name, node in self.nodes.items():
      async_result = pool.apply_async(node.backup, (force,))
      results[node.node_name] = async_result

    success_details = {}
    failure_details = {}
    for node_name, async_result in results.items():
      status, message, details = async_result.get()
      if status:
        success_details[node_name] = (status, message, details)
      else:
        failure_details[node_name] = (status, message, details)

    return (len(failure_details) == 0, success_details, failure_details)

  #def _backup_node(self, node, force=False):
  #  status, message, details = node.backup(force)
  #  if not status:
  #      return (status, message, details)
  #  return (True, 'success', 'Node %s backedup successfully'%(node.name))


  def _snapshot_backup(self):
    return self.store.create_snapshot(self.name)

  def _rotate_backups(self):
    return self.store.rotate_snapshots(self.name, self.keep)

  def post_backup(self, delete_snapshots):
    '''
    clean up the nodes after backup is complted like deleting temp folders or LVM snapshots
    :return:
    '''
    pool = ThreadPool(processes=len(self.nodes))
    results = {}
    for node in self.nodes.values():
      async_result = pool.apply_async(node.post_backup, (delete_snapshots,))
      results[node.node_name] = async_result

    for node_name, async_result in results.items():
      status, mesg, detail = async_result.get()
      if not status:
        raise Exception("Failed to complete post_backup procedure for %s, reason: %s" % (node_name, mesg))

  def pre_restore(self, force=False):
    snapshot = self.store.pre_restore(self.snapshot_name, self.name)
    pool = ThreadPool(processes=len(self.nodes))
    results = {}
    for node in self.nodes.values():
      async_result = pool.apply_async(node.pre_restore)
      results[node.node_name] = async_result

    for node_name, async_result in results.items():
      status, mesg, detail = async_result.get()
      if not status:
        raise Exception("Failed to run pre_restore for %s, reason: %s" % (node_name, mesg))

  def restore(self, force=False):
    pool = ThreadPool(processes=int(DEFAULT_NUM_THREADS))
    results = {}
    for node_name, node in self.nodes.items():
      async_result = pool.apply_async(node.restore, (force,))
      results[node.node_name] = async_result

    success_details = {}
    failure_details = {}
    for node_name, async_result in results.items():
      status, message, details = async_result.get()
      if status:
        success_details[node_name] = (status, message, details)
      else:
        failure_details[node_name] = (status, message, details)

    return (len(failure_details) == 0, success_details, failure_details)

  def post_restore(self, force=False):
    pool = ThreadPool(processes=len(self.nodes))
    results = {}
    for node in self.nodes.values():
      async_result = pool.apply_async(node.post_restore)
      results[node.node_name] = async_result

    for node_name, async_result in results.items():
      status, mesg, detail = async_result.get()
      if not status:
        raise Exception("Failed to complete post_restore procedure for %s, reason: %s" % (node_name, mesg))

  def run_backup(self, force=False):
    try:
      self.collect_node_details()
      self.pre_backup(force=force)
      backup_status = status, success_c,failure_c = self.backup(force=force)
    except Exception as ex:
      logging.error('Got exception while running backup procedure :%s' % (str(ex)))
      backup_status = False

    try:
      deleted_snapshots = []
      if backup_status:
        self._snapshot_backup()
        deleted_snapshots = self._rotate_backups()

      delete_names = []
      for deleted_snapshot in deleted_snapshots:
        delete_names.append(deleted_snapshot.snapshot_name)
      self.post_backup(delete_names)
    except Exception as ex:
      logging.error('Got exception while running post backup procedure :%s' % (str(ex)))

    self.dump_status('backup')
    display_status(os.path.join(self.store.backupdir, 'backup.yaml'))

  def run_restore(self, force=False):
    try:
      self.collect_node_details()
      self.pre_restore(force=force)
      self.restore(force=force)
      self.post_restore(force=force)
    except Exception as ex:
      logging.error('Got exception while running restore procedure :%s' % (str(ex)))

    self.dump_status('restore')
    display_status(os.path.join(self.store.backupdir, 'restore.yaml'))




"""
Prints out the usage for the command line.
"""
def usage():
  usage = ["backupmanager.py [-hnksctu]\n"]
  usage.append("  [-h | --help] prints this help and usage message\n")
  usage.append("  [-n | --name] backup namespace\n")
  usage.append("  [-k | --keep] number of backups to keep before deleting\n")
  usage.append("  [-s | --servers] comma separated list of the servers to backup, if remote\n")
  usage.append("  [-c | --config] configuration file for backup\n")
  usage.append("  [-t | --store] directory locally to store the backups\n")
  usage.append("  [-u | --user] the remote username used to ssh for backups\n")
  usage.append("  [-r | --restore] run restore instead of default backup job\n")
  usage.append("  [-S | --snapshot_name] name of the snapshot directory to restore\n")
  message = string.join(usage)
  print message

def _load_config(config_file):
  file_handle = open(config_file, 'r')
  config = pystache.render(file_handle.read(), dict(os.environ))
  return yaml.safe_load(config)



"""
Main method that starts up the backup.
"""
def main(argv):

  # set the default values
  pid_file = tempfile.gettempdir() + os.sep + "backupmanager.pid"
  name = None
  keep = None
  servers = None
  config_file = None
  store = None
  user = None
  restore = False
  snapshot_name = None

  try:

    # process the command line options
    opts, args = getopt.getopt(argv, "hn:k:s:c:t:u:rS:", ["help", "name=",
      "keep=", "servers=", "config=", "store=", "user=", "restore", "snapshot_name="])

    # if no arguments print usage
    if len(argv) == 0:
      usage()
      sys.exit()

    # loop through all of the command line options and set the appropriate
    # values, overriding defaults
    for opt, arg in opts:
      if opt in ("-h", "--help"):
        usage()
        sys.exit()
      elif opt in ("-n", "--name"):
        name = arg
      elif opt in ("-k", "--keep"):
        keep = int(arg)
      elif opt in ("-s", "--servers"):
        servers = ",".split(arg)
      elif opt in ("-c", "--config"):
        config_file = arg
      elif opt in ("-t", "--store"):
        store = arg
      elif opt in ("-u", "--user"):
        user = arg
      elif opt in ("-r", "--restore"):
        restore = True
      elif opt in ("-S", "--snapshot_name"):
        snapshot_name = arg

  except getopt.GetoptError, msg:
    # if an error happens print the usage and exit with an error
    usage()
    sys.exit(errno.EIO)

  # load the config file and update override with CLI
  if config_file == None:
    usage()
    sys.exit(errno.EPERM)

  config = _load_config(config_file)

  name = name if name is not None else config.get('name', DEFAULT_NAME)
  config['name'] = name

  keep = keep if keep is not None else config.get('keep', DEFAULT_KEEP)
  config['keep'] = keep

  user = user if user is not None else config.get('user', DEFAULT_USER)
  config['user'] = user

  store = store if store is not None else config.get('store', None)
  config['store'] = store

  servers = servers if servers is not None else config.get('servers', [])
  config['servers'] = servers

  config['snapshot_name'] = snapshot_name
  # check options are set correctly
  if store == None:
    usage()
    sys.exit(errno.EPERM)

  if restore and snapshot_name == None:
    usage()
    sys.exit(errno.EPERM)

  # process backup, catch any errors, and perform cleanup
  try:

    # another backup can't already be running, if pid file doesn't exist, then
    # create it
    if os.path.exists(pid_file):
      logging.warning("Backup/Restore running, %s pid exists, exiting." % pid_file)
      sys.exit(errno.EBUSY)
    else:
      pid = str(os.getpid())
      f = open(pid_file, "w")
      f.write("%s\n" % pid)
      f.close()

      if restore:
        backupmgr = BackupManager(config, snapshot_name=snapshot_name)
        backupmgr.run_restore(False)
      else:
        backupmgr = BackupManager(config)
        backupmgr.run_backup(False)

  except(Exception):
    logging.exception("Incremental backup failed.")
  finally:
    os.remove(pid_file)

# if we are running the script from the command line, run the main function
if __name__ == "__main__":
  #args = sys.argv[1:]
  logging.basicConfig(level=logging.DEBUG)
  args = []
  args.extend(['-n','dailyjuly19','-c', '../examples/example_backup.yaml', '-t', '/backups'])
  #args.extend(['-n', 'dailyjuly19', '-c', '../examples/example.yaml', '-t', '/backups', '-r', '-S', '2017-07-12T17:42:34'])
  main(args)
