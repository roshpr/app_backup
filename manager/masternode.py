import logging
import utils
import requests
import os
import json
import datetime
import yaml
import copy

class BackupNode:
  def __init__(self, store, components, **kwargs):
    self.store = store
    self.components = copy.deepcopy(components)
    self.node_config = None

    self.host = None
    self.port = kwargs.get('port', 443)
    self.protocol = kwargs.get('protocol','https')
    self.cluster = kwargs.get('cluster', '1')
    self.backup_name = kwargs.get('backup_name', 'backup')
    self.snapshot_time = kwargs.get('snapshot_time', datetime.datetime.utcnow())
    self.snapshot_name = kwargs.get('snapshot_name', self.snapshot_time.strftime('%Y-%m-%dT%H:%M:%S'))

    config = kwargs.get('config', None)
    if config:
      if type(config) is dict:
          self.host = config.get('host', kwargs.get('host', None))
          self.node_name = config.get('name', kwargs.get('name', self.host))
          self.port = config.get('port', self.port)
          #self.user = config.get('user', kwargs.get('user', None))
          self.protocol = config.get('protocol', self.protocol)
          self.cluster = config.get('cluster', self.cluster)
      else:
          self.host = config
          self.node_name = kwargs.get('name', self.host)

    else:
      self.host = kwargs.get('host', None)
      self.node_name = kwargs.get('name', self.host)

    if None in [self.host]:
      raise Exception('host argumet is required')


  def get_node_info(self, refresh=False):
      '''
      Collect node information which is useful to describe the backup and to understand what needs to be backedup
      from the node
      :return: (True, success, Details) or (False, Error Message, Details)
      '''
      '''
      structure of describe_node request and response from agent
      request:

      response:
        components:
            cassandra:
              role: member
              supports: [backup, restore]
            elasticserarch:
              role: master
              supports: [backup]
            mysql:
              role: slave
              supports: [backup, restore]

        config:
            hostname: host1
            ip: 192.168.1.1
            time: current time
            data: arbitary data that client wants to store
      '''
      if refresh or self.node_config is None:
          url = self._get_agent_url('describenode')
          try:
              r = requests.get(url)
              if not r.ok:
                  raise Exception('Got %s:%s response from %s'%(r.status_code, r.reason, self.node_name))
              rdata = r.json()
              info_comps = rdata.get('components', {})
              self.node_config = rdata.get('config', {})

              for key, info_comp in info_comps.items():
                  if key not in self.components.keys():
                      logging.warn('Ignoring node %s component %s not configured'%(self.node_name, key))
                      continue
                  self.components[key] = dict(self.components[key].items() + info_comp.items())

                  self.node_config = dict(self.node_config.items() + {
                          'node': self.node_name,
                          'host': self.host,
                          'cluster': self.cluster,
                          'backup_name': self.backup_name,
                          'snapshot_name': self.snapshot_name,
                          'snapshot_time': utils.totimestamp(self.snapshot_time)
                  }.items())

          except Exception as ex:
              self.node_config = None
              return (False, ex.message, ex)

      return (True, 'success', {'components':self.components, 'config': self.node_config})

  def pre_backup(self, force=False):
      '''
      Prepare the backup data on the node
      The node can do tasks like
      Create backup metadata, flush the buffers, freeze the applications, LVM snapshots etc
      :return: (True, success, Details) or (False, Error Message, Details)
      '''
      '''
        structure of pre_backup request and response
        request:
          cluster: central/regional
          server: server_ip
          nfs_share: exported path
          
          backup_name: name of the backup
          snapshot_name: name of the snapshot
          snapshot_time: future_time_stamp for snapshoting
          components:
            cassandra:
              path: /data/cassandra

        response:
          pre_backup: 'success' or 'error message'
          
          components:
            cassandra:
              pre_backup: 'success' or 'error message'

      '''

      server_ip = utils.get_ip_to(self.host, self.port)
      url = self._get_agent_url('prebackup')
      data = {
          'node`': self.node_name,
          'cluster': self.cluster,
          'server': server_ip,
          'nfs_share': self.store.backupdir,
          'backup_name': self.backup_name,
          'snapshot_name': self.snapshot_name,
          'snapshot_time': utils.totimestamp(self.snapshot_time),
          'components': {

          }
      }
      headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

      for key, comp in self.components.items():
          if 'backup' in comp.get('supports', []) or force:
              data['components'][key] = comp

      try:
        r = requests.post(url, data=json.dumps(data), headers=headers)
        if not r.ok:
            raise Exception('Got %s:%s response from %s' % (r.status_code, r.reason, self.node_name))
        rdata = r.json()
        rcomps = rdata.get('components', {})
        logging.info('Prepared %s backup components for %s' % (len(rcomps), self.node_name))

        for key, comp in rcomps.items():
            status = comp.get('pre_backup', 'unknown')
            logging.info('Prepare returned %s for component %s on %s' %(status, key, self.node_name))
            self.components[key]['pre_backup'] = status

        return (True, 'success', self.components)
      except Exception as ex:
        return (False, ex.message, ex)

  def backup(self, force=False):
      '''

      :param force:
      :return:
      '''
      '''
      structure of backup request and response
      request:
        server: server_ip
        nfs_share: exported path

        backup_name: name of the backup
        snapshot_name: name of the snapshot
        snapshot_time: future_time_stamp for snapshoting
        components:
          cassandra:
            path: /data/cassandra

      response:
        backup: 'success', 'error message'

        components:
          cassandra:
            backup: 'success' or 'error message'
      '''
      server_ip = utils.get_ip_to(self.host, self.port)
      url = self._get_agent_url('backup')
      data = {
          'node': self.node_name,
          'cluster': self.cluster,
          'server': server_ip,
          'nfs_share': self.store.backupdir,
          'backup_name': self.backup_name,
          'snapshot_name': self.snapshot_name,
          'snapshot_time': utils.totimestamp(self.snapshot_time),
          'components': {

          }
      }
      headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

      for key, comp in self.components.items():
          if 'success' in comp.get('pre_backup', '') or force:
              data['components'][key] = comp

      try:
        r = requests.post(url, data=json.dumps(data), headers=headers)
        if not r.ok:
            raise Exception('Got %s:%s response from %s' % (r.status_code, r.reason, self.node_name))
        rdata = r.json()
        rcomps = rdata.get('components', {})
        logging.info('Backedup %s components for %s' % (len(rcomps), self.node_name))

        for key, comp in rcomps.items():
            status = comp.get('backup', 'unknown')
            logging.info('Backup returned %s for component %s on %s' %(status, key, self.node_name))
            self.components[key]['backup'] = status

        self.dump_status('backup')
        return (True, 'success', self.components)
      except Exception as ex:
        return (False, ex.message, ex)


  def post_backup(self, delete_snapshots=[]):
      '''
      Does cleanup stuff  after the backup is complete like deleting LVM snapshot and files etc
      :return: (True, success, Details) or (False, Error Message, Details)
      '''
      url = self._get_agent_url('postbackup')
      server_ip = utils.get_ip_to(self.host, self.port)
      data = {
          'server': server_ip,
          'cluster': self.cluster,
          'nfs_share': self.store.backupdir,
          'backup_name': self.backup_name,
          'snapshot_name': self.snapshot_name,
          'snapshot_time': utils.totimestamp(self.snapshot_time),
          'delete_snapshots': delete_snapshots,
          'components': {

          }
      }
      headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

      for key, comp in self.components.items():
          if 'success' in comp.get('pre_backup', '') or 'success' in comp.get('backup',''):
              data['components'][key] = comp

      try:
          r = requests.post(url, data=json.dumps(data), headers=headers)
          if not r.ok:
              raise Exception('Got %s:%s response from %s' % (r.status_code, r.reason, self.node_name))

          rdata = r.json()
          rcomps = rdata.get('components', {})
          logging.info('Postbackup %s components for %s' % (len(rcomps), self.node_name))

          for key, comp in rcomps.items():
              status = comp.get('post_backup', 'unknown')
              logging.info('Postbackup returned %s for component %s on %s' % (status, key, self.node_name))
              self.components[key]['post_backup'] = status

          self.dump_status('backup')
          return (True, 'Post backup completed successfully for %s' % (self.node_name), r.json())
      except Exception as ex:
          return (False, ex.message, str(ex))

  def rsync_backup(self, force=False):
      success_details = {}
      failed_details = {}

      node_store = utils.ensure_directory(self.store)
      for key, component in self.components.items():
          enabled = component.get('enabled', force)
          prepared = component.get('prepared', force)
          if not enabled or not prepared:
              logging.info('Skipping component %s:%s as node suggested enabled:%s, prepared:%s' % (self.node_name, key, enabled, prepared))
              continue

          comp_store = utils.ensure_directory(node_store, key)
          source = component['path']
          excludes = component['excludes']
          status, message, detail = self._rsync_backup(comp_store,  source, excludes)
          component['backup_status'] = status
          component['backup_message'] = message

          if status:
              success_details[key] = (status, message, detail)
          else:
              failed_details[key] = (status, message, detail)

      if len(failed_details) > 0:
          return (False, success_details, failed_details)
      else:
          return (True, success_details, failed_details)


  def _rsync_backup(self, store, source, excludes):
      '''
      Does the actual backup of data from node to the backup store
      :return: (True, success, Details) or (False, Error Message, Details)
      '''

      try:
          rsync_base = ["rsync", "-avR", "--ignore-errors", "--delete", "--delete-excluded"]
          rsync_cmd = rsync_base[:]
          for exclude in excludes:
            rsync_cmd.extend(["--exclude", exclude])

          if self.host:
              source = self.user + "@" + self.host + ":" + source

          rsync_cmd.append(source)
          rsync_cmd.append(store)
          status, code, output = utils.run_command(command=rsync_cmd)
          if not status:
              return (False, 'Failed to run command', output)

          return (True, 'success', 'Backed up %s to %s' % (source, store))
      except Exception as ex:
          return (False, 'Exception while backup', str(ex))

  def pre_restore(self):
      server_ip = utils.get_ip_to(self.host, self.port)
      url = self._get_agent_url('prerestore')
      data = {
          'node': self.node_name,
          'cluster': self.cluster,
          'server': server_ip,
          'nfs_share': self.store.collect_snapshot(self.snapshot_name, self.backup_name).path,
          'backup_name': self.backup_name,
          'snapshot_name': self.snapshot_name,
          'snapshot_time': utils.totimestamp(self.snapshot_time),
          'components': {

          }
      }
      headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

      for key, comp in self.components.items():
          if 'restore' in comp.get('supports', []):
              data['components'][key] = comp

      try:
          r = requests.post(url, data=json.dumps(data), headers=headers)
          if not r.ok:
              raise Exception('Got %s:%s response from %s' % (r.status_code, r.reason, self.node_name))
          rdata = r.json()
          rcomps = rdata.get('components', {})
          logging.info('Prepared %s restore components for %s' % (len(rcomps), self.node_name))

          for key, comp in rcomps.items():
              status = comp.get('pre_restore', 'unknown')
              logging.info('Prerestore returned %s for component %s on %s' % (status, key, self.node_name))
              self.components[key]['pre_restore'] = status

          return (True, 'success', self.components)
      except Exception as ex:
          return (False, ex.message, ex)

  def restore(self, force=False):
    '''
    structure of backup request and response
    request:
      server: server_ip
      nfs_share: exported path

      backup_name: name of the backup
      snapshot_name: name of the snapshot
      snapshot_time: future_time_stamp for snapshoting
      components:
        cassandra:
          path: /data/cassandra

    response:
      backup: 'success', 'error message'

      components:
        cassandra:
          backup: 'success' or 'error message'
    '''
    server_ip = utils.get_ip_to(self.host, self.port)
    url = self._get_agent_url('restore')
    data = {
      'node': self.node_name,
      'cluster': self.cluster,
      'server': server_ip,
      'nfs_share': self.store.collect_snapshot(self.snapshot_name, self.backup_name).path,
      'backup_name': self.backup_name,
      'snapshot_name': self.snapshot_name,
      'snapshot_time': utils.totimestamp(self.snapshot_time),
      'components': {
      }
    }

    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

    for key, comp in self.components.items():
      if 'success' == comp.get('pre_restore', '') or force:
         data['components'][key] = comp

    try:
       r = requests.post(url, data=json.dumps(data), headers=headers)
       if not r.ok:
         raise Exception('Got %s:%s response from %s' % (r.status_code, r.reason, self.node_name))
       rdata = r.json()
       rcomps = rdata.get('components', {})
       logging.info('Restored %s components for %s' % (len(rcomps), self.node_name))

       for key, comp in rcomps.items():
          status = comp.get('restore', 'unknown')
          logging.info('Restore returned %s for component %s on %s' % (status, key, self.node_name))
          self.components[key]['restore'] = status

       self.dump_status('restore')
       return (True, 'success', self.components)
    except Exception as ex:
      return (False, ex.message, ex)

  def post_restore(self):
      url = self._get_agent_url('postrestore')
      server_ip = utils.get_ip_to(self.host, self.port)
      data = {
          'node': self.node_name,
          'server': server_ip,
          'cluster': self.cluster,
          'nfs_share': self.store.collect_snapshot(self.snapshot_name, self.backup_name).path,
          'backup_name': self.backup_name,
          'snapshot_name': self.snapshot_name,
          'snapshot_time': utils.totimestamp(self.snapshot_time),
          'components': {

          }
      }
      headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

      for key, comp in self.components.items():
          if 'success' == comp.get('pre_restore', '') or 'success' in comp.get('restore', []):
              data['components'][key] = comp

      try:
          r = requests.post(url, data=json.dumps(data), headers=headers)
          if not r.ok:
              raise Exception('Got %s:%s response from %s' % (r.status_code, r.reason, self.node_name))

          rdata = r.json()
          rcomps = rdata.get('components', {})
          logging.info('Postrestore %s components for %s' % (len(rcomps), self.node_name))

          for key, comp in rcomps.items():
              status = comp.get('post_restore', 'unknown')
              logging.info('Postrestore returned %s for component %s on %s' % (status, key, self.node_name))
              self.components[key]['post_restore'] = status

          self.dump_status('restore')
          return (True, 'Post restore completed successfully for %s' % (self.node_name), r.json())
      except Exception as ex:
          return (False, ex.message, str(ex))


  def _get_agent_url(self, path):
      return self.protocol + '://' + self.host + ':' + str(self.port) + '/' + path

  def get_status(self):
      info_data = {
          'components': self.components,
          'config': self.node_config
      }

      return info_data

  def dump_status(self, prefix):
      dump_path = os.path.join(self.store.backupdir, prefix + '_' + self.node_name + '.yaml')
      status_data = self.get_status()
      try:
        os.remove(dump_path)
      except:
        pass

      with open(dump_path, 'w') as outfile:
          yaml.dump(status_data, outfile, indent=2)

if __name__ == "__main__":
  pass
