from agent.appcontrollers.abstractcontroller import AbstractController
import agent.utils.utils as utils
import datetime
import os
import re
from agent.utils.processutils import execute_long_process

from agent.utils.es_manager import *

# TODO : Get snapshot name & Indexes name from Backup Master
# TODO : Add data directory to elasticsearch.yaml file under path.repo config \
# TODO path.repo: ["/mount/backups", "/mount/longterm_backups", "/mount/data/elasticsearch"]

class Elasticsearch(AbstractController):
    IP_ADDRESS = '127.0.0.1'
    STOP_SERVICE = 'service elasticsearch stop'
    START_SERVICE = 'service elasticsearch start'
    FIND_MASTER_URL = '/_cat/master'

    def pre_backup(self, data):
        self.log.info('%s pre_backup', self.__class__.__name__)
        self.configure_es_backup(data)
        return self.populate_response()

    def backup(self, data):
        self.log.info('%s backup', self.__class__.__name__)
        master = data.get('role', 'master')
        self.take_snapshot(data)

        self.log.info('######### %s backup successfully completed #########', self.__class__.__name__)
        return self.populate_response()

    def post_backup(self, data):
        self.log.info('%s post_backup', self.__class__.__name__)
        self.delete_snapshots(data)
        return self.populate_response()

    def pre_restore(self, data):
        self.log.info('%s pre_restore', self.__class__.__name__)
        path = self.get_backup_component_target(data)
        if not os.path.exists(path):
           raise Exception('Path %s does not exist')
        self.configure_es_backup(data)
        return self.populate_response()

    def restore(self, data):
        self.log.info('%s restore', self.__class__.__name__)
        self.restore_snapshot(data)
        self.log.info('######### %s backup successfully completed #########', self.__class__.__name__)
        return self.populate_response()

    def post_restore(self, data):
        self.log.info('%s post_restore', self.__class__.__name__)
        self.delete_snapshots(data)
        return self.populate_response()

    def configure_es_backup(self, data):
        location = self.get_backup_component_target(data)
        utils.ensure_directory(location)
        repo = self.get_backup_name(data).lower()
        connection = self._get_es_connection(data)
        payload = {
            'type': 'fs',
            'settings': {
                'location': location
            }
        }

        try:
            self.log.info("Configuring elastic repo %s" % (repo))
            connection.request('PUT', '/_snapshot/%s' % (repo), json.dumps(payload))
            result = connection.getresponse()
            if result.status != 200:
                raise Exception('status: {0}, reason: {1}, data: {2}'.format(result.status, result.reason, result.read()))
        except Exception as ex:
            raise Exception("Elasticsearch configure backup failed: " + str(ex))

    def take_snapshot(self, data):
        self.log.info('take snapshot')
        repo = self.get_backup_name(data).lower()
        snapshot_name = self._get_snapshot_name(data)
        connection = self._get_es_connection(data)
        # TODO Add indices once provided in data
        # pay_load = '{"indices":"'+indices+'","ignore_unavailabe":"true","include_global_state":"false"}'
        pay_load = '{"ignore_unavailabe":"true","include_global_state":"false"}'
        snapshot_time = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
        # TODO: change snapshot name by getting from backup controller

        try:
            self.log.info("Creating elastic snapshot %s" % (snapshot_name))
            connection.request('PUT', '/_snapshot/'+repo+'/'+snapshot_name+'?wait_for_completion=true', pay_load)
            result = connection.getresponse()
            if result.status != 200:
                raise Exception('status: {0}, reason: {1}, data: {2}'.format(result.status, result.reason, result.read()))
        except Exception as ex:
            raise Exception("Elasticsearch snapshot backup failed: " + str(ex))


    def _find_matching_es_snapshot(self, data, lookup_name):
        location = self.get_backup_component_target(data)
        index_file = os.path.join(location, 'index')
        data = {}
        if os.path.exists(index_file):
            data = json.load(file(index_file, 'r'))
        es_snapshots_names = data.get('snapshots', [])

        match = SNAPSHOT_PATTERN.search(lookup_name)
        lookup_timestamp = utils.totimestamp(datetime.datetime(*(int(group, 10) for group in match.groups('0'))))
        best_match_timestamp = 0
        best_match_name = None

        for snapshot_name in es_snapshots_names:
            match = SNAPSHOT_PATTERN.search(snapshot_name)
            timestamp = utils.totimestamp(datetime.datetime(*(int(group, 10) for group in match.groups('0'))))
            if lookup_timestamp == timestamp:
                best_match_name = snapshot_name
                break;

            if timestamp > best_match_timestamp and timestamp < lookup_timestamp:
                best_match_timestamp = timestamp
                best_match_name = snapshot_name

        return best_match_name


    def close_indices(self, data):
        connection = self._get_es_connection(data)
        try:
            connection.request('POST', '/_all/_close')
            result = connection.getresponse()
            if result.status != 200:
                raise Exception(
                    'status: {0}, reason: {1}, data: {2}'.format(result.status, result.reason, result.read()))
        except Exception as ex:
            raise Exception("Failed to close indices: " + str(ex))

    def open_indices(self, data):
        connection = self._get_es_connection(data)
        try:
            connection.request('POST', '/*/_open')
            result = connection.getresponse()
            if result.status != 200:
                raise Exception(
                    'status: {0}, reason: {1}, data: {2}'.format(result.status, result.reason, result.read()))
        except Exception as ex:
            raise Exception("Failed to open indices: " + str(ex))

    def restore_snapshot(self, data):
        self.log.info('restore snapshot')
        repo = self.get_backup_name(data).lower()
        lookup_name = self._get_snapshot_name(data)
        snapshot_name = self._find_matching_es_snapshot(data, lookup_name)
        if snapshot_name is None:
            raise Exception("Elastic snapshot not found for %s" % (lookup_name))

        self.close_indices(data)
        connection = self._get_es_connection(data)
        # TODO Add indices once provided in data
        # pay_load = '{"indices":"'+indices+'","ignore_unavailabe":"true","include_global_state":"false"}'
        pay_load = '{"include_global_state":"false"}'
        snapshot_time = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
        # TODO: change snapshot name by getting from backup controller

        try:
            self.log.info("Restoring elastic snapshot %s" % (snapshot_name))
            connection.request('POST', '/_snapshot/'+repo+'/'+snapshot_name+'/_restore'+'?wait_for_completion=true', pay_load)
            result = connection.getresponse()
            if result.status != 200:
                raise Exception('status: {0}, reason: {1}, data: {2}'.format(result.status, result.reason, result.read()))
        except Exception as ex:
            raise Exception("Elasticsearch snapshot restore failed: " + str(ex))

        #self.open_indices(data)

    def delete_snapshots(self, data):
        self.log.info('delete snapshots')
        repo = self.get_backup_name(data).lower()
        snapshots = self.get_delete_snapshots(data)

        status = 'success'
        es_snapshots = []
        for snapshot in snapshots:
            es_snapshot = self._find_matching_es_snapshot(data, snapshot.lower())
            if es_snapshot:
                es_snapshots.append(es_snapshot)
        connection = self._get_es_connection(data)
        for snapshot in es_snapshots:
            try:
                self.log.info("Deleting elastic snapshot %s"%(snapshot))
                connection.request('DELETE', '/_snapshot/' + repo + '/' + snapshot + '?wait_for_completion=true')
                result = connection.getresponse()
                if result.status != 200:
                    status += 'status: {0}, reason: {1}, data: {2}'.format(result.status, result.reason, result.read())
            except Exception as ex:
                raise Exception("Elasticsearch snapshot delete failed: " + str(ex))

        if status != 'success':
            raise Exception("Elasticsearch snapshot delete failed: " + status)


    def _get_snapshot_name(self, data):
        return self.get_snapshot_name(data).lower()

    def _get_es_connection(self, data):
        port = data.get('port', '9200')
        current_ip = data.get('host', utils.find_listen_ip(port))
        connection = httplib.HTTPConnection('{0}:{1}'.format(current_ip, port))
        return connection

    def reindex(self, options):
        try:
            es_maanager = ElasticSearchMgr(
                keyip=options.keystoneip,
                keyuser=options.keystoneuser,
                keypass=options.keystonepassword,
                keyport=options.keystoneport,
                ms_ip=options.microserviceipport,
                indexfile=options.indexfile
            )
            es_maanager.reindex()
        except Exception as exp:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=12, file=sys.stdout)


    def describe_app(self, data=None):
        # curl -XGET 'http://10.204.248.34:9200/_cluster/state?pretty'
        # master_node
        # curl -XGET 'http://10.204.248.34:9200/_cluster/state/nodes?pretty'
        # TODO get ports from data. Data tobe supplied to describenode
        role = 'member'
        try:
            port = '9200'
            listen_ip = utils.find_listen_ip(port)
            connection = httplib.HTTPConnection('{0}:{1}'.format(listen_ip, port))
            connection.request('GET',self.FIND_MASTER_URL)
            result = connection.getresponse()
            if result.status != 200:
                raise Exception(
                    'status: {0}, reason: {1}, data: {2}'.format(result.status, result.reason, result.read()))
            data = result.read()
            master_ip = data.split()[1]
            if master_ip == '127.0.0.1' or master_ip in utils.get_local_ips():
                role = 'master'
        except Exception as ex:
            self.log.exception('Elasticsearch get master failed')
            raise Exception("Elasticsearch get master failed: " + str(ex))

        return {
            'role': role,
            'supports': ['backup', 'restore'] if role == 'master' else []
        }

SNAPSHOT_PATTERN = re.compile(r'''
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

if __name__ == '__main__':
    es = Elasticsearch()
    print(es.get_role())