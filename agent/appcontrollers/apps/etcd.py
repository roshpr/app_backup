from agent.appcontrollers.abstractcontroller import AbstractController
from agent.utils.processutils import execute_process, execute_long_process
import json
import urllib2
import agent.utils.utils as utils



# TODO Data directory backup only for master
class Etcd(AbstractController):
    STOP_SERVICE = 'service etcd stop'
    START_SERVICE = 'service etcd start'
    FIND_LEADER_URL = "http://{0}:2379/v2/stats/self"
    MASTER_STATE = 'StateLeader'

    def pre_backup(self, data):
        self.log.info('%s pre_backup', self.__class__.__name__)
        return self.populate_response()

    def backup(self, data):
        self.log.info('%s backup', self.__class__.__name__)
        try:
            master = data.get('role', 'member')
            if master == 'member':
                is_master = False
            else:
                is_master = True
            is_master = False  # We will back all directories and restore all directories
            self.start_backup_sync(data, data['path'], is_master=is_master)
        except Exception as e:
            err = 'Rsync to data directory failed for etcd: %s' % (str(e))
            self.log.exception(err)
            return self.populate_response(status=err)
        self.log.info('######### %s backup successfully completed #########', self.__class__.__name__)
        return self.populate_response()

    def post_backup(self, data):
        self.log.info('%s backup', self.__class__.__name__)
        return self.populate_response()

    def pre_restore(self, data):
        self.log.info('%s pre_restore', self.__class__.__name__)
        ret_code, resp = execute_long_process(self.STOP_SERVICE)
        # if ret_code == 0:
        #     self.log.info('Backup data to local folder before restore')
        #     try:
        #         ret_code, resp = execute_long_process(self.BACK_DATA_DIR_4_RESTORE.format(data['path'], self.LOCAL_DATA_DIR))
        #         if ret_code != 0:
        #             return self.fetch_response(ret_code, resp)
        #     except Exception as exp:
        #         self.log.exception('ETCD pre restore failed')
        #         return self.populate_response(status=str(exp))
        # else:
        #     return self.fetch_response(ret_code, resp)
        return self.fetch_response(ret_code, resp)

    def restore(self, data):
        self.log.info('%s restore', self.__class__.__name__)
        try:
            master = data.get('role', 'master')
            if master == 'member':
                is_master = False
            else:
                is_master = True
            is_master = False # We will back all directories and restore all directories
            self.start_restore_sync(data, data['path'], is_master=is_master)
        except Exception as e:
            err = 'Rsync to data directory failed for etcd: %s' % (str(e))
            self.log.exception(err)
            return self.populate_response(status=err)
        self.log.info('######### %s restore successfully completed #########', self.__class__.__name__)
        return self.populate_response()

    def post_restore(self, data):
        self.log.info('%s post_restore', self.__class__.__name__)
        ret_code, resp = execute_long_process(self.START_SERVICE)
        return self.fetch_response(ret_code, resp)

    def describe_app(self, data=None):
        self.log.info('%s describe_app', self.__class__.__name__)
        sys_ip = utils.find_listen_ip('2379')
        node_leader_data = dict()
        try:
            self.log.debug('ETCD connect to URL: %s', self.FIND_LEADER_URL.format(sys_ip))
            node_leader_data = json.load(urllib2.urlopen(self.FIND_LEADER_URL.format(sys_ip)))
        except urllib2.URLError as exp:
            self.log.exception('ETCD URL connection error')
        self.log.debug('ETCD leader info %s', node_leader_data)
        if node_leader_data is not None and 'state' in node_leader_data:
            node_status = node_leader_data['state']
            if node_status == self.MASTER_STATE:
                role = 'master'
            else:
                role = 'member'
        else:
            role = 'master'
        role = 'member' # We will back all directories and restore all directories
        return {
            'role': role,
            'supports': ['backup', 'restore'] if role == 'master' else ['backup', 'restore']
        }
