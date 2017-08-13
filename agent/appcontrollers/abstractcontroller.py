import abc
import os
from agent.utils.utils import rsync_archive
import logging
from agent import constants


class AbstractController(object):
    __metaclass__ = abc.ABCMeta
    log = None
    LOCAL_DATA_DIR = constants.DUMP_DATA_DIR
    BACKUP_DIR = constants.BACK_UP_DIR
    BACK_DATA_DIR_4_RESTORE = 'cp -al {0} {1}'

    def __init__(self):
        self.log = logging.getLogger('BackupAgent')

    def loaded(self):
        self.log.info('Loaded %s', self.__class__.__name__)

    @abc.abstractmethod
    def pre_backup(self, data):
        '''
        Prepare for backup
        :param data:
        :return:
        '''

    @abc.abstractmethod
    def backup(self, data):
        '''
        Backup content specific to application
        :param data:
        :return:
        '''

    @abc.abstractmethod
    def post_backup(self, data):
        '''
        Prepare for backup
        :param data:
        :return:
        '''

    @abc.abstractmethod
    def pre_restore(self, data):
        '''
        Prepare for backup
        :param data:
        :return:
        '''

    @abc.abstractmethod
    def restore(self, data):
        '''
        Prepare for backup
        :param data:
        :return:
        '''

    @abc.abstractmethod
    def post_restore(self, data):
        '''
        Prepare for backup
        :param data:
        :return:
        '''

    def fetch_response(self, ret_code, resp):
        if ret_code == 0:
            return self.populate_response()
        else:
            return self.populate_response(resp)

    def populate_response(self, status='success'):
        return {'status': status}

    def check_data_folder(self, data):
        '''
        Check & create a folder
        Args:
            path:
        Returns: True/ False
        '''
        path = self.get_backup_component_target(data)
        ret = os.path.exists(path)
        if not ret:
            os.makedirs(path)
        return True

    def get_backup_name(self, data):
        return data['backup_name']

    def get_snapshot_name(self, data):
        return data['snapshot_name']

    def get_snapshot_time(self, data):
        return data['snapshot_time']

    def get_backup_node_target(self, data):
        # return the target directory to copy backup data, this will create one directory per node in the cluster
        return os.path.join(self.BACKUP_DIR, data['cluster'], data['component'], data['node'])

    def get_backup_component_target(self, data):
        # return the target directory to copy backup data, this will create global directory common to all components
        return os.path.join(self.BACKUP_DIR, data['cluster'], data['component'])

    def get_delete_snapshots(self, data):
        return data.get('delete_snapshots', [])

    def start_backup_sync(self, data, src, is_master=False):
        self.log.info('Start syncing data to backup dir %s', self.__class__.__name__)
        if is_master:
            tgt = self.get_backup_component_target(data)
        else:
            tgt = self.get_backup_node_target(data)
        self.log.debug('Computed Target (src for restore) path: %s', tgt)
        component_path = '{0}/'.format(src)
        resp = rsync_archive(component_path, tgt)

        return resp

    def start_restore_sync(self, data, tgt, is_master=False):
        self.log.info('Start restoring data to app dir %s', self.__class__.__name__)
        if is_master:
            src = self.get_backup_component_target(data)
        else:
            src = self.get_backup_node_target(data)
        src_data_path = '{0}/'.format(src)
        self.log.debug('Computed Source path: %s', src_data_path)
        resp = rsync_archive(src_data_path, tgt)


    def describe_app(self, data=None):
        return {
            'role': 'member',
            'supports': ['backup', 'restore']
        }




