from agent.appcontrollers.abstractcontroller import AbstractController
from agent.utils.processutils import execute_long_process, execute_process
from agent.utils import utils
from agent.utils import cassandra_restore
import time
import os, glob

FIND_NODETOOL = 'which "nodetool"'
NODETOOL_FLUSH = 'nodetool flush'
SNAPSHOT_NAME = '0000'


class Cassandra(AbstractController):
    STOP_SERVICE = 'service cassandra stop'
    START_SERVICE = 'service cassandra start'
    NODETOOL_REPAIR = 'nodetool repair'

    def pre_backup(self, data):
        self.log.info('%s pre_backup', self.__class__.__name__)
        ret_code = execute_process(FIND_NODETOOL)
        if ret_code != 0:
            resp = 'nodetool not available in the system required for backup'
            return self.populate_response(resp)


        self.log.info('find nodetool returned {0}'.format(ret_code))
        return self.populate_response()

    def backup(self, data):
        self.log.info('%s backup', self.__class__.__name__)

        self.delete_snapshot(data)
        #snapshot_name = self.get_backup_name(data)
        snapshot_cmd = ['nodetool', 'snapshot', '-t', SNAPSHOT_NAME]
        utils.run_command(snapshot_cmd)

        data_dir = data.get('data_dir', data['path'])
        target_dir = self.get_backup_node_target(data)
        utils.ensure_directory(target_dir)

        rsync_cmd = ["rsync", "-a", "--ignore-errors", "--delete", "--delete-excluded", "--prune-empty-dirs"]
        rsync_cmd.extend(["--include=**/snapshots/" + SNAPSHOT_NAME + "/**"])
        rsync_cmd.extend(["--include=*/", "--exclude=*"])
        rsync_cmd.extend([data_dir+'/', target_dir+'/'])
        utils.run_command(rsync_cmd)
        self.log.info('######### %s backup successfully completed #########', self.__class__.__name__)
        return self.populate_response()

    def post_backup(self, data):
        self.log.info('%s post_backup', self.__class__.__name__)
        self.delete_snapshot(data)
        return self.populate_response()

    def pre_restore(self, data):
        self.log.info('%s pre_restore', self.__class__.__name__)
        ret_code, resp = execute_long_process(self.STOP_SERVICE)
        return self.fetch_response(ret_code, resp)
        #return self.populate_response()

    def restore(self, data):
        self.log.info('%s restore', self.__class__.__name__)

        try:
            #check if cassandra is running
            status, ret, output = utils.ps_grep('org.apache.cassandra')
            if ret == 0:
                raise Exception("Restore can not run while cassandra is still running")

            #snapshot_name = self.get_backup_name(data)
            cassandra_lib_path = data['path']
            tgt_data_dir = os.path.join(cassandra_lib_path, 'data')

            # get owner from parent folder
            data_owner, data_group, _, _ = utils.get_owner(tgt_data_dir)

            # delete commit logs
            del_commitlogs_cmd = 'rm -f {0}/commitlog/*.log'.format(cassandra_lib_path)
            status, code, _ = utils.run_command(del_commitlogs_cmd, shell=True, ignore=True)
            if code != 0:
                raise Exception("Failed to delete commit logs")

            del_data_cmd = 'rm -fr {0}'.format(os.path.join(tgt_data_dir, '*'))
            status, code, _ = utils.run_command(del_data_cmd, shell=True, ignore=True)
            if code != 0:
                raise Exception("Failed to delete data folder")

            src_basedir = self.get_backup_node_target(data)
            src_files_regex = '{0}/data/*/*'.format(src_basedir)
            src_dir_list = glob.glob(src_files_regex)
            for src_dir in src_dir_list:
                rel_dir = os.path.relpath(src_dir, src_basedir)
                cp_tgt_dir = os.path.join(cassandra_lib_path, rel_dir)
                cp_src_dir = os.path.join(src_dir, 'snapshots', SNAPSHOT_NAME)
                utils.ensure_directory(cp_tgt_dir, user=data_owner, group=data_group, mode='766')
                for f in os.listdir(cp_src_dir):
                    cp_cmd = ['cp', '-ar', os.path.join(cp_src_dir,f), cp_tgt_dir]
                    utils.run_command(cp_cmd)

        except Exception as ex:
            self.log.exception('Cassandra restore failed')
            return self.populate_response(status='Cassandra restore failed: %s'.format(str(ex)))
        return self.populate_response()

    def post_restore(self, data):
        self.log.info('%s post_restore', self.__class__.__name__)
        ret_code, resp = execute_long_process(self.START_SERVICE)
        if ret_code == 0:
            time.sleep(10)
            retry_cnt = 0
            while retry_cnt < 5:
                nodetool_repair_cmd = self.NODETOOL_REPAIR
                repair_ret_code = execute_process(nodetool_repair_cmd)
                if repair_ret_code == 0:
                    self.log.info('Nodetool repair successfully completed')
                    break
                else:
                    self.log.info('Nodetool repair failed. Retry count %s nodetool repair', str(retry_cnt))
                    retry_cnt += 1
                    time.sleep(10)
        else:
            return self.fetch_response(ret_code, resp)
        return self.populate_response()

    def delete_snapshot(self, data):
        #snapshot_name = self.get_backup_name(data)
        snapshot_cmd = ['nodetool', 'clearsnapshot', '-t', SNAPSHOT_NAME]
        utils.run_command(snapshot_cmd, ignore=True)

if __name__ == '__main__':
    cas = Cassandra()
    cas.pre_backup('')