from agent.appcontrollers.abstractcontroller import AbstractController
from agent.utils.processutils import execute_process, execute_long_process
from agent.utils.utils import ensure_directory


class Mariadb(AbstractController):
    FIND_COMMAND = 'which mysqladmin'
    FLUSH_COMMAND = 'mysqladmin  -h {0} -u {1} -p{2} flush-hosts'
    DUMP_COMMAND = 'mysqldump --add-drop-table -h {0} -u {1} -p{2} --all-databases > {3}/{4}'
    RESTORE_COMMAND = 'mysql -h {0} -u {1} -p{2} < {3}/{4}'
    STOP_SERVICE = 'service mysql stop'
    START_SERVICE = 'service mysql start'
    DUMP_FILE = 'mariadbdump.sql'
    CLEAR_DUMP_DIR = 'rm -rf %s'
    FIND_MASTER = 'echo "SHOW MASTER STATUS;" | mysql -u {0} -p{1} | tail -n1'

    def __init__(self):
        super(Mariadb, self).__init__()
        self.DUMP_DIR = '%s/mariadb' % (self.LOCAL_DATA_DIR)
        self.app = self.__class__.__name__ = self.__class__.__name__

    def pre_backup(self, data):
        self.log.info('%s pre_backup', self.app)
        ret_code = execute_process(self.FIND_COMMAND)
        flush_ret_code = 0
        resp = 'success'
        if ret_code == 0:
            user = data.get('user', 'root')
            password = data.get('password', 'passw0rd')
            ip_address = data.get('host', '127.0.0.1')
            flush_ret_code, resp = execute_long_process(self.FLUSH_COMMAND.format(ip_address, user, password))
        return self.fetch_response(flush_ret_code, resp)

    def backup(self, data):
        self.log.info('%s backup', self.app)
        #
        ensure_directory(self.DUMP_DIR)
        # ret_code, resp = execute_long_process(self.dump_command % (data['path'], self.dump_file))
        user = data.get('user', 'root')
        password = data.get('password', 'passw0rd')
        ip_address = data.get('host', '127.0.0.1')
        ret_code = execute_process(self.DUMP_COMMAND.format(ip_address, user, password, self.DUMP_DIR, self.DUMP_FILE))
        if ret_code == 0:
            try:
                master = data.get('role', 'master')
                if master == 'member':
                    is_master = False
                else:
                    is_master = True
                self.start_backup_sync(data, is_master=is_master, src=self.DUMP_DIR)
            except Exception as e:
                err = 'Rsync to NFS directory from dump dir failed for MariaDB: %s' % (str(e))
                self.log.exception(err)
                return self.populate_response(status=err)
        else:
            return self.populate_response('MySql dump failed')
        self.log.info('######### %s backup successfully completed #########', self.app)
        return self.populate_response()

    def post_backup(self, data):
        self.log.info('%s post_backup', self.app)
        self.log.info('Deleting backup dump data directory: %s', self.DUMP_DIR)
        ret_code, resp = execute_long_process(self.CLEAR_DUMP_DIR % self.DUMP_DIR)
        return self.fetch_response(ret_code, resp)

    def pre_restore(self, data):
        self.log.info('%s pre_restore', self.app)
        # ret_code, resp = execute_long_process(self.stop_service)
        return self.populate_response()

    def restore(self, data):
        self.log.info('%s restore', self.app)
        ensure_directory(self.DUMP_DIR)
        try:
            # self.start_backup_sync(data, is_restore=True, src=self.DUMP_DIR)
            src = self.get_backup_component_target(data)
            user = data.get('user', 'root')
            password = data.get('password', 'passw0rd')
            ip_address = data.get('host', '127.0.0.1')
            ret_code = execute_process(self.RESTORE_COMMAND.format(ip_address, user, password, src, self.DUMP_FILE))
            if ret_code != 0:
                return self.populate_response(status='MariaDB Restore failed')

            self.log.debug('Computed source path: %s', src)
        except Exception as e:
            err = 'Rsync to data directory from NFS failed for MariaDB: %s' % (str(e))
            self.log.exception(err)
            return self.populate_response(status=err)

        self.log.info('######### %s restore successfully completed #########', self.app)
        return self.populate_response()


    def post_restore(self, data):
        self.log.info('%s post_restore', self.app)
        # ret_code, resp = execute_long_process(self.start_service)
        return self.populate_response()

    def describe_app(self, data=None):
        user = 'root'
        password = 'passw0rd'
        ip_address = '127.0.0.1'
        flush_ret_code, resp = execute_long_process(self.FIND_MASTER.format(ip_address, user, password))
        print resp

        role = 'member'
        if flush_ret_code == 0:
            if resp != '':
                role = 'master'


        return {
            'role': role,
            'supports': ['backup', 'restore'] if role == 'master' else []
        }

if __name__ == '__main__':
    mariadb = Mariadb()
    print(mariadb.get_role())