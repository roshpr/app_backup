from agent.constants import *
from agent.utils.utils import ensure_directory
from agent.utils.processutils import execute_process
import glob, os, sys, pwd
import logging
log = logging.getLogger('BackupAgent')


def backup_data_before_restore(src_dir, tgt_dir, snapshotname):
    log.info('backup cassandra file locally')
    if is_cassandra_running():
        log.info('Stop Cassandra before running the script')
        return False
    backup_cmd = 'cp -al {0} {1}'
    tgt_dir_ext = '{0}/restore/{1}'.format(tgt_dir, snapshotname)
    ensure_directory(tgt_dir_ext)
    ret_code = execute_process(backup_cmd.format(src_dir, tgt_dir_ext))
    if ret_code == 0:
        print('Successfully copied to dump')
        # db_file_path = '{0}/data/*/*/*.db'.format(src_dir)
        # delete_db_files = 'rm -f {0}'.format(db_file_path)
        # print('Command to delete: {0}'.format(delete_db_files))
        # if execute_process(delete_db_files) == 0:
        #     print('DB files deleted successfully')
    else:
        print('Local backup failed')
        return False
    return True


def restore_data_from_snapshot(src_dir, tgt_dir, snapshotname):
    '''
    Copy contents from snapshots of each schema/table/snapshot to target schema/table
    Args:
        src_dir: The path to the snapshot dir. Ex: /backup/cassandra/2017-07-12:10-20
        tgt_dir: The path to the cassandra data store. Ex: /var/lib/cassandra
        snapshotname: The name of the snapshot taken during backup.

    Returns:

    '''
    log.info('Restore snapshot')
    if is_cassandra_running():
        log.info('Stop Cassandra before running the script')
        return False

    tgt_data_dir = os.path.join(tgt_dir, 'data')

    # get owner from parent folder
    parent_owner = pwd.getpwuid(os.stat(tgt_dir).st_uid).pw_name

    # delete commit logs
    del_commitlogs_cmd = 'rm -f {0}/commitlog/*.log'.format(tgt_dir)
    ret_code = execute_process(del_commitlogs_cmd)
    if ret_code != 0:
        raise Exception("Failed to delete commit logs")

    del_dbfiles_cmd = 'rm -fr /data/*'
    ret_code = execute_process(del_commitlogs_cmd)
    if ret_code != 0:
        raise Exception("Failed to delete commit logs")

    # Both snapshot dir & target dir are same cause we have chdir into basepath of snapshot
    snapshot_dir = '{0}/data/*/*'.format(src_dir)
    # Derive Data directories
    tgt_dir_regex = '{0}/data/*/*'.format(tgt_dir)
    log.info('Cassandra snapshot dir: %s', snapshot_dir)
    log.info('Cassandra target dir: %s', tgt_dir)
    listof_tgt_dir_path = glob.glob(tgt_dir_regex)
    dict_tgt_path = dict((os.path.basename(tgt_path),tgt_path) for tgt_path in listof_tgt_dir_path)
    # Derive Snapshot directories
    os.chdir(src_dir)
    listof_table_path = glob.glob(snapshot_dir)
    for table_path in listof_table_path:
        table_base_path = os.path.basename(table_path)
        table_ss_path = '/{0}/snapshots/{1}/*'.format(table_path[1:], snapshotname)

        copy_command = 'cp -r {0} {1}'
        if table_base_path in dict_tgt_path.keys():
            copy_command_populated = copy_command.format(table_ss_path, dict_tgt_path.get(table_base_path))
            log.info(copy_command_populated)
            ret_code = execute_process(copy_command_populated)
            if ret_code == 0:
                log.info('copying {0} files'.format(table_ss_path))
            else:
                log.info('copying {0} files failed'.format(table_ss_path))
    return True

def is_cassandra_running():
    grep_cassandra_cmd = 'ps ax| grep cassandra | grep -v "grep" | grep -v cassandra_restore'
    ret_code = execute_process(grep_cassandra_cmd)
    if ret_code == 0:
        return True
    else:
        return False

if __name__ == '__main__':
    if not is_cassandra_running():
        if len(sys.argv) > 3:
            snapshotname = sys.argv[1]
            cassandra_data_path = sys.argv[2]
            snapshot_basepath = sys.argv[3]
            backup_data_before_restore(cassandra_data_path, DUMP_DATA_DIR, snapshotname)

            restore_data_from_snapshot(snapshot_basepath, cassandra_data_path, snapshotname)
        else:
            log.info('Usage: cassandra_restore.py <snapshotname> <cassandra data path> <snapshot_base_path>')
    else:
        log.info('Stop Cassandra before running the script')