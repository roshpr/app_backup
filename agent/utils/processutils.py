import subprocess
import shlex
import sys
import logging
log = logging.getLogger('BackupAgent')


def execute_long_process(command):
    '''
    execute a short command
    :param command:
    :return: return_code, out
        :return_code - return the exit code of the command
        :out - returns the response of the command
    '''
    args = shlex.split(command)
    log.debug('Command args {0}'.format(args))
    try:
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        # while True:
        #     out = process.stdout.read(1)
        #     if out == '' and process.poll() != None:
        #         break
        #     if out != '':
        #         sys.stdout.write(out)
        #         sys.stdout.flush()
        resp = out if process.returncode == 0 else err
        return process.returncode, resp
    except Exception as exp:
        log.exception('Command %s failed', command)
        return 1, str(exp)


def execute_process(command):
    '''
    execute a long running command
    :param command:
    :return:
    '''
    try:
        retcode = subprocess.call(command, shell=True)
        if retcode < 0:
            log.error('Subprocess failed with return code {0}'.format(retcode))
        return retcode
    except OSError as e:
        log.exception('Command %s failed', command)
        return -1

if __name__ == '__main__':
    execute_long_process("ps -ef")
