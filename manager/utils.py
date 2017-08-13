import logging
import subprocess
import os
import pwd, grp
import errno
from datetime import datetime

def run_command(command, output=True, shell=False, ignore=False, retry_sudo=False,  **kwargs):

    try:
        logging.info('Running command: %s' % " ".join(command) if type(command) is list else command)
        if output:
            output = subprocess.check_output(command, shell=shell, stderr=subprocess.STDOUT, **kwargs)
            return (True, 0, output)
        else:
            retcode = subprocess.call(command, shell=shell, stderr=subprocess.STDOUT, **kwargs)
            return (True, retcode, None)

    except subprocess.CalledProcessError as ex:
        if retry_sudo and 'sudo' not in command and ( "Permission denied" in ex.output or "only root can do that" in ex.output):
            if type(command) is list:
                sudo_cmd = ['sudo'] + command
            else:
                sudo_cmd = 'sudo ' + command
            logging.info('Retrying command: %s' % " ".join(sudo_cmd) if type(sudo_cmd) is list else sudo_cmd)
            return run_command(sudo_cmd, output, shell=shell, ignore=ignore, retry_sudo=False, **kwargs)

        logging.error('Failed to run command %s details:%s,%s' % (ex.cmd, ex.returncode, ex.output))
        if ignore:
            return (False, ex.returncode, ex.output)
        else:
            raise Exception('Failed to run command %s' % ex.cmd, ex.returncode, ex.output)

def ps_grep(pattern, excludes=[]):
    ps_cmd = 'sudo ps -ax | grep "' + pattern + '" | grep -v "grep"'
    for exclude in excludes:
        ps_cmd = ps_cmd + ' | grep -v "' + exclude + '"'
    return run_command(ps_cmd, output=True, shell=True, ignore=True)

def ensure_directory(*paths, **kwargs):
    path = os.path.join(*paths)

    mode = '777' #kwargs.get('mode',0777)
    owner, group, _, _ = get_owner()
    owner = kwargs.get('user', owner)
    group = kwargs.get('group', group)

    try:
        os.makedirs(path )
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            if not os.path.exists(path):
                run_command(command=['mkdir', '-p', path], retry_sudo=True)
    run_command(command=['sudo', 'chmod', mode, path])
    run_command(command=['sudo', 'chown', owner+':'+group, path])
    return path

def get_owner(path=None):
    uid = os.getuid()
    gid = os.getgid()

    if path is not None:
        uid = os.stat(path).st_uid
        gid = os.stat(path).st_gid

    return pwd.getpwuid(uid).pw_name, grp.getgrgid(gid).gr_name, uid, gid


def get_ip_to(node, port):
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((node, port))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        logging.warn('Failed to resolve ip address by connecting to node %s:%s' % (node, port))
        s.close()

    logging.info('Failed to resolve ip address for %s:%s' % (node, port) )


    for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
        if not ip.startswith("127."):
            return ip

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect('8.8.8.8', 53)
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        s.close()

    logging.info('All methods failed to resolve ip address, returning 127.0.0.1')
    return '127.0.0.1'

def find_listen_ip(port, protocol='TCP'):
    lsof_command = ['sudo', 'lsof', '-i'+protocol+':'+str(port), '-sTCP:LISTEN', '-n', '-P']
    status, code, output = run_command(command=lsof_command, output=True, ignore=True)
    if status:
        lines = output.split('\n')
        if len(lines) > 1:
            line = lines[1]
            splits = line.split()
            ip, port = splits[8].split(':')
            if ip == '*':
                return '127.0.0.1'
            else:
                return ip

    raise Exception("Nobody listening on port %s"%str(port))

def get_local_ips():
    lsof_command = ['sudo', 'hostname', '-I']
    status, code, output = run_command(command=lsof_command, output=True)
    return output.split()


def totimestamp(dt, epoch=datetime(1970,1,1)):
    td = dt - epoch
    # return td.total_seconds()
    return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6

def rsync_archive(source, target, excludes=[], options=["-av", "--ignore-errors", "--delete", "--delete-excluded"]):
  '''
  run rsync incrementally
  :return: (True, success, Details) or (False, Error Message, Details)
  '''
  ensure_directory(target)
  rsync_cmd = ["rsync"] + options
  for exclude in excludes:
      rsync_cmd.extend(["--exclude", exclude])

  rsync_cmd.append(source)
  rsync_cmd.append(target)

  run_command(command=rsync_cmd, retry_sudo=True)

if __name__ == "__main__":
  #print find_listen_ip(80)
  #print ensure_directory('~/temp', user='root', group='users')
  print ps_grep('cassandra', excludes=['grep'])
  print get_owner('~/temp')
