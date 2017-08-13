import logging
import sys
from agent.utils.checkinit import *
from bottle import route, run
import routetargets

logging.basicConfig(format='[%(asctime)s]:[%(levelname)s]:[{%(filename)s:%(lineno)d}]:[%(message)s]',
                    datefmt='%D-%H:%M:%S', level=logging.DEBUG,
                    filename='/var/log/bragent.log')
formatter = logging.Formatter('[%(levelname)s]:[%(message)s]', datefmt='%D-%H:%M:%S')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
log = logging.getLogger('BackupAgent')
log.addHandler(console)

# log_format_str = '[%(levelname)s]:[%(filename)s:%(lineno)d]:[%(message)s]'
# # log_format_str = '[%(asctime)s] %(levelname)s {%(filename)s:%(lineno)d} - %(message)s'
# logging.basicConfig(format=log_format_str,
#                     stream=sys.stdout, level=logging.DEBUG)
# log = logging.getLogger('BackupAgent')
AGENT_PORT = 7777
SERVER_IP = '0.0.0.0'


log.info('Check whether running as root')
if not is_user_root():
    log.error('Backup agent needs to be run as root user. Exiting...')
    #exit(-1)

log.info('Check NFS installation')
install_nfs_server()
routetargets.register_routes(route)

if len(sys.argv) > 1:
    SERVER_IP = sys.argv[1]
log.info('Start backup agent')
run(host=SERVER_IP, port=AGENT_PORT, debug=True)
