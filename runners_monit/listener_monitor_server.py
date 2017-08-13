import threading
import logging

log = logging.getLogger('MonitorListener')


def start_listener():
    """thread worker function"""
    log.info('Monitor listener server started')
    from bottle import route, run

    @route('/status/<appname>')
    def status(appname):
        return 'running'

    @route('/status/<appname>/<event>')
    def status_app_event(appname, changeover):
        return 'changed'
    run(host='localhost', port=8080)


def invoke_listener():
    t = threading.Thread(target=start_listener)
    t.start()
