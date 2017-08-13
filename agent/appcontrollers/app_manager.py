import os, sys
import logging
from agent.utils import fileutils
import copy
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
log = logging.getLogger('BackupAgent')


class AppManager(object):
    app_obj_list = {}

    def __init__(self):
        self.debug_data = None

    def initialize(self):
        apps = fileutils.get_available_apps()
        for app in apps:
            self.app_obj_list[app] = self.load_app(app)
        if os.getenv('DEBUG', '0') == '1':
            self.debug_data = fileutils.get_yaml_config('data/example.yaml')

    def load_app(self, app):
        appCls = app.title()
        try:
            modname = "apps." + app
            module = __import__(modname, globals(), locals(), [app], -1)
            class_ = getattr(module, appCls)
            classObj = class_()
            classObj.loaded()
            return classObj
        except Exception as e:
            log.exception("No App : " + app + ", Error : " + e.message)
            raise

    def get_app(self, app_name):
        app_obj = self.app_obj_list[app_name]
        if app_obj is None:
            raise ImportWarning('App %s not available', app_name)
        return app_obj

    def fetch_apps_info(self, device_data):
        components = device_data.get('components', {})
        log.info('Fetching app info for each application')
        for app, app_obj in self.app_obj_list.iteritems():
            try:
                log.debug('Fetching app info for %s', app)
                app_data = app_obj.describe_app(device_data)
                components[app] = dict(components.get(app,{}).items() + app_data.items())
            except Exception as exp:
                log.exception('Failed to fetch app info for %s', app)

        return device_data

    def call_apps(self, func, req_data):
        log.info('call_apps app_mamanger %s', func)
        log.debug('Input: %s', req_data)
        resp_components = dict()
        response = {
            'status': 'success',
            'components': resp_components
        }

        success_count = 0
        errors = []

        for comp_name, comp_data in req_data['components'].items():
            log.debug('Execute app %s method %s started', comp_name, func)
            app = self.get_app(comp_name)
            app_data =  copy.deepcopy(req_data)
            app_data['component'] = comp_name
            for key, value in comp_data.items():
                app_data[key] = value

            method = getattr(app, func)
            log.debug('Input: %s' % str(app_data))
            resp = {}
            try:
                resp = method(app_data)
                status = resp.get('status', 'unknown')
            except Exception as ex:
                log.error('Failed to execute %s#%s, exception:%s' % (comp_name, func, str(ex)))
                status = str(ex)

            if 'success' == status:
                success_count += success_count
            else:
                errors.append(comp_name + ": " + status)

            resp[func] = status
            resp_components[comp_name] = resp
            log.debug('Execute app %s method %s completed', comp_name, func)
            log.debug('Output app %s: %s' % (func, str(resp)))

        if len(req_data['components']) != success_count:
            response['status'] = ", ".join(errors)
        log.debug('%s result: %s', func, response)
        log.info('########## %s completed for all apps #############', func)
        return response
