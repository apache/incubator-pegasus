from WebStudioLib import *
from WebStudioUtil import *

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)
JINJA_ENVIRONMENT_Vue = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True,
    variable_start_string='{@', variable_end_string='@}')

class BaseHandler(webapp2.RequestHandler):
    def render_template(self, view_filename, params=None):
        if not params:
            params = {}
        path = 'static/view/' + view_filename

        template = JINJA_ENVIRONMENT.get_template(path)
        self.response.out.write(template.render(params))

    def render_template_Vue(self, view_filename, params=None):
        if not params:
            params = {}
        path = 'static/view/' + view_filename

        template = JINJA_ENVIRONMENT_Vue.get_template(path)
        self.response.out.write(template.render(params))

    def SendJson(self, r):
        self.response.headers['content-type'] = 'text/plain'
        self.response.write(json.dumps(r))
