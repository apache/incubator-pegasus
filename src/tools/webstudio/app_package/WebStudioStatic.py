from WebStudioLib import *
from WebStudioUtil import *

class StaticFileHandler(webapp2.RequestHandler):
    path = ''
    def get(self, path):
        abs_path = os.path.abspath(os.path.join(self.path, path))
        if os.path.isdir(abs_path)  != 0:
            self.response.set_status(403)
            return
        try:
            f = open(abs_path, 'rb')
            self.response.headers.add_header('Content-Type', mimetypes.guess_type(abs_path)[0])
            self.response.out.write(f.read())
            f.close()
        except:
            self.response.set_status(404)

class AppStaticFileHandler(StaticFileHandler):
    def __init__(self, request, response):
        # Set self.request, self.response and self.app.
        self.initialize(request, response)
        self.path = './'

class LocalStaticFileHandler(StaticFileHandler):
    def __init__(self, request, response):
        # Set self.request, self.response and self.app.
        self.initialize(request, response)
        self.path = GetWebStudioDirPath() + '/local/'

