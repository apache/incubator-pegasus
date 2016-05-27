from WebStudioLib import *
from WebStudioUtil import *
from WebStudioBase import *
from WebStudioStatic import *
from WebStudioPage import *
from WebStudioApi import *

def start_http_server(portNum):  
    web_app = webapp2.WSGIApplication([
        ('/', PageMainHandler),
        ('/main.html', PageMainHandler),
        ('/table.html', PageTableHandler),
        ('/task_analyzer.html', PageTaskAnalyzerHandler),
        ('/queue.html', PageQueueHandler),
        ('/cli.html', PageCliHandler),
        ('/bash.html', PageBashHandler),
        ('/editor.html', PageEditorHandler),
        ('/configure.html', PageConfigureHandler),
        ('/fileview.html', PageFileViewHandler),
        ('/analyzer.html', PageAnalyzerHandler),
        ('/counterview.html', PageCounterViewHandler),
        ('/store.html', PageStoreHandler),
        ('/multicmd.html', PageMulticmdHandler),
        ('/service_meta.html', PageServiceMetaHandler),
        ('/machine.html', PageMachineHandler),
        ('/setting.html', PageSettingHandler),

        ('/api/bash', ApiBashHandler),
        ('/api/psutil', ApiPsutilHandler),
        ('/api/view/save', ApiSaveViewHandler),
        ('/api/view/load', ApiLoadViewHandler),
        ('/api/view/del', ApiDelViewHandler),
        ('/api/pack/load', ApiLoadPackHandler),
        ('/api/pack/detail', ApiPackDetailHandler),
        ('/api/pack/del', ApiDelPackHandler),
        ('/api/scenario/save', ApiSaveScenarioHandler),
        ('/api/scenario/load', ApiLoadScenarioHandler),
        ('/api/scenario/del', ApiDelScenarioHandler),

        ('/app/(.+)', AppStaticFileHandler),
        ('/local/(.+)', LocalStaticFileHandler),
    ], debug=True)

    static_app = webob.static.DirectoryApp(os.path.join(GetWebStudioDirPath(),"static"))
    app_list = Cascade([static_app, web_app])

    httpserver.serve(app_list, host='0.0.0.0', port=str(portNum))
