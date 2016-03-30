import WebStudioApp 
from WebStudioLib import * 
from dev.python.ServiceApp import *
THREAD_POOL_DEFAULT = ThreadPoolCode.threadpool_code_register('THREAD_POOL_DEFAULT')

class WebStudioService(ServiceApp):
    __server = None
    __echo_client = None
    __task = None

    def start(self, argv):
        _server_thread= threading.Thread(target=WebStudioApp.start_http_server,args=(argv[1],))
        _server_thread.start()
        return 0

    def stop(self, cleanup = 0):
        #_server_thread.stop()
        #_server_thread.join()
        pass


