import sys
import os
import inspect
sys.path.append(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) + '/app_package')

from WebStudioApp import *

def start_dsn():
    service_app = ServiceApp()
    app_dict['webstudio'] = WebStudioService
    service_app.register_app('webstudio')
    if len(sys.argv) < 2:
        #rDSN.WebStudio run as an embedded service
        print "rDSN.WebStudio registered. Later run in embedded mode."

        Native.dsn_app_loader_signal()
        #to be fix, hangs forever now to keep python interpreter alive
        dummy_event = threading.Event()
        dummy_event.wait() 

    elif sys.argv[1] == 'standalone':
        #rDSN.WebStudio run as a caller calling the monitored program 
        print "rDSN.WebStudio running in standalone mode."

        argv = (c_char_p*2)()
        argv[0] = b'rDSN.WebStudio.exe'
        argv[1] = b'config.ini'
        
        Native.dsn_run(2, argv, c_bool(1))

    elif sys.argv[1] == 'light':
        #rDSN.WebStudio run as a simple HTTP server 
        print "rDSN.WebStudio running in light mode."

        
        start_http_server(sys.argv[2])

    else:
        print '''Wrong parameters. 
        Usage:
            For embedded mode, just add WebStudio in the config.ini and run the compiled programs:
                dsn.replication.simple_kv config.ini
            For standalone mode, run:
                python rDSN.WebStudio.py standalone
            For light mode, run:
                python rDSN.WebStudio.py light 8088(portnum)

        '''


if __name__ == '__main__':
    start_dsn()

    

