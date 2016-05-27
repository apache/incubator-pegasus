import sys
import os
import inspect
sys.path.append(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) + '/app_package')

from WebStudioApp import *

if __name__ == '__main__':
    #rDSN.WebStudio run as a simple HTTP server 
    print "rDSN.WebStudio running in light mode."
    if len(sys.argv) < 2:
        print '''Wrong parameters. 
        Usage:
            For light mode, run:
                python rDSN.WebStudio.py 8088(portnum)

        '''
    else:
        start_http_server(sys.argv[1])

    

