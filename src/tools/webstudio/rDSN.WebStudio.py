import sys
import os
import inspect
sys.path.append(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) + '/app_package')

from WebStudioApp import *

if __name__ == '__main__':
    
    port = "8088"    
    if len(sys.argv) >= 2:
        port = sys.argv[1];
    else:
        print "using default listen port " + port + ", run 'python rDSN.WebStudio.py %port%' to use another port instead"

    print "rDSN WebStudio running at port " + port
    start_http_server(port)

    

