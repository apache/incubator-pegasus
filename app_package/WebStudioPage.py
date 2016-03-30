from WebStudioCodeDefinition import *
from WebStudioLib import *
from WebStudioUtil import *
from WebStudioBase import *
from WebStudioApi import *

class PageMainHandler(BaseHandler):
    def get(self):
        self.render_template('main.html')

class PageTableHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('table.html')

class PageTaskAnalyzerHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('task_analyzer.html')

class PageQueueHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('queue.html')

class PageCliHandler(BaseHandler):
    def get(self):
        self.render_template('cli.html')

class PageBashHandler(BaseHandler):
    def get(self):
        self.render_template('bash.html')

class PageEditorHandler(BaseHandler):
    def get(self):
        params = {}
        dir = os.getcwd()
        working_dir = self.request.get('working_dir')
        file_name = self.request.get('file_name')
        if file_name != '':
            read_file = open(os.path.join(dir,working_dir, file_name),'r')
            content = read_file.read()
            read_file.close()
        else:
            content = ''

        dir_list = []
        lastPath = ''
        for d in working_dir.split('/'):
            if lastPath!='':
                lastPath += '/'
            lastPath +=d
            dir_list.append({'path':lastPath,'name':d})
        params['FILES'] = [f for f in os.listdir(os.path.join(dir,working_dir)) if os.path.isfile(os.path.join(dir,working_dir,f))]
        params['FILEFOLDERS'] = [f for f in os.listdir(os.path.join(dir,working_dir)) if os.path.isdir(os.path.join(dir,working_dir,f))]
        params['WORKING_DIR'] = working_dir
        params['DIR_LIST'] = dir_list
        params['CONTENT'] = content 
        params['FILE_NAME'] = file_name 
        
        self.render_template('editor.html',params)
    def post(self):
        content = self.request.get('content')
        dir = os.path.dirname(__file__)
        working_dir = self.request.get('working_dir')
        file_name = self.request.get('file_name')
        if file_name != '':
            write_file = open(os.path.join(dir,working_dir, file_name),'w')
            write_file.write(content)
            write_file.close()
            self.response.write("Successfully saved!")
        else:
            self.response.write("No file opened!")

class PageConfigureHandler(BaseHandler):
    def get(self):
        params = {}
        queryRes = Native.dsn_cli_run('config-dump')
        params['CONTENT'] = queryRes 
        self.render_template('configure.html',params)

class PageFileViewHandler(BaseHandler):
    def get(self):
        params = {}
        working_dir = self.request.get('working_dir')
        root_dir = self.request.get('root_dir')
        
        if root_dir == 'local':
            dir = os.path.dirname(GetWebStudioDirPath()+'/local/')
        elif root_dir == 'app':
            dir = os.path.dirname(os.getcwd()+"/")
        elif root_dir == '':
            root_dir = 'app'
            dir = os.path.dirname(os.getcwd()+"/")
        

        try:
            params['FILES'] = [f for f in os.listdir(os.path.join(dir,working_dir)) if os.path.isfile(os.path.join(dir,working_dir,f))]
            params['FILEFOLDERS'] = [f for f in os.listdir(os.path.join(dir,working_dir)) if os.path.isdir(os.path.join(dir,working_dir,f))]
        except:
            self.response.write('Cannot find the specified file path, please check again')
            return

        dir_list = []
        lastPath = ''
        for d in working_dir.split('/'):
            if lastPath!='':
                lastPath += '/'
            lastPath +=d
            dir_list.append({'path':lastPath,'name':d})
        params['WORKING_DIR'] = working_dir
        params['ROOT_DIR'] = root_dir
        params['DIR_LIST'] = dir_list
        
        self.render_template('fileview.html',params)
    def post(self):
        params = {}
        dir = os.path.dirname(os.getcwd()+"/")
        working_dir = self.request.get('working_dir')
        
        try:
            raw_file = self.request.get('fileToUpload')
            file_name = self.request.get('file_name')
            savedFile = open(os.path.join(dir,working_dir,file_name),'wb')
            savedFile.write(raw_file)
            savedFile.close()

            params['RESPONSE'] = 'success'
        except:
            params['RESPONSE'] = 'fail'

        dir_list = []
        lastPath = ''
        for d in working_dir.split('/'):
            if lastPath!='':
                lastPath += '/'
            lastPath +=d
            dir_list.append({'path':lastPath,'name':d})
        params['FILES'] = [f for f in os.listdir(os.path.join(dir,working_dir)) if os.path.isfile(os.path.join(dir,working_dir,f))]
        params['FILEFOLDERS'] = [f for f in os.listdir(os.path.join(dir,working_dir)) if os.path.isdir(os.path.join(dir,working_dir,f))]
        params['WORKING_DIR'] = working_dir
        params['DIR_LIST'] = dir_list

        self.render_template('fileview.html',params)

class PageAnalyzerHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('analyzer.html')

class PageCounterViewHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('counterview.html')
        
class PageStoreHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('store.html')
    def post(self):
        def updateSQL(newstate, c, conn, if_close=False):
            c.execute(TUpdate.render({'dataType':'app_package','dataName':file_name,'updateColumn':'register_state','updateValue':newstate}))
            conn.commit()
            if if_close:
                conn.close()

        raw_file = self.request.get('fileToUpload')
        raw_icon = self.request.get('iconToUpload')
        file_name = self.request.get('file_name')
        author = self.request.get('author')
        description = self.request.get('description')
        cluster_type = self.request.get('cluster_type')
        schema_info = self.request.get('schema_info')
        schema_type = self.request.get('schema_type')
        server_type = self.request.get('server_type')
        parameters = self.request.get('parameters')
        if_stateful = self.request.get('if_stateful')
        register_state = 'REGISTER_PROCESS_INIT'

        conn = sqlite3.connect(os.path.join(GetWebStudioDirPath(),'local','data.db'))
        c = conn.cursor()
        c.execute(TCreate.render({'dataType':'app_package','elems': sqlDataType['app_package']['elems']}))
        c.execute(TInsert.render({'dataType':'app_package','val_list':[
            file_name,
            author,
            description,
            register_state,
            cluster_type,
            schema_info,
            schema_type,
            server_type,
            parameters,
            if_stateful
        ]}))
        conn.commit()

        pack_dir = os.path.join(GetWebStudioDirPath(),'local','pack')
        if not os.path.exists(pack_dir):
            os.makedirs(pack_dir)

        bin_dir = os.path.join(os.path.dirname(GetWebStudioDirPath()),'bin')
        if not os.path.exists(bin_dir):
            updateSQL('ERR_NO_BIN_FOLDER',c,conn,True)
            return webapp2.redirect('/store.html')

        updateSQL('WRITING_7Z_FILE',c,conn)

        savedFile = open(os.path.join(pack_dir, file_name + '.7z'), 'wb')
        savedFile.write(raw_file)
        savedFile.close()

        updateSQL('WRITING_iCON_FILE',c,conn)

        iconFile = open(os.path.join(pack_dir,file_name + '.jpg'), 'wb')
        iconFile.write(raw_icon)
        iconFile.close()

        updateSQL('WRITING_SCHEMA_FILE',c,conn)

        schemaFile = open(os.path.join(pack_dir, file_name + '.' + schema_type), 'wb')
        schemaFile.write(schema_info)
        schemaFile.close()

        loc_of_7z = ''
        #to detect if 7z exists
        os_type = platform.system()
        if os_type == 'Windows':
            loc_of_7z = os.path.join(bin_dir,'7z.exe')
        elif os_type == 'Linux':
            loc_of_7z = os.path.join(bin_dir,'7z')
        if loc_of_7z =='':
            updateSQL('ERR_NO_7Z',c,conn,True)
            return webapp2.redirect('/store.html')

        updateSQL('EXTRACTING_7Z_FILE',c,conn)

        subprocess.call([loc_of_7z,'x', os.path.join(pack_dir, file_name) + '.7z','-y','-o'+os.path.join(pack_dir, file_name)])
        if not os.path.exists(os.path.join(pack_dir, file_name)):
            updateSQL('ERR_7Z_EXTRACT_FAIL',c,conn,True)
            return webapp2.redirect('/store.html')

        loc_of_tron = ''
        #to detect if Tron exists
        if os_type == 'Windows':
            loc_of_tron = os.path.join(bin_dir,'Tron.exe')
        elif os_type == 'Linux':
            loc_of_tron = os.path.join(bin_dir,'Tron')
        if loc_of_tron =='':
            updateSQL('ERR_NO_TRON',c,conn,True)
            return webapp2.redirect('/store.html')

        updateSQL('TRON_GENERATING_DLL',c,conn)

        from subprocess import Popen, PIPE
        p = Popen([loc_of_tron,'gcs','thrift','dsn', os.path.join(pack_dir,file_name + '.thrift')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,cwd=bin_dir)
        while p.poll() is None:
            print p.stdout.readline()

        if not os.path.exists(os.path.join(bin_dir,'tmp',file_name+'.Tron.Composition.dll')):
            updateSQL('ERR_TRON_GENERATE_DLL_FAIL',c,conn,True)
            return webapp2.redirect('/store.html')

        os.rename(os.path.join(bin_dir,'tmp',file_name+'.Tron.Composition.dll'), os.path.join(pack_dir,file_name,file_name+'.Tron.Composition.dll'))

        updateSQL('SUCCESS',c,conn,True)

        return webapp2.redirect('/store.html')

class PageMulticmdHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('multicmd.html')

class PageServiceMetaHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('service_meta.html')

class PageMachineHandler(BaseHandler):
    def get(self):
        self.render_template_Vue('machine.html')

