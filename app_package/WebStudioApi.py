from WebStudioCodeDefinition import *
from WebStudioLib import *
from WebStudioUtil import *
from WebStudioBase import *

sqlDataType = {}
sqlDataType['counter_view'] = {
    'elems': [
        {'name': 'name','type': 'text'},
        {'name': 'author','type': 'text'},
        {'name': 'description','type': 'text'},
        {'name': 'counterList','type': 'text'},
        {'name': 'graphtype','type': 'text'},
        {'name': 'interval','type': 'text'},
    ],
}

sqlDataType['app_package'] = {
    'elems': [
        {'name': 'name','type': 'text'},
        {'name': 'author','type': 'text'},
        {'name': 'description','type': 'text'},
        {'name': 'register_state','type': 'text'},
        {'name': 'cluster_type','type': 'text'},
        {'name': 'schema_info','type': 'text'},
        {'name': 'schema_type','type': 'text'},
        {'name': 'server_type','type': 'text'},
        {'name': 'parameters','type': 'text'},
        {'name': 'if_stateful','type': 'text'},
    ],
}

sqlDataType['cmd_scenario'] = {
    'elems': [
        {'name': 'name','type': 'text'},
        {'name': 'author','type': 'text'},
        {'name': 'description','type': 'text'},
        {'name': 'machines','type': 'text'},
        {'name': 'cmdtext','type': 'text'},
        {'name': 'interval','type': 'text'},
        {'name': 'times','type': 'text'},
    ],
}

TCreate = jinja2.Template('CREATE TABLE IF NOT EXISTS {{dataType}} ({% for elem in elems %}{{elem.name}} {{elem.type}}{% if not loop.last %},{% endif %}{% endfor %});')

TDelete = jinja2.Template("DELETE FROM {{dataType}} WHERE name = '{{dataName}}';")

TDeleteall = jinja2.Template('DELETE FROM {{dataType}};')

TInsert = jinja2.Template("INSERT INTO {{dataType}} VALUES ({% for val in val_list %}'{{val}}'{% if not loop.last %},{% endif %}{% endfor %});")

TSelect = jinja2.Template('SELECT * FROM {{dataType}}')

TSelectone = jinja2.Template("SELECT * FROM {{dataType}} WHERE name = '{{dataName}}';")

TUpdate = jinja2.Template("UPDATE {{dataType}} SET {{updateColumn}}='{{updateValue}}' WHERE name='{{dataName}}';")

def sqlOp(op='',dataType='',dataName='',val_list=''):
    res = None

    local_dir = os.path.join(GetWebStudioDirPath(),'local')
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    conn = sqlite3.connect(os.path.join(GetWebStudioDirPath(),'local','data.db'))
    c = conn.cursor()
    
    c.execute(TCreate.render({'dataType':dataType,'elems': sqlDataType[dataType]['elems']}))
    if op == 'save':
        c.execute(TDeleteall.render({'dataType':dataType}))
        c.execute(TInsert.render({'dataType':dataType,'val_list':val_list}))
    elif op == 'load':
        res = list(c.execute(TSelect.render({'dataType':dataType})))
    elif op == 'delete':
        c.execute(TDelete.render({'dataType':dataType,'dataName':dataName}))
    elif op == 'detail':
        c.execute(TSelectone.render({'dataType':dataType,'dataName':dataName}))
        res = list(c.fetchone())
        
    conn.commit()
    conn.close()
    return res

class ApiBashHandler(BaseHandler):
    def get(self):
        command = self.request.get('command');
        queryRes = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read()
        self.response.write(queryRes)

class ApiPsutilHandler(BaseHandler):
    def get(self):
        queryRes = {}
        queryRes['cpu'] = psutil.cpu_percent(interval=1);
        queryRes['memory'] = psutil.virtual_memory()[2];
        queryRes['disk'] = psutil.disk_usage('/')[3];
        queryRes['networkio'] = psutil.net_io_counters()
        self.response.write(json.dumps(queryRes))

class ApiSaveViewHandler(BaseHandler):
    def post(self):
        sqlOp(op='save',dataType='counter_view',val_list=[
                self.request.get('name'),
                self.request.get('author'),
                self.request.get('description'),
                self.request.get('counterList'),
                self.request.get('graphtype'),
                self.request.get('interval')
        ])

        self.response.write('view "'+ self.request.get('name') +'" is successfully saved!')

class ApiLoadViewHandler(BaseHandler):
    def post(self): 
        viewList = []
        for view in sqlOp(op='load',dataType='counter_view'):
            viewList.append({'name':view[0],'author':view[1],'description':view[2],'counterList':view[3],'graphtype':view[4],'interval':view[5]})

        self.SendJson(viewList)

class ApiDelViewHandler(BaseHandler):
    def post(self): 
        sqlOp(op='delete',dataType='counter_view',dataName=self.request.get('name'))

        self.response.write('success')

class ApiLoadPackHandler(BaseHandler):
    def post(self):
        pack_dir = os.path.join(GetWebStudioDirPath(),'local','pack')
        if not os.path.exists(pack_dir):
            os.makedirs(pack_dir)

        packList = []
        for pack in sqlOp(op='load',dataType='app_package'):
            packList.append({'name':pack[0],'author':pack[1],'description':pack[2],'register_state':pack[3],'cluster_type':pack[4],'if_stateful':pack[9]})
        
        self.SendJson(packList)    

class ApiPackDetailHandler(BaseHandler):
    def post(self):
        pack_info = sqlOp(op='detail',dataType='app_package',dataName=self.request.get('id'))

        if pack_info == [] :
            self.response.write('cannot find the package : ' + pack_id)
            return

        ret = {'name' : pack_info[0], \
            'schema_info' : pack_info[5], \
            'schema_type' : pack_info[6], \
            'server_type' : pack_info[7], \
            'parameters' : pack_info[8]};
        self.SendJson(ret)


class ApiDelPackHandler(BaseHandler):
    def post(self):
        packName = self.request.get('name')

        sqlOp(op='delete',dataType='app_package',dataName=packName)

        try:
            shutil.rmtree(os.path.join(pack_dir,packName))
            os.remove(os.path.join(pack_dir,packName+'.jpg'))
            os.remove(os.path.join(pack_dir,packName+'.7z'))
            os.remove(os.path.join(pack_dir,packName+'.thrift'))
        
            self.response.write('success')
        except:
            self.response.write('fail')

class ApiSaveScenarioHandler(BaseHandler):
    def post(self):
        sqlOp(op='save',dataType='cmd_scenario',val_list=[
                self.request.get('name'),
                self.request.get('author'),
                self.request.get('description'),
                self.request.get('machines'),
                self.request.get('cmdtext'),
                self.request.get('interval'),
                self.request.get('times')
        ])

        self.response.write('success')

class ApiLoadScenarioHandler(BaseHandler):
    def post(self): 
        scenarioList = []
        for scenario in sqlOp(op='load',dataType='cmd_scenario'):
            scenarioList.append({'name':scenario[0],'author':scenario[1],'description':scenario[2],'machines':scenario[3],'cmdtext':scenario[4],'interval':scenario[5],'times':scenario[6]})

        self.SendJson(scenarioList)
class ApiDelScenarioHandler(BaseHandler):
    def post(self): 
        sqlOp(op='delete',dataType='cmd_scenario',dataName=self.request.get('name'))

        self.response.write('success')
