<?php

// the current program
global $_PROG;

class thelpers
{
    public static function begin_with($haystack, $needle) 
    {
        if (strlen($needle) > strlen($haystack)) return FALSE;
        else return substr($haystack, 0, strlen($needle)) === $needle;
    }

    public static function is_container_type($full_name)
    {
        return thelpers::begin_with($full_name, "vector<")
            || thelpers::begin_with($full_name, "list<")
            || thelpers::begin_with($full_name, "map<")
            || thelpers::begin_with($full_name, "set<")
            ;
    }
    
    public static function get_container_type($full_name)
    {
        if (thelpers::is_container_type($full_name))
            return trim(substr($full_name, 0, strpos($full_name, "<", 0)));
        else
            return FALSE;
    }
    
    public static function get_container_key_type($full_name)
    {
        if (thelpers::is_container_type($full_name))
        {
            $pos = strpos($full_name, "<");
            $kvs = trim(substr($full_name, $pos + 1,  strrpos($full_name, ">") - $pos - 1));
            if (strpos($kvs, ",") == FALSE)
                return $kvs;
            else
                return trim(substr($kvs, 0, strpos($kvs, ",") - 1));
        }
        else
            return FALSE;
    }
    
    public static function get_container_value_type($full_name)
    {
        if (thelpers::is_container_type($full_name))
        {
            $pos = strpos($full_name, "<");
            $kvs = trim(substr($full_name, $pos + 1,  strrpos($full_name, ">") - $pos - 1));
            if (strpos($kvs, ",") == FALSE)
                return FALSE;
            else
                return trim(substr($kvs, strpos($kvs, ",") + 1));
        }
        else
            return FALSE;
    }
    
    public static function base_type_to_cpp_type($base_type)
    {
        //echo "base_type_to_cpp_type'".$base_type."'".PHP_EOL;
        switch ($base_type)
        {
        case "list": return "std::list";
        case "map": return "std::map";
        case "set": return "std::set";
        case "vector": return "std::vector";
        case "string": return "std::string";
        case "double": return "double";
        case "float": return "float";
        case "i64": return "int64_t";
        case "int64": return "int64_t";
        case "int64_t": return "int64_t";
        case "ui64": return "uint64_t";
        case "uint64": return "uint64_t";
        case "uint64_t": return "uint64_t";
        case "i32": return "int32_t";
        case "int32": return "int32_t";
        case "int32_t": return "int32_t";
        case "ui32": return "uint32_t";
        case "uint32": return "uint32_t";
        case "uint32_t": return "uint32_t";
        case "byte": return "byte";
        case "BYTE": return "byte";
        case "Byte": return "byte";
        case "bool": return "bool";
        case "BOOL": return "bool";
        case "Bool": return "bool";
        case "sint32": return "int32_t";
        case "sint64": return "int64_t";
        case "fixed32": return "int32_t";
        case "fixed64": return "int64_t";
        case "sfixed32": return "int32_t";
        case "sfixed64": return "int64_t";
                
        default: return $base_type;
        }
    }

    public static function get_cpp_type_name($full_name)
    {
        global $_PROG;
        if (thelpers::is_container_type($full_name))
            return thelpers::get_cpp_name_internal($full_name);
        else
        {
            $pos = strrpos($full_name, ".");
            if (FALSE == $pos)
                return thelpers::get_cpp_name_internal($full_name);
            else
            {        
                // check cpp namespace as prefix
                $prog = NULL;
                $left = "";
                if (thelpers::begin_with($full_name, $_PROG->get_namespace("cpp")."."))
                {
                    $left = substr($full_name, strlen($_PROG->get_namespace("cpp")) + 1);
                    $prog = $_PROG;
                }
                else 
                {
                    foreach ($_PROG->includes as $pn => $p)
                    {
                        if (thelpers::begin_with($full_name, $p->get_namespace("cpp")."."))
                        {
                            $left = substr($full_name, strlen($p->get_namespace("cpp")) + 1);
                            $prog = $p;
                            break;
                        }
                    }
                }
                
                // check package as prefix
                if ($prog == NULL)
                {
                    if (thelpers::begin_with($full_name, $_PROG->name."."))
                    {
                        $left = substr($full_name, strlen($_PROG->name) + 1);
                        $prog = $_PROG;
                    }
                    else 
                    {
                        foreach ($_PROG->includes as $pn => $p)
                        {
                            if (thelpers::begin_with($full_name, $p->name."."))
                            {
                                $left = substr($full_name, strlen($p->name) + 1);
                                $prog = $p;
                                break;
                            }
                        }
                    }
                }
                    
                if (NULL == $prog)
                {
                    return "full type translation from '". $full_name. "' failed.";
                }
                
                return $prog == $_PROG ? 
                    thelpers::get_cpp_name_internal($left) :
                    $prog->get_cpp_namespace() . thelpers::get_cpp_name_internal($left);
            }
        }
    }
    
    private static function get_cpp_name_internal($full_name)
    {
        if (thelpers::is_container_type($full_name))
        {
            $kt = thelpers::get_container_key_type($full_name);
            $vt = thelpers::get_container_value_type($full_name);
            $ct = thelpers::get_container_type($full_name);
            return thelpers::base_type_to_cpp_type($ct)."< ".
                    ($vt == FALSE ? thelpers::get_cpp_type_name($kt)
                        : (thelpers::get_cpp_type_name($kt).", ".thelpers::get_cpp_type_name($vt)))
                    .">";
        }
        else if (FALSE != strpos($full_name, "."))
        {
            return str_replace(".", "_", $full_name);
            //return substr($full_name, 0, strpos($full_name, ".")) 
            //    ."::".thelpers::get_cpp_name_internal(
            //    substr($full_name, strpos($full_name, ".") + 1)
            //    );
        }
        else
        {
            return thelpers::base_type_to_cpp_type($full_name);
        }
    }
}


class t_program
{
    var $name;
    var $namespaces;
    var $includes;
    var $typedefs;
    var $enums;
    var $structs;
    var $services;
    var $types;
    var $annotations;
    
    function __construct($name)
    {
        $this->name = $name;
        $this->namespaces = array();
        $this->includes = array();
        $this->typedefs = array();
        $this->enums = array();
        $this->structs = array();
        $this->services = array();
        $this->types = array();
        $this->annotations = array();
    }
    
    function get_test_task_code()
    {
        return "LPC"
            ."_". strtoupper($this->name) 
            ."_TEST_TIMER"
            ;
    }
    
    function get_cpp_namespace()
    {
        if (!array_key_exists("cpp", $this->namespaces))
        {
            return "";
        }
        
        $nms = $this->namespaces["cpp"];
        $nms = explode(".", $nms);
        $rt = "::";
        foreach ($nms as $nm)
        {
            $rt .= $nm ."::";
        }
        return $rt;
    }
    
    function get_namespace($lang)
    {
        if (!array_key_exists($lang, $this->namespaces))
        {
            return FALSE;
        }
        
        return $this->namespaces[$lang];
    }
    
    function get_cpp_namespace_begin()
    {
        if (!array_key_exists("cpp", $this->namespaces))
        {
            return "";
        }
        
        $nms = $this->namespaces["cpp"];
        $nms = explode(".", $nms);
        $rt = "";
        foreach ($nms as $nm)
        {
            $rt .= "namespace ". $nm ." { ";
        }
        return $rt;
    }
    
    function get_cpp_namespace_end()
    {
        if (!array_key_exists("cpp", $this->namespaces))
        {
            return "";
        }
        
        $nms = $this->namespaces["cpp"];
        $nms = explode(".", $nms);
        $rt = "";
        foreach ($nms as $nm)
        {
            $rt .= "} ";
        }
        return $rt;
    }
    
    function add_annotations($atts)
    {
        $this->annotations = $atts;
        
        foreach ($this->structs as $s)
        {
            $s->on_annotations();
        }
        
        foreach ($this->services as $s)
        {
            $s->on_annotations();
        }
    }
}

class t_type
{
    var $program;
    var $name;
    
    function __construct($program, $name)
    {
        if (thelpers::begin_with($name, $program->get_namespace("cpp")."."))
        {
            $name = substr($name, strlen($program->get_namespace("cpp")) + 1);
        }
        else if (thelpers::begin_with($name, $program->name."."))
        {
            $name = substr($name, strlen($program->name) + 1);
        }
        
        $this->program = $program;
        $this->name = $name;
        $program->types[] = $this;
    }
    
    function get_cpp_name()
    {
        $pos = strpos($this->name, ".");
        if ($pos == FALSE)
            return $this->name;
        else
        {
            $prefix = substr($this->name, 0, $pos);
            if (0 == strcmp($prefix, $this->program->name)
             || 0 == strcmp($prefix, $this->program->get_namespace("cpp")))
            {
                $prefix = substr($this->name, $pos + 1);
            }
            else
            {
                $prefix = $this->name;
            }
            
            return str_replace(".", "_", $prefix);
        }
    }
    
    function is_void() { return false; }
    function is_enum() { return false; }
    function is_alias() { return false; }
    function is_base_type() { return true; }
}

class t_typedef extends t_type
{
    var $type;
    
    function __construct($program, $type, $alias)
    {
        parent::__construct($program, $alias);
        $this->type = $type;
        $program->typedefs[] = $this;
    }

    function is_alias() { return true; }    
}

class t_enum extends t_type
{
    var $values;
    
    function __construct($program, $name)
    {
        parent::__construct($program, $name);
        $this->values = array();
        $program->enums[] = $this;
    }
    
    function add_value($name, $value)
    {
        $this->values[$name] = $value;
    }    
    
    function is_enum() { return true; }
}

class t_field
{
    var $name;
    var $type_name;
    
    function __construct($name, $type_name)
    {
        $this->name = $name;
        $this->type_name = $type_name;
    }
    
    function get_cpp_type()
    {
        return thelpers::get_cpp_type_name($this->type_name);
    }
}

class t_struct extends t_type
{
    var $fields;

    function __construct($program, $name)
    {
        parent::__construct($program, $name);
        $this->fields = array();
        $program->structs[] = $this;
    }
    
    function add_field($name, $type_name)
    {
        $this->fields[] = new t_field($name, $type_name);
    }
    
    function is_base_type() { return false; }
    
    function on_annotations()
    {
        // nothing to do for now
    }
}

class t_function
{
    var $service;
    var $ret;
    var $name;
    var $params;
    var $is_write;
    
    function __construct($service, $ret, $name)
    {
        $this->service = $service;
        $this->ret = $ret;
        $this->name = $name;
        $this->is_write = false;
        $this->params = array();
    }
    
    function add_param($name, $type_name)
    {
        $this->params[] = new t_field($name, $type_name);
    }
    
    function get_cpp_return_type()
    {
        return thelpers::get_cpp_type_name($this->ret);
    }
    
    function get_first_param()
    {
        return $this->params[0];
    }
    
    function get_rpc_code()
    {
        return "RPC"
            ."_". strtoupper($this->service->program->name)
            ."_". strtoupper($this->service->name) 
            ."_". strtoupper($this->name)
            ;
    }
    
    function is_one_way()
    {
        return $this->ret == "void" || $this->ret == "VOID";
    }
    
    function on_annotations()
    {
        $atts = $this->service->program->annotations;
            
        // [function.service.add]
        // write = true  ; service.add is a write function
        $key = "function.".$this->service->name.".".$this->name;
        
        if (array_key_exists($key, $atts))
        {
            if (array_key_exists("write", $atts[$key]))
                $b = $atts[$key]["write"];
            else
                $b = NULL;
                
            $this->is_write = ($b != NULL && ($b == "1" || $b == 1));
        }
    }
}

class t_service extends t_type
{
    var $functions;
    var $is_stateful;
    
    function __construct($program, $name)
    {
        parent::__construct($program, $name);
        $this->functions = array();
        $this->is_stateful = false;
        
        $program->services[] = $this;        
    }
    
    function add_function($ret, $name)
    {
        $f = new t_function($this, $ret, $name);
        $this->functions[] = $f;
        return $f;
    }
    
    function on_annotations()
    {
        $atts = $this->program->annotations;
        
        // [service.counter]
        // stateful = true ; counter is a stateful service
        $key = "service.".$this->name;
        
        if (array_key_exists($key, $atts))
        {
            if (array_key_exists("stateful", $atts[$key]))
                $b = $atts[$key]["stateful"];
            else
                $b = NULL;
                
            $this->is_stateful = ($b != NULL && ($b == "1" || $b == 1));
        }        
        
        // continue for each function
        foreach ($this->functions as $f)
        {
            $f->on_annotations();
        }
    }
}
?>
