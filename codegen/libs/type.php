<?php

// the current program
global $_PROG;

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
}

class t_type
{
	var $program;
	var $name;
	
	function __construct($program, $name)
	{
		$this->program = $program;
		$this->name = $name;
		$program->types[] = $this;
	}
	
	function is_void() { return false; }
	function is_enum() { return false; }
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
}

class t_function
{
	var $service;
	var $ret;
	var $name;
	var $params;
	
	function __construct($service, $ret, $name)
	{
		$this->service = $service;
		$this->ret = $ret;
		$this->name = $name;
		$this->params = array();
	}
	
	function add_param($name, $type_name)
	{
		$this->params[] = new t_field($name, $type_name);
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
}

class t_service extends t_type
{
	var $functions;
	
	function __construct($program, $name)
	{
		parent::__construct($program, $name);
		$this->functions = array();
		$program->services[] = $this;
	}
	
	function add_function($ret, $name)
	{
		$f = new t_function($this, $ret, $name);
		$this->functions[] = $f;
		return $f;
	}
}
?>
