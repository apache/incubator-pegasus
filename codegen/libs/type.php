<?php

// the current program
global $_PROG;

if (is_null($_PROG))
	$_PROG = new t_program();

class t_program
{
	var $name;
	var $namespaces;
	var $includes;
	var $typedefs;
	var $enums;
	var $structs;
	var $services;
	
	function __construct()
	{
		$this->name = "not set yet";
		$this->namespaces = array();
		$this->includes = array();
		$this->typedefs = array();
		$this->enums = array();
		$this->structs = array();
		$this->services = array();
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
	}	
}

class t_enum extends t_type
{
	var $values;
	
	function __construct($program, $name)
	{
		parent::__construct($program, $name);
		$this->values = array();
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
	var $type;
	var $value;
	
	function __construct($name, $type, $value)
	{
		$this->name = $name;
		$this->type = $type;
		$this->value = $value;
	}
}

class t_struct extends t_type
{
	var $fields;

	function __construct($program, $name)
	{
		parent::__construct($program, $name);
		$this->fields = array();
	}
	
	function add_field($name, $type, $value)
	{
		$this->fields[] = new t_field($name, $type, $value);
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
	
	function add_param($name, $type)
	{
		$this->params[] = new t_field($name, $type, NULL);
	}
}

class t_service extends t_type
{
	var $functions;
	
	function __construct($program, $name)
	{
		parent::__construct($program, $name);
		$this->functions = array();
	}
	
	function add_function($ret, $name)
	{
		$f = new t_function($this, $ret, $name);
		$this->functions[] = $f;
		return $f;
	}
}

/*
class Func {
	var $module;
	var $name;
	var $rawName;
	var $attrs;
	var $ret;
	var $params;
	var $space;
	
	function __construct($module, $name, $attrs, $ret, $params) {
		$this->module = $module;
		$this->name = $name;
		$this->attrs = $attrs;
		$this->ret = $ret;
		$this->params = $params;
		$this->rawName = $name;
		$this->space = "BEYOND_DEFAULT_MONITOR_MODE";
	}

	function has_key($key)
	{
		return array_key_exists($key, $this->attrs);
	}

	function value_by_key($key)
	{
		if (array_key_exists($key, $this->attrs))
			return $this->attrs[$key];
		else
			return false;
	}

	function has_attr($aname)
	{
		return $this->has_key($aname);
	}
	
	function has_nonh_out_pa()
	{
		foreach ($this->params as $pa) 
		{
			if ($pa->has_attr('out') && !$pa->is_handle())
				return true;
		}
		return false;
	}
	
	function has_named_param($pname)
	{
		foreach ($this->params as $pa) 
		{
			if ($pa->name == $pname)
				return true;
		}
		return false;
	}
	
	function has_attred_param($att)
	{
		foreach ($this->params as $pa) 
		{
			if ($pa->has_attr($att))
				return true;
		}
		return false;
	}
	function get_attred_param($att)
	{
		foreach ($this->params as $pa) 
		{
			if ($pa->has_attr($att))
				return $pa;
		}
		return NULL;
	}

	function get_res_handle()
	{
		$rh = "";
		foreach ($this->params as $pa) 
		{
			if (array_key_exists('handle', $pa->attrs))
			{
				if (!array_key_exists('out', $pa->attrs) || array_key_exists('in', $pa->attrs))  // sole [in] annotation may be omited, but [in] cannot be omited in case of [in,out]
				{
					$rh = $pa->name;
					break;
				}
			}
		}
	
		if ($rh != "")
			return $rh;
		else if (count($this->params) == 0)
			return "RESID_VOID";
		else if (array_key_exists('out', $this->params[0]->attrs) && !array_key_exists('in', $this->params[0]->attrs))
			return "RESID_UNKNOWN";
		else
			return "(*(uint32*)(&" . $this->params[0]->name . "))";
	}

	function is_ret_void()
	{
		return (strcasecmp($this->ret->type, "VOID") == 0);
	}
	
	function get_succ_predicate()
	{
		$pred = "";
		if ($this->has_attr('succ'))
			$pred = $this->attrs['succ'];
		else if ($this->ret->type == "NTSTATUS")
			$pred = "NT_SUCCESS(sx_last_ret)";
		else if ($this->ret->type == "BOOL" || $this->ret->type == "bool")
			$pred = "sx_last_ret != FALSE";
			
		if ($pred != "")
			$pred = str_replace("return", "sx_last_ret", $pred);
		return $pred;
	}

	function get_fail_predicate()
	{
		$pred = "";
		if ($this->has_attr('fail'))
			$pred = $this->attrs['fail'];
		else if ($this->ret->type == "NTSTATUS")
			$pred = "!NT_SUCCESS(sx_last_ret)";
		else if ($this->ret->type == "BOOL" || $this->ret->type == "bool")
			$pred = "sx_last_ret == FALSE";
			
		if ($pred != "")
			$pred = str_replace("return", "sx_last_ret", $pred);
		return $pred;
	}
	
	function get_event_flag()
	{
		$ev_flag = "EVENT_FLAG_NORMAL";
		if ($this->has_attr('nlr'))
			$ev_flag = $ev_flag . "|EVENT_FLAG_API_NLR";
		if ($this->has_attr('cache'))
			$ev_flag = $ev_flag . "|EVENT_FLAG_API_CACHE";
		if ($this->has_attr('nonblock'))
			$ev_flag = $ev_flag . "|EVENT_FLAG_NON_BLOCK";
		if ($this->has_attr('sched'))
			$ev_flag = $ev_flag . "|EVENT_FLAG_API_USER_SCHED";
		return $ev_flag;
	}
}

class Param {
	var $type;
	var $name;
	var $attrs;
	function __construct($type, $name, $attrs) {
		$this->type = $type;
		$this->name = $name;
		$this->attrs = $attrs;
	}

	function has_key($key)
	{
		return array_key_exists($key, $this->attrs);
	}

	function value_by_key($key)
	{
		if (array_key_exists($key, $this->attrs))
			return $this->attrs[$key];
		else
			return false;
	}

	function get_binded_prototype()
	{
		global $_FUNCTION_PROTOTYPES;
		if (array_key_exists($this->type, $_FUNCTION_PROTOTYPES))
			return $_FUNCTION_PROTOTYPES[$this->type];
		else
			return false;
	}

	function get_etype()
	{
		$etype = "";
		if ($this->has_attr('bcap') || $this->has_attr('bsize'))
			$etype = "CHAR";
		else if ($this->has_attr('etype'))
			$etype = $this->attrs['etype'];
		else
			$etype = "deref<" . $this->type . ">::type";
		return $etype;
	}

	function get_ecap()
	{
		$ecap = "";
		if ($this->has_attr('bcap') || $this->has_attr('bsize'))
		{
			if ($this->has_attr('bcap'))
				$ecap = $this->attrs['bcap'];
			else
				$ecap = $this->attrs['bsize'];
		}
		else if ($this->has_attr('ecap') || $this->has_attr('esize'))
		{
			if ($this->has_attr('ecap'))
				$ecap = $this->attrs['ecap'];
			else
				$ecap = $this->attrs['esize'];
		}			
		
		if ($ecap != "")
			$ecap = str_replace("return", "sx_last_ret", $ecap);

		return $ecap;
	}

}
*/
?>
