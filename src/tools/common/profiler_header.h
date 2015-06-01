#pragma once
#define WIN32_LEAN_AND_MEAN 
#include <iomanip>
#include <Pdh.h>
#include <PDHMsg.h>
#include "shared_io_service.h"

namespace dsn {
	namespace tools {

		const int data_width = 15;
		const int taskname_width = 30;
		const int call_width = 15;
		const int pdh_calc_interval = 1;

		enum perf_counter_ptr_type
		{
			TASK_QUEUEING_TIME_NS,
			TASK_EXEC_TIME_NS,
			TASK_THROUGHPUT,
			TASK_CANCELLED,
			AIO_LATENCY_NS,
			RPC_SERVER_LATENCY_NS,
			RPC_CLIENT_NON_TIMEOUT_LATENCY_NS,
			RPC_CLIENT_TIMEOUT_THROUGHPUT,

			PREF_COUNTER_COUNT,
			PREF_COUNTER_INVALID
		};

		class counter_info
		{
		public:
			counter_info(const std::vector<const char*>& command_keys, perf_counter_ptr_type _index, perf_counter_type _type, const char * _title, const char* _unit)
				: type(_type), title(_title), unit_name(_unit)
			{
				for (auto key : command_keys)
				{
					if (key != nullptr)
					{
						auto it = pointer_type.find(std::string(key));
						dassert(it == pointer_type.end(), "command '%s' already regisered", key);
						pointer_type[std::string(key)] = _index;
					}
				}
			}

			static std::map<std::string, perf_counter_ptr_type> pointer_type;
			perf_counter_type type;
			const char* title; // exec time
			const char* unit_name; // #/s, ms, 

		};

		class profiler_output_data_type
		{
		public:
			profiler_output_data_type(int name_width, int data_width, int call_width)
			{
				std::stringstream namess, datass, tmpss;
				int tmp;

				for (int i = 0; i < name_width; i++)
					namess << "-";
				namess << "+";
				for (int i = 0; i < data_width; i++)
					datass << "-";
				datass << "+";

				for (int i = 0; i < name_width; i++)
					tmpss << " ";
				tmpss << "|";
				blank_name = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				for (int i = 0; i < data_width; i++)
					tmpss << " ";
				tmpss << "|";
				blank_data = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				for (int i = 0; i < call_width; i++)
					tmpss << " ";
				tmpss << "|";
				blank_matrix = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				for (int i = 0; i < call_width; i++)
					tmpss << "-";
				tmpss << "+";
				separate_line_call_times = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmpss << "+-------+" << namess.str() << datass.str();
				separate_line_top = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmpss << "+-------+" << namess.str() << "-------+";
				for (int i = 0; i < PREF_COUNTER_COUNT; i++)
					tmpss << datass.str();
				separate_line_info = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmpss << "+" << namess.str() << separate_line_call_times;
				separate_line_depmatrix = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmpss << "+" << namess.str() << namess.str() << separate_line_call_times;
				separate_line_deplist = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmpss << std::setw(data_width) << "none" << "|";
				none = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmp = (call_width - 5) >> 1;
				tmpss << std::setw(call_width - tmp) << "TIMES";
				for (int i = 0; i < tmp; i++)
					tmpss << " ";
				tmpss << "|";
				view_call_times = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmp = (name_width - 9) >> 1;
				tmpss << std::setw(name_width - tmp) << "TASK TYPE";
				for (int i = 0; i < tmp; i++)
					tmpss << " ";
				tmpss << "|";
				view_task_type = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmp = (name_width - 11) >> 1;
				tmpss << std::setw(name_width - tmp) << "CALLER TYPE";
				for (int i = 0; i < tmp; i++)
					tmpss << " ";
				tmpss << "|";
				view_caller_type = tmpss.str();
				tmpss.clear();
				tmpss.str("");

				tmp = (name_width - 11) >> 1;
				tmpss << std::setw(name_width - tmp) << "CALLEE TYPE";
				for (int i = 0; i < tmp; i++)
					tmpss << " ";
				tmpss << "|";
				view_callee_type = tmpss.str();
			}
			std::string separate_line_call_times;
			std::string separate_line_info;
			std::string separate_line_top;
			std::string separate_line_deplist;
			std::string separate_line_depmatrix;
			std::string view_task_type;
			std::string view_caller_type;
			std::string view_callee_type;
			std::string view_call_times;
			std::string blank_matrix;
			std::string blank_name;
			std::string blank_data;
			std::string none;
		};
		
		class PROFILER_PDH
		{
		public:
			static bool install();
			static void end();
			static void profiler_CPU(std::stringstream &ss);
		private:
			static void Error_Display();
			static bool calc();
			static void on_timer(std::shared_ptr<boost::asio::deadline_timer>& timer, const boost::system::error_code& ec);
			static HMODULE HModule;
			static HQUERY HQuery;
			static HCOUNTER* CounterHandle_Time;
			static PDH_STATUS status;
			static double CounterValue_CPU;
		};

		struct task_spec_profiler
		{
			perf_counter_ptr ptr[PREF_COUNTER_COUNT];
			bool collect_call_count;
			bool is_profile;
			std::atomic<int64_t>* call_counts;
		};

		std::string profiler_output_handler(const std::vector<std::string>& args);
		void profiler_output_dependency_list_callee(std::stringstream &ss, const int &request);
		void profiler_output_dependency_list_caller(std::stringstream &ss, const int &request);
		void profiler_output_dependency_matrix(std::stringstream &ss);
		void profiler_output_information_table(std::stringstream &ss, const int &request, const bool &flag);
		void profiler_output_infomation_line(std::stringstream &ss, const int request, counter_percentile_type type, const bool flag);
		void profiler_output_top(std::stringstream &ss, const perf_counter_ptr_type &counter_type, const counter_percentile_type &type, const int &num);
	}
}