
#include<stdio.h>
#include "profiler_header.h"
#pragma comment(lib, "Pdh.lib")

namespace dsn {
	namespace tools {
		bool PROFILER_PDH::install()
		{
			HModule = LoadLibraryEx("Pdh.dll", NULL, LOAD_LIBRARY_AS_DATAFILE);
			if (HModule == NULL)
			{
				printf("Cannot open library\n");
				return false;
			}

			status = PdhOpenQuery(0, 0, &HQuery);
			if (status != ERROR_SUCCESS)
			{
				printf("Cannot open query\n");
				Error_Display();
				return false;
			}
			CounterHandle_Time = (HCOUNTER*)GlobalAlloc(GPTR, sizeof(HCOUNTER));

			status = PdhAddCounter(HQuery, "\\Processor Information(_Total)\\% Processor Time", 0, CounterHandle_Time);
			if (status != ERROR_SUCCESS)
			{
				printf("Add counter error\n");
				Error_Display();
				return false;
			}

			status = PdhCollectQueryData(HQuery);
			if (status != ERROR_SUCCESS)
			{
				printf("Query Collection Error\n");
				Error_Display();
				return false;
			}

			std::shared_ptr<boost::asio::deadline_timer> timer(new boost::asio::deadline_timer(shared_io_service::instance().ios));
			timer->expires_from_now(boost::posix_time::seconds(pdh_calc_interval));
			timer->async_wait(std::bind(on_timer, timer, std::placeholders::_1));

			return true;
		}

		void PROFILER_PDH::end()
		{
			status = PdhRemoveCounter(*CounterHandle_Time);
			if (status != ERROR_SUCCESS)
			{
				printf("Counter Removing Error\n");
				Error_Display();
			}

			status = PdhCloseQuery(HQuery);
			if (status != ERROR_SUCCESS)
			{
				printf("Close Abnormally\n");
				Error_Display();
			}
			FreeLibrary(HModule);
		}
			
		void PROFILER_PDH::profiler_CPU(std::stringstream &ss)
		{
			ss << CounterValue_CPU << "%";
		}

		void PROFILER_PDH::Error_Display()
		{
			LPVOID message;
			FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_FROM_HMODULE | FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_IGNORE_INSERTS, HModule, status, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR)&message, 0, NULL);
			printf("%s", message);
			LocalFree(message);
		}

		bool PROFILER_PDH::calc()
		{
			PDH_FMT_COUNTERVALUE CounterValue;
			DWORD valuetype;

			status = PdhCollectQueryData(HQuery);
			if (status != ERROR_SUCCESS)
			{
				printf("Query Collection Error\n");
				Error_Display();
				return false;
			}

			status = PdhGetFormattedCounterValue(*CounterHandle_Time, PDH_FMT_DOUBLE, &valuetype, &CounterValue);
			if (status != ERROR_SUCCESS)
			{
				printf("CounterValue Calculation Error\n");
				Error_Display();
				return false;
			}

			CounterValue_CPU = CounterValue.doubleValue;
			return true;
		}

		void PROFILER_PDH::on_timer(std::shared_ptr<boost::asio::deadline_timer>& timer, const boost::system::error_code& ec)
		{
			if (!ec)
			{
				calc();

				std::shared_ptr<boost::asio::deadline_timer> timer(new boost::asio::deadline_timer(shared_io_service::instance().ios));
				timer->expires_from_now(boost::posix_time::seconds(pdh_calc_interval));
				timer->async_wait(std::bind(on_timer, timer, std::placeholders::_1));
			}
			else
			{
				// TODO: err handling
			}
		}

		HMODULE PROFILER_PDH::HModule;
		HQUERY PROFILER_PDH::HQuery = NULL;
		HCOUNTER* PROFILER_PDH::CounterHandle_Time = NULL;
		PDH_STATUS PROFILER_PDH::status;
		double PROFILER_PDH::CounterValue_CPU = 0;
	}
}