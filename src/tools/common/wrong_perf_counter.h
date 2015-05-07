/*
* The MIT License (MIT)

* Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:

* The above copyright notice and this permission notice shall be included in
* all copies or substantial portions of the Software.

* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
* THE SOFTWARE.
*/
#pragma once

# include <dsn/tool_api.h>
#include "shared_io_service.h"
const int sleep_seconds = 2;
# define MAX_TYPE_NUMBER 5
# define MAX_QUEUE_LENGTH 50000
# define _LEFT 0
# define _RIGHT 1
# define _QLEFT 2
# define _QRIGHT 3
namespace dsn {
	namespace tools {

		// NUMBER
		class perf_counter_number : public perf_counter
		{
		public:
			perf_counter_number(const char *section, const char *name, perf_counter_type type)
				: perf_counter(section, name, type), _val(0){}
			~perf_counter_number(void) {}

			virtual void   increment() { _val++; }
			virtual void   decrement() { _val--; }
			virtual void   add(uint64_t val) { _val += val; }
			virtual void   set(uint64_t val) { dassert(false, "the task doesn't have this method"); }
			virtual double get_value(counter_percentile_type type) { return static_cast<double>(_val.load()); }

		private:
			std::atomic<uint64_t> _val;
		};

		// RATE
		class perf_counter_rate : public perf_counter
		{
		public:
			perf_counter_rate(const char *section, const char *name, perf_counter_type type)
				: perf_counter(section, name, type), _val(0)
			{
				qts = 0;
			}
			~perf_counter_rate(void) {}

			virtual void   increment() { _val++; }
			virtual void   decrement() { _val--; }
			virtual void   add(uint64_t val) { _val += val; }
			virtual void   set(uint64_t val) { dassert(false, "the task doesn't have this method"); }
			virtual double get_value(counter_percentile_type type)
			{
				uint64_t now = ::dsn::service::env::now_ns();
				uint64_t interval = now - qts;
				double val = static_cast<double>(_val.load());
				qts = now;
				_val = 0;
				return val / interval * 1000 * 1000 * 1000;
			}

		private:
			std::atomic<uint64_t> _val;
			std::atomic<uint64_t> qts;
		};

		// NUMBER_PERCENTILE
		class perf_counter_number_percentile : public perf_counter
		{
		public:
			perf_counter_number_percentile(const char *section, const char *name, perf_counter_type type)
				: perf_counter(section, name, type), _tail(0)
			{
				std::shared_ptr<boost::asio::deadline_timer> timer(new boost::asio::deadline_timer(shared_io_service::instance().ios));
				timer->expires_from_now(boost::posix_time::seconds(sleep_seconds));
				timer->async_wait(std::bind(&perf_counter_number_percentile::on_timer, this, timer, std::placeholders::_1));
			}
			~perf_counter_number_percentile(void) {}

			virtual void   increment() { dassert(false, "the task doesn't have this method"); }
			virtual void   decrement() { dassert(false, "the task doesn't have this method"); }
			virtual void   add(uint64_t val) { dassert(false, "the task doesn't have this method"); }
			virtual void   set(uint64_t val)
			{
				auto idx = _tail++;
				_queue[idx % MAX_QUEUE_LENGTH] = val;
			}
			virtual double get_value(counter_percentile_type type)
			{
				if (_tail == 0)
					return -1.0;
				if ((type < 0) || (type >= COUNTER_PERCENTILE_COUNT))
				{
					dassert(false, "send a wrong counter percentile type");
					return -1;
				}
				return _ans[type];
			}

		private:
			inline void insert_calc_queue(int left, int right, int qleft, int qright, int &calc_tail)
			{
				calc_tail++;
				calc_queue[calc_tail][_LEFT] = left;
				calc_queue[calc_tail][_RIGHT] = right;
				calc_queue[calc_tail][_QLEFT] = qleft;
				calc_queue[calc_tail][_QRIGHT] = qright;
				return;
			}

			uint64_t find_mid(int left, int right)
			{
				if (left == right)
					return mid_tmp[left];

				int index;
				for (index = left; index < right; index += 5)
				{
					int remain_num = index + 5 >= right ? right - index + 1 : 5;
					for (int i = index; i < index + remain_num; i++)
					{
						int j;
						uint64_t k = mid_tmp[i];
						for (j = i - 1; (j >= index) && (mid_tmp[j] > k); j--)
							mid_tmp[j + 1] = mid_tmp[j];
						mid_tmp[j + 1] = k;
					}
					mid_tmp[(index - left) / 5] = mid_tmp[index + remain_num / 2];
				}

				return find_mid(0, (right - left - 1) / 5);
			}

			inline void select(int left, int right, int qleft, int qright, int &calc_tail)
			{
				int i, j, index, now;
				uint64_t mid;

				if (qleft > qright)
					return;

				if (left == right)
				{
					for (i = qleft; i <= qright; i++)
					if (_ask[i] == 1)
						_ans[i] = _tmp[left];
					else
						dassert(false, "select percentail wrong!!!");
					return;
				}

				for (i = left; i <= right; i++)
					mid_tmp[i] = _tmp[i];
				mid = find_mid(left, right);

				for (index = left; index <= right; index++)
				if (_tmp[index] == mid)
					break;

				_tmp[index] = _tmp[left];
				index = left;
				for (i = left, j = right; i <= j;)
				{
					while ((i <= j) && (_tmp[j] > mid)) j--;
					if (i <= j) _tmp[index] = _tmp[j], index = j--;
					while ((i <= j) && (_tmp[i] < mid)) i++;
					if (i <= j) _tmp[index] = _tmp[i], index = i++;
				}
				_tmp[index] = mid;

				now = index - left + 1;
				for (i = qleft; (i <= qright) && (_ask[i] < now); i++);
				for (j = i; j <= qright; j++) _ask[j] -= now;
				for (j = i; (j <= qright) && (_ask[j] == 0); j++) _ask[j]++;
				insert_calc_queue(left, index - 1, qleft, i - 1, calc_tail);
				insert_calc_queue(index, index, i, j - 1, calc_tail);
				insert_calc_queue(index + 1, right, j, qright, calc_tail);
				return;
			}

			void   calc(uint64_t interval)
			{
				if (_tail == 0)
					return;

				int tmp_num = _tail > MAX_QUEUE_LENGTH ? MAX_QUEUE_LENGTH : _tail.load();
				for (int i = 0; i < tmp_num; i++)
					_tmp[i] = _queue[i];

				_ask[COUNTER_PERCENTILE_50] = (int)(tmp_num * 0.5) + 1;
				_ask[COUNTER_PERCENTILE_90] = (int)(tmp_num * 0.90) + 1;
				_ask[COUNTER_PERCENTILE_95] = (int)(tmp_num * 0.95) + 1;
				_ask[COUNTER_PERCENTILE_99] = (int)(tmp_num * 0.99) + 1;
				_ask[COUNTER_PERCENTILE_999] = (int)(tmp_num * 0.999) + 1;
				// must be sorted
				// std::sort(_ask, _ask + MAX_TYPE_NUMBER);

				int l, r = 0;

				insert_calc_queue(0, tmp_num - 1, 0, COUNTER_PERCENTILE_COUNT - 1, r);
				for (l = 1; l <= r; l++)
					select(calc_queue[l][_LEFT], calc_queue[l][_RIGHT], calc_queue[l][_QLEFT], calc_queue[l][_QRIGHT], r);

				return;
			}

			void on_timer(std::shared_ptr<boost::asio::deadline_timer>& timer, const boost::system::error_code& ec)
			{
				if (!ec)
				{
					calc(sleep_seconds);
					std::shared_ptr<boost::asio::deadline_timer> timer(new boost::asio::deadline_timer(shared_io_service::instance().ios));
					timer->expires_from_now(boost::posix_time::seconds(sleep_seconds));
					timer->async_wait(std::bind(&perf_counter_number_percentile::on_timer, this, timer, std::placeholders::_1));
				}
				else
				{
					dassert(false, "on timer error!!!");
				}
			}

			std::atomic<int> _tail;
			uint64_t _queue[MAX_QUEUE_LENGTH];
			uint64_t _tmp[MAX_QUEUE_LENGTH];
			uint64_t mid_tmp[MAX_QUEUE_LENGTH];
			uint64_t _ask[COUNTER_PERCENTILE_COUNT];
			uint64_t _ans[COUNTER_PERCENTILE_COUNT];
			uint64_t calc_queue[MAX_QUEUE_LENGTH * 10][4];
		};

		class wrong_perf_counter : public perf_counter
		{
		public:
			wrong_perf_counter(const char *section, const char *name, perf_counter_type type)
				: perf_counter(section, name, type)
			{
				if (type == perf_counter_type::COUNTER_TYPE_NUMBER)
					_counter_impl = new perf_counter_number(section, name, type);
				else if (type == perf_counter_type::COUNTER_TYPE_RATE)
					_counter_impl = new perf_counter_rate(section, name, type);
				else
					_counter_impl = new perf_counter_number_percentile(section, name, type);
			}
			~wrong_perf_counter(void) {}

			virtual void   increment() { _counter_impl->increment(); }
			virtual void   decrement() { _counter_impl->decrement(); }
			virtual void   add(uint64_t val) { _counter_impl->add(val); }
			virtual void   set(uint64_t val) { _counter_impl->set(val); }
			virtual double get_value(counter_percentile_type type) { return _counter_impl->get_value(type); }

		private:
			perf_counter *_counter_impl;
		};

	}
}

