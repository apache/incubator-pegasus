/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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

# include <Zion/ToolAPI.h>
# include <Zion/ServiceAPI.h>
# include <Zion/Internal/Synchronize.h>

namespace Zion { namespace Tools {

// @ return true for terminating further checkers
typedef bool (*GlobalPredicateFailureHandler)(const char* predName);

class GlobalPredicate
{
public:
	// return false when the predicate fails
	virtual bool operator () () = 0;
};

class GlobalPredicateChecker : public Toollet
{
public:
	GlobalPredicateChecker(const char* name, ConfigurationPtr config);

	virtual void Install(ServiceSpec& spec) override 
    {
        for (int i = 0; i <= TaskCode::MaxValue(); i++)
        {
            TaskSpec::Get(i)->OnTaskEnd.PutBack(OnTaskEnd, "GlobalPredicateChecker.OnTaskEnd");
        }
    } 

	bool Add(const char* name, GlobalPredicate* pred);

	GlobalPredicate* Remove(const char* name);

	void Execute(GlobalPredicateFailureHandler failureHandler);

private:
	Utils::CRWLock                         m_lock;
	std::map<std::string, GlobalPredicate*> m_predicates;

    static void OnTaskEnd(Task* task);
};

}} // end namespace Zion::Tools
