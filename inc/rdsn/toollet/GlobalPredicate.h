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