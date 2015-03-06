#pragma once
#include "Zion/ToolAPI.h"
#include <vector>

namespace Zion { namespace Tools {

class ModelCheckingExtension
{
public:
    ModelCheckingExtension();

    virtual void Initialize(const std::vector<Service::ServiceApp *> &apps);
	virtual void GlobalAssert();	
    virtual uint64_t GetStateSig();

protected:
    uint64_t m_sig;
};

extern void RegisterModelChecking();

extern void RegisterModelCheckingExtension(ModelCheckingExtension *ext);
}}