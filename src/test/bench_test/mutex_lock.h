// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once
#include "sync.h"

namespace pegasus {
namespace test {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the mutex_lock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     mutex_lock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class mutex_lock {
 public:
  explicit mutex_lock(mutex *mu) : mu_(mu) { this->mu_->lock(); }
  ~mutex_lock() { this->mu_->unlock(); }

 private:
  mutex *const mu_;
  // No copying allowed
  mutex_lock(const mutex_lock&);
  void operator=(const mutex_lock&);
};
}  // namespace rocksdb
}
