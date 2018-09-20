// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

// What and why
// ============
//
// For general code making use of mutexes, use these ceph:: types.  The main
// difference is that the ceph::mutex ctor takes a string argument naming
// the lock for use by lockdep.
//
// For legacy Mutex users that passed recursive=true, use ceph::recursive_mutex.
//
// For legacy Mutex users that passed lockdep=false, use std::mutex directly.

#ifdef CEPH_DEBUG_MUTEX

// ============================================================================
// debug (lockdep-capable, various sanity checks and asserts)
// ============================================================================

#include "common/mutex_debug.h"
#include "common/condition_variable_debug.h"

namespace ceph {
  typedef ceph::mutex_debug mutex;
  typedef ceph::mutex_recursive_debug recursive_mutex;
  typedef ceph::condition_variable_debug condition_variable;
}

#else

// ============================================================================
// fast and minimal
// ============================================================================

#include <mutex>
#include <string>

namespace ceph {

  class mutex {
    std::mutex m;

  public:
    mutex(const mutex&) = delete;
    mutex(mutex&) = delete;

    // mutex_debug-style ctor for compat with the CEPH_DEBUG_MUTEX
    mutex(const std::string &n,
	  bool bt=false) noexcept {}
    mutex(const char *n,
	  bool bt=false) noexcept {}

    // enable silent conversion to std::mutex, e.g. for unique_lock<>
    // (see deduction guide below)
    operator std::mutex&() {
      return m;
    }

    void lock() {
      m.lock();
    }
    void unlock() {
      m.unlock();
    }
    bool try_lock() {
      return m.try_lock();
    }
    std::mutex::native_handle_type native_handle() {
      return m.native_handle();
    }

    // mutex_debug-like debug methods.  Note that these can blindly return true
    // because any code that does anything other than assert these
    // are true is broken.
    bool is_locked() const {
      return true;
    }
    bool is_locked_by_me() const {
      return true;
    }
  };

  class recursive_mutex {
    std::recursive_mutex m;

  public:
    recursive_mutex(const recursive_mutex&) = delete;
    recursive_mutex(recursive_mutex&) = delete;

    // recursive_mutex_debug-style ctor for compat with the CEPH_DEBUG_MUTEX
    recursive_mutex(const std::string &n,
		    bool backtraces=false) noexcept {}
    recursive_mutex(const char *n,
		    bool backtraces=false) noexcept {}

    // enable silent conversion to std::recursive_mutex, e.g. for
    // unique_lock<> (see deduction guide below)
    operator std::recursive_mutex&() {
      return m;
    }

    void lock() {
      m.lock();
    }
    void unlock() {
      m.unlock();
    }
    bool try_lock() {
      return m.try_lock();
    }
    std::recursive_mutex::native_handle_type native_handle() {
      return m.native_handle();
    }

    // recursive_mutex_debug-like debug methods.  Note that these can
    // blindly return true because any code that does anything other
    // than assert these are true is broken.
    bool is_locked() const {
      return true;
    }
    bool is_locked_by_me() const {
      return true;
    }
  };

  typedef std::condition_variable condition_variable;
}

namespace std {
  // deduction guides for unique_lock<>.  this is important for
  // std::condition_variable, which needs a
  // std::unique_lock<std::mutex>, not std::unique_lock<ceph::mutex>.
  //
  // WARNING:  http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2017/n4659.pdf#paragraph.20.5.4.2.1
  // says:
  //
  //   The behavior of a C++ program is undefined if it declares ... a
  //   deduction guide for any standard library class template.
  //
  // We don't think this is likely in practice to cause problems, so we're
  // doing it anyway.  If it *does* turn out to be problematic, we can work
  // around it with a typedev std::unique_lock<std::mutex> unique_lock in the
  // ceph namespace and update all of the users to ceph::unique_lock instead of
  // std::unique_lock.
  unique_lock(ceph::mutex&) -> unique_lock<std::mutex>;

  // let's do the same for recursive_mutex too (even though it is not
  // useful for std::condition_variable).
  unique_lock(ceph::recursive_mutex&) -> unique_lock<std::recursive_mutex>;
}

#endif
