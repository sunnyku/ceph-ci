// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <initializer_list>
#include <optional>
#include <thread>
#include <tuple>
#include <string_view>
#include <vector>

#include <sys/param.h>

#include <unistd.h>

#include <boost/system/system_error.hpp>

#include <fmt/format.h>

#include "include/RADOS/RADOS.hpp"

#include "include/scope_guard.h"

#include "common/asio_misc.h"
#include "common/ceph_time.h"
#include "common/ceph_argparse.h"
#include "common/waiter.h"

#include "global/global_init.h"

namespace bs = boost::system;

template<typename V>
std::ostream& printseq(const V& v, std::ostream& m) {
  std::cout << "[";
  auto o = v.cbegin();
  while (o != v.cend()) {
    std::cout << *o;
    if (++o != v.cend())
      std::cout << " ";
  }
  std::cout << "]" << std::endl;
  return m;
}

template<typename V, typename F>
std::ostream& printseq(const V& v, std::ostream& m, F&& f) {
  std::cout << "[";
  auto o = v.cbegin();
  while (o != v.cend()) {
    std::cout << f(*o);
    if (++o != v.cend())
      std::cout << " ";
  }
  std::cout << "]" << std::endl;
  return m;
}

std::int64_t lookup_pool(RADOS::RADOS& r, const std::string& pname) {
  int64_t pool;
  ceph::waiter<boost::system::error_code, std::int64_t> w;
  r.lookup_pool(pname, w.ref());
  boost::system::error_code ec;
  std::tie(ec, pool) = w.wait();
  if (ec)
    throw bs::system_error(ec);
  return pool;
}


#if 0

boost::system::error_code create_several(RADOS::RADOS& r,
                                         const RADOS::IOContext& i,
                                         std::initializer_list<std::string> l) {
  for (const auto& o : l) {
    ceph::waiter<boost::system::error_code> w;
    RADOS::WriteOp op;
    std::cout << "Creating " << o << std::endl;
    ceph::bufferlist bl;
    bl.append("My bologna has no name.");
    op.write_full(std::move(bl));
    r.execute(o, i, std::move(op), w.ref());
    auto ec = w.wait();
    if (ec) {
      std::cerr << "RADOS::execute: " << ec << std::endl;
      return ec;
    }
  }
  return {};
}

#endif

void lspools(RADOS::RADOS& r) {
  ceph::waiter<std::vector<std::pair<std::int64_t, std::string>>> w;
  r.list_pools(w.ref());
  auto pools = w.wait();

  printseq(pools, std::cout, [](const auto& p) -> const std::string& {
                               return p.second;
                             });
}

void ls(RADOS::RADOS& r, const std::string& pname) {
  std::int64_t pool = lookup_pool(r, pname);

  ceph::waiter<boost::system::error_code> w;
  RADOS::EnumerationCursor next;
  std::vector<RADOS::EnumeratedObject> v;

  r.enumerate_objects(pool,
                      RADOS::EnumerationCursor::begin(),
                      RADOS::EnumerationCursor::end(),
                      1000, {}, &v, &next, w.ref(),
                      RADOS::all_nspaces);
  auto ec = w.wait();
  if (ec)
    throw bs::system_error(ec);

  printseq(v, std::cout);
}

int main(int argc, char** argv)
{
  using namespace std::literals;

  std::vector<const char*> args;
  argv_to_vec(argc, const_cast<const char**>(argv), args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  ceph::io_context_pool p(cct.get());
  RADOS::RADOS r(p, cct.get());

  // Do this properly later.

  try {
    if (args.size() == 0) {
      return 1;
    }
    if (args[0] == "lspools"sv) {
      lspools(r);
    } else if (args[0] == "ls"sv) {
      if (args.size() < 2)
        return 1;
      ls(r, args[1]);
    }

    else {
      return 1;
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }


#if 0


  {
    ceph::waiter<boost::system::error_code> w;
    r.create_pool(pool_name, std::nullopt, w.ref());
    auto ec = w.wait();
    if (ec) {
      std::cerr << "RADOS::create_pool: " << ec << std::endl;
      return 1;
    }
  }

  auto pd = make_scope_guard(
    [&pool_name, &r]() {
      ceph::waiter<boost::system::error_code> w;
      r.delete_pool(pool_name, w.ref());
      auto ec = w.wait();
      if (ec)
        std::cerr << "RADOS::delete_pool: " << ec << std::endl;
    });


  RADOS::IOContext i(pool);

  if (noisy_list(r, pool)) {
    return 1;
  }

  if (create_several(r, i, {"meow", "woof", "squeak"})) {
    return 1;
  }

  std::this_thread::sleep_for(5s);

  if (noisy_list(r, pool)) {
    return 1;
  }

#endif

  return 0;
}
