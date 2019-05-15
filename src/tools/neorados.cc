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

#include <algorithm>
#include <atomic>
#include <iostream>
#include <vector>

#include <boost/asio.hpp>
#include <boost/system/system_error.hpp>

#include "include/RADOS/RADOS.hpp"

namespace bs = boost::system;
namespace R = RADOS;

std::atomic<std::size_t> pending = 0;

template<typename F>
struct Callback {
  F f;
  Callback(F&& f) : f(std::forward<F>(f)) {
    ++pending;
  }

  template<typename... Args>
  void operator()(Args&&... args) {
    --pending;
    std::move(f)(std::forward<Args>(args)...);
  }
};

template<typename V>
std::ostream& printseq(const V& v, std::ostream& m) {
  std::for_each(v.cbegin(), v.cend(),
		[&m](const auto& e) {
		  m << e << std::endl;
		});
  return m;
}

template<typename V, typename F>
std::ostream& printseq(const V& v, std::ostream& m, F&& f) {
  std::for_each(v.cbegin(), v.cend(),
		[&m, &f](const auto& e) {
		  m << f(e) << std::endl;
		});
  return m;
}

template<typename F>
void lookup_pool(R::RADOS& r, const std::string& pname, F&& f) {
  ++pending;
  r.lookup_pool(pname,
		[f = std::move(f)](bs::error_code ec, std::int64_t p) {
		  --pending;
		  if (ec)
		    throw bs::system_error(ec);
		  std::move(f)(p);
		});
}


void lspools(R::RADOS& r) {
  ++pending;
  r.list_pools([](std::vector<std::pair<std::int64_t, std::string>>&& l) {
		 --pending;
		 printseq(l,
			  std::cout, [](const auto& p) -> const std::string& {
				       return p.second;
				     });
	       });
}

void ls(R::RADOS& r, const std::string& pname) {
  ++pending;
  lookup_pool(r, pname,
	      [&r](std::int64_t p) {
		--pending;
		auto next = std::make_unique<R::EnumerationCursor>();
		auto v = std::make_unique<std::vector<R::EnumeratedObject>>();
		++pending;
		r.enumerate_objects(
		  p, R::EnumerationCursor::begin(),
		  R::EnumerationCursor::end(), 1000, {}, v.get(), next.get(),
		  [v = std::move(v), next = std::move(next)]
		  (bs::error_code ec) {
		    --pending;
		    printseq(*v, std::cout);
		  }, R::all_nspaces);
	      });
}

int main(int argc, char** argv)
{
  boost::asio::io_context c;
  auto r = RADOS::RADOS::Builder{}.build(c);

  // Do this properly later.

  try {
    if (argc == 0) {
      return 1;
    }
    if (argv[1] == "lspools"sv) {
      lspools(r);
    } else if (argv[1] == "ls"sv) {
      if (argc < 2)
        return 1;
      ls(r, argv[2]);
    } else {
      return 1;
    }
    while (pending) {
      c.run();
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }

  return 0;
}
