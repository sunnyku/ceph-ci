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
#include <functional>
#include <iostream>
#include <vector>

#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <boost/system/system_error.hpp>

#include "include/RADOS/RADOS.hpp"

namespace bs = boost::system;
namespace R = RADOS;

std::atomic<std::size_t> pending = 0;

template<typename F>
struct Pending {
  F f;
  Pending(F&& f) : f(std::forward<F>(f)) {
    ++pending;
  }

  template<typename... Args>
  void operator()(Args&&... args) {
    std::move(f)(std::forward<Args>(args)...);
    --pending;
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
  r.lookup_pool(pname,
		Pending([f = std::move(f)](bs::error_code ec, std::int64_t p)
			mutable {
			  if (ec)
			    throw bs::system_error(ec);
			  std::move(f)(p);
			}));
}


void lspools(R::RADOS& r) {
  r.list_pools(
    Pending([](std::vector<std::pair<std::int64_t, std::string>>&& l) {
	      printseq(l,
		       std::cout, [](const auto& p) -> const std::string& {
				    return p.second;
				  });
	    }));
}

struct Ls : public Pending<std::reference_wrapper<Ls>> {
  R::RADOS& r;
  std::int64_t p;

  Ls(R::RADOS& r, std::int64_t p)
    : Pending(std::ref(*this)), r(r),  p(p) {}

  void operator()(bs::error_code ec, std::vector<R::EnumeratedObject> ls,
		  R::EnumerationCursor next) {
    printseq(ls, std::cout);
    if (next != R::EnumerationCursor::end()) {
      r.enumerate_objects(
	p, R::EnumerationCursor::begin(),
	R::EnumerationCursor::end(), 1000, {},
	Ls(r, p), R::all_nspaces);
    }
  }
};

void ls(R::RADOS& r, const std::string& pname) {
  lookup_pool(r, pname,
	      Pending(
		[&r](std::int64_t p) {
		  r.enumerate_objects(
		    p, R::EnumerationCursor::begin(),
		    R::EnumerationCursor::end(), 1000, {},
		    Ls(r, p), R::all_nspaces);
		}));
}

int main(int argc, char* argv[])
{
  namespace po = boost::program_options;
  try {
    std::string command;
    std::vector<std::string> parameters;

    po::options_description desc("neorados options");
    desc.add_options()
      ("help", "show help")
      ("version", "show version")
      ("command", po::value<std::string>(&command), "the operation to perform")
      ("parameters", po::value<std::vector<std::string>>(&parameters),
       "parameters to the command");

    po::positional_options_description p;
    p.add("command", 1);
    p.add("parameters", -1);

    po::variables_map vm;

    po::store(po::command_line_parser(argc, argv).
	      options(desc).positional(p).run(), vm);

    po::notify(vm);

    if (vm.count("help")) {
      std::cout << desc;
      return 0;
    }

    if (vm.count("version")) {
      std::cout
	<< "neorados: RADOS command exerciser, v0.0.1\n"
	<< "Copyright (C) 2019 Red Hat <contact@redhat.com>\n"
	<< "This is free software; you can redistribute it and/or\n"
	<< "modify it under the terms of the GNU Lesser General Public\n"
	<< "License version 2.1, as published by the Free Software\n"
	<< "Foundation.  See file COPYING." << std::endl;
      return 0;
    }

    if (vm.find("command") == vm.end()) {
      std::cerr << "A command is required." << std::endl;
      return 1;
    }

    boost::asio::io_context c;
    auto r = RADOS::RADOS::Builder{}.build(c);

    // Do this properly later.

    if (command == "lspools"sv) {
      if (!parameters.empty()) {
	std::cerr << "lspools takes no parameters" << std::endl;
	return 1;
      }
      lspools(r);
    }
    else if (command == "ls"sv) {
      if (parameters.empty()) {
	std::cerr << "ls requires pool argument" << std::endl;
	return 1;
      } else if (parameters.size() > 1) {
	std::cerr << "ls requires exactly more than one argument" << std::endl;
	return 1;
      }
      ls(r, parameters[1]);
    }
    else {
      std::cerr << "unknown command: " << command << std::endl;
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
