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
#include <iostream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <boost/system/system_error.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "include/RADOS/RADOS.hpp"

using namespace std::literals;

namespace bs = boost::system;
namespace R = RADOS;

std::string verstr(const std::tuple<uint32_t, uint32_t, uint32_t>& v) {
  auto [maj, min, p] = v;
  return fmt::format("v{}.{}.{}", maj, min, p);
}

template<typename V>
void printseq(const V& v, std::ostream& m) {
  std::for_each(v.cbegin(), v.cend(),
		[&m](const auto& e) {
		  fmt::print(m, "{}\n", e);
		});
}

template<typename V, typename F>
void printseq(const V& v, std::ostream& m, F&& f) {
  std::for_each(v.cbegin(), v.cend(),
		[&m, &f](const auto& e) {
		  fmt::print(m, "{}\n", f(e));
		});
}

template<typename F>
void lookup_pool(R::RADOS& r, const std::string& pname, F&& f) {
  r.lookup_pool(pname,
		[f = std::move(f), pname = std::move(pname)]
		(bs::error_code ec, std::int64_t p) mutable {
		  if (ec)
		    throw bs::system_error(
		      ec, fmt::format("when looking up '{}'", pname));
		  std::move(f)(p);
		});
}


void lspools(R::RADOS& r, const std::vector<std::string>&) {
  r.list_pools(
    [](std::vector<std::pair<std::int64_t, std::string>>&& l) {
      printseq(l, std::cout, [](const auto& p) -> const std::string& {
			       return p.second;
			     });
    });
}

struct Ls {
  R::RADOS& r;
  std::int64_t p;
  std::string pname;

  Ls(R::RADOS& r, std::int64_t p, std::string&& pname)
    : r(r), p(p), pname(std::move(pname)) {}

  void operator()(bs::error_code ec, std::vector<R::EnumeratedObject> ls,
		  R::EnumerationCursor next) {
    if (ec)
      throw bs::system_error(ec, fmt::format("when listing {}", pname));

    printseq(ls, std::cout);
    if (next != R::EnumerationCursor::end()) {
      r.enumerate_objects(
	p, next,
	R::EnumerationCursor::end(), 1000, {},
	Ls(r, p, std::move(pname)), R::all_nspaces);
    }
  }
};

void ls(R::RADOS& r, const std::vector<std::string>& p) {
  const auto& pname = p[0];
  lookup_pool(r, pname,
	      [&r, pname = std::string(pname)](std::int64_t p) mutable {
		r.enumerate_objects(
		  p, R::EnumerationCursor::begin(),
		  R::EnumerationCursor::end(), 1000, {},
		  Ls(r, p, std::move(pname)), R::all_nspaces);
	      });
}

void mkpool(R::RADOS& r, const std::vector<std::string>& p) {
  auto pname = p[0];
  r.create_pool(pname, std::nullopt,
		[pname](bs::error_code ec) {
		  if (ec)
		    throw bs::system_error(
		      ec, fmt::format("when creating '{}'", pname));
		});
}

void rmpool(R::RADOS& r, const std::vector<std::string>& p) {
  auto pname = p[0];
  r.delete_pool(pname,
		[pname](bs::error_code ec) {
		  if (ec)
		    throw bs::system_error(
		      ec, fmt::format("when removing '{}'", pname));
		});
}

static constexpr auto version = std::make_tuple(0ul, 0ul, 1ul);

using cmdfunc = void (*)(R::RADOS& r, const std::vector<std::string>& p);

struct cmdesc {
  std::string_view name;
  std::size_t arity;
  cmdfunc f;
  std::string_view usage;
  std::string_view desc;
};

const std::array commands = {
  // Pools operations ;)

  cmdesc{ "lspools"sv,
	  0, &lspools,
	  ""sv,
	  "List all pools"sv },

  // Pool operations

  cmdesc{ "ls"sv,
	  1, &ls,
	  "POOL"sv,
	  "list all objects in POOL"sv },
  cmdesc{ "mkpool"sv,
	  1, &mkpool,
	  "POOL"sv,
	  "create POOL"sv },
  cmdesc{ "rmpool"sv,
	  1, &rmpool,
	  "POOL"sv,
	  "remove POOL"sv }
};

int main(int argc, char* argv[])
{
  std::string_view prog(argv[0]);
  std::string command;
  namespace po = boost::program_options;
  try {
    std::vector<std::string> parameters;

    po::options_description desc(fmt::format("{} options", prog));
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
      fmt::print("{}", desc);
      fmt::print("\nCommands:\n");
      for (const auto& cmd : commands) {
	fmt::print("\t{} {}\n\t\t{}\n",
		   cmd.name, cmd.usage, cmd.desc);
      }
      return 0;
    }

    if (vm.count("version")) {
      fmt::print(
	"{}: RADOS command exerciser, {},\n"
	"RADOS library version {}\n"
	"Copyright (C) 2019 Red Hat <contact@redhat.com>\n"
	"This is free software; you can redistribute it and/or\n"
	"modify it under the terms of the GNU Lesser General Public\n"
	"License version 2.1, as published by the Free Software\n"
	"Foundation.  See file COPYING.\n", prog,
	verstr(version), verstr(R::RADOS::version()));
      return 0;
    }

    if (vm.find("command") == vm.end()) {
      fmt::print(std::cerr, "{}: A command is required\n", prog);
      return 1;
    }

    boost::asio::io_context c;
    auto r = RADOS::RADOS::Builder{}.build(c);

    if (auto ci = std::find_if(commands.begin(), commands.end(),
			       [&command](const cmdesc& c) {
				 return c.name == command;
			       }); ci != commands.end()) {
      if (parameters.size() < ci->arity) {
	fmt::print(std::cerr, "{}: {}: too few arguments\n\t{} {}\n",
		   prog, command, ci->name, ci->usage);
	return 1;
      }
      if (parameters.size() > ci->arity) {
	fmt::print(std::cerr, "{}: {}: too many arguments\n\t{} {}\n",
		   prog, command, ci->name, ci->usage);
	return 1;
      }
      ci->f(r, parameters);
    } else {
      fmt::print(std::cerr, "{}: {}: unknown command\n", prog, command);
      return 1;
    }
    c.run();
  } catch (const std::exception& e) {
    fmt::print(std::cerr, "{}: {}: {}\n", prog, command, e.what());
    return 1;
  }

  return 0;
}
