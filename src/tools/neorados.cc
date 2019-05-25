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
#include <cassert>
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

#include "include/buffer.h" // :(

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

class Thrower {
private:
  std::string what;

public:
  Thrower(std::string&& what)
    : what(std::move(what)) {}

  void operator ()(bs::error_code ec) {
    if (ec)
      throw bs::system_error(ec, std::move(what));
  }
};

void mkpool(R::RADOS& r, const std::vector<std::string>& p) {
  auto pname = p[0];
  r.create_pool(pname, std::nullopt,
		Thrower(fmt::format("when creating pool '{}'", pname)));
}

void rmpool(R::RADOS& r, const std::vector<std::string>& p) {
  auto pname = p[0];
  r.delete_pool(pname,
		Thrower(fmt::format("when removing pool '{}'", pname)));
}

void create(R::RADOS& r, const std::vector<std::string>& p) {
  auto pname = p[0];
  auto oname = p[1];
  lookup_pool(r, pname,
	      [&r, pname, oname = std::move(oname)](std::int64_t pool) mutable {
		RADOS::WriteOp op;
		op.create(true);
		r.execute(oname, pool, std::move(op),
		Thrower(
		  fmt::format(
		    "when creating object '{}' in pool '{}'",
		    oname, pname)));
    });
}

static constexpr std::size_t io_size = 4 << 20;

class Writer {
  RADOS::RADOS& r;
  std::unique_ptr<char[]> buf = std::make_unique<char[]>(io_size);
  const std::int64_t pool;
  const std::string pname;
  const RADOS::Object obj;
  std::size_t off = 0;
  std::ios_base::iostate old_ex = cin.exceptions();

public:

  Writer(RADOS::RADOS& r, std::int64_t pool, std::string&& pname,
	 std::string&& obj)
    : r(r), pool(pool), pname(pname), obj(std::move(obj)) {
    std::cin.exceptions(std::istream::badbit);
    std::cin.clear();
  }

  ~Writer() {
    std::cin.exceptions(old_ex);
  }

  void write(std::unique_ptr<Writer> me) {
    auto curoff = off;
    std::cin.read(buf.get(), io_size);
    auto len = std::cin.gcount();
    off += len;
    if (len == 0)
      return; // Nothin' to do.

    ceph::buffer::list bl;
    bl.append(buffer::create_static(len, buf.get()));
    RADOS::WriteOp op;
    op.write(curoff, std::move(bl));
    r.execute(obj, pool, std::move(op),
	      [me = std::move(me),
	       eof = std::cin.eof()](bs::error_code ec) mutable {
		if (ec)
		  throw bs::system_error(
		    ec,
		    fmt::format("when writing object '{}' in pool '{}'",
				me->obj, me->pname));
		if (!eof) {
		  auto u = me.get();
		  u->write(std::move(me));
		}
	      });
  }
};

void write(R::RADOS& r, const std::vector<std::string>& p) {
  auto pname = p[0];
  auto oname = p[1];
  lookup_pool(r, pname,
	      [&r, pname, oname = std::move(oname)](std::int64_t pool) mutable {
		auto w = std::make_unique<Writer>(r, pool, std::move(pname),
						  std::move(oname));
		auto u = w.get();
		u->write(std::move(w));
    });
}

class Reader {
  RADOS::RADOS& r;
  ceph::buffer::list bl;
  const std::int64_t pool;
  const std::string pname;
  const RADOS::Object obj;
  std::size_t off = 0;
  std::size_t len;

public:

  Reader(RADOS::RADOS& r, std::int64_t pool, std::string&& pname,
	 std::string&& obj)
    : r(r), pool(pool), pname(pname), obj(std::move(obj)) {
  }

  void start_read(std::unique_ptr<Reader> me) {
    RADOS::ReadOp op;
    op.stat(&len, nullptr);
    r.execute(obj, pool, std::move(op),
	      nullptr,
	      [me = std::move(me)](bs::error_code ec) mutable {
		if (ec)
		  throw bs::system_error(
		    ec,
		    fmt::format("when getting length of object '{}' in pool '{}'",
				me->obj, me->pname));

		if (me->len) {
		  auto u = me.get();
		  u->read(std::move(me));
		}
	      });
  }

  void read(std::unique_ptr<Reader> me) {
    bl.clear();
    auto toread = std::max(len - off, io_size);
    if (toread == 0)
      return;
    RADOS::ReadOp op;
    op.read(off, toread, &bl);
    r.execute(obj, pool, std::move(op), nullptr,
	      [me = std::move(me)](bs::error_code ec) mutable {
		if (ec)
		  throw bs::system_error(
		    ec,
		    fmt::format("when reading from object '{}' in pool '{}'",
				me->obj, me->pool));
		me->off += me->bl.length();
		me->bl.write_stream(std::cout);
		if (me->off != me->len) {
		  auto u = me.get();
		  u->read(std::move(me));
		}
	      });
  }
};

void read(R::RADOS& r, const std::vector<std::string>& p) {
  auto pname = p[0];
  auto oname = p[1];
  lookup_pool(r, pname,
	      [&r, pname, oname = std::move(oname)](std::int64_t pool) mutable {
		auto d = std::make_unique<Reader>(r, pool, std::move(pname),
						  std::move(oname));
		auto u = d.get();
		u->start_read(std::move(d));
    });
}

void rm(R::RADOS& r, const std::vector<std::string>& p) {
  auto pname = p[0];
  auto oname = p[1];
  lookup_pool(r, pname,
	      [&r, pname, oname = std::move(oname)](std::int64_t pool) mutable {
		RADOS::WriteOp op;
		op.remove();
		r.execute(oname, pool, std::move(op),
			  Thrower(
		  fmt::format(
		    "when removing object '{}' in pool '{}'",
		    oname, pname)));
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
	  "remove POOL"sv },

  // Object operations

  cmdesc{ "create"sv,
	  2, &create,
	  "POOL OBJECT"sv,
	  "exclusively create OBJECT in POOL"sv },
  cmdesc{ "write"sv,
	  2, &write,
	  "POOL OBJECT"sv,
	  "write to OBJECT in POOL from standard input"sv },
  cmdesc{ "read"sv,
	  2, &read,
	  "POOL OBJECT"sv,
	  "read contents of OBJECT in POOL to standard out"sv },
  cmdesc{ "rm"sv,
	  2, &rm,
	  "POOL OBJECT"sv,
	  "remove OBJECT in POOL"sv }
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
