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

#include <string>

#include "common/error_code.h"
#include "error_code.h"

namespace bs = boost::system;

class osdc_error_category : public ceph::converting_category {
public:
  osdc_error_category(){}
  const char* name() const noexcept override;
  std::string message(int ev) const noexcept override;
  bs::error_condition default_error_condition(int ev) const noexcept
    override;
  bool equivalent(int ev, const bs::error_condition& c) const
    noexcept override;
  using ceph::converting_category::equivalent;
  int from_code(int ev) const noexcept override;
};

const char* osdc_error_category::name() const noexcept {
  return "osdc";
}

std::string osdc_error_category::message(int ev) const noexcept {
  using namespace ::osdc_errc;


  switch (ev) {
  case 0:
    return "No error";

  case pool_dne:
    return "Pool does not exist";

  case pool_exists:
    return "Pool already exists";

  case precondition_violated:
    return "Precondition for operation not satisfied";

  case not_supported:
    return "Operation not supported";

  case snapshot_exists:
    return "Snapshot already exists";

  case snapshot_dne:
    return "Snapshot does not exist";

  case timed_out:
    return "Operation timed out";
  }

  return "Unknown error";
}

bs::error_condition
osdc_error_category::default_error_condition(int ev) const noexcept {
  using namespace ::osdc_errc;
  switch (ev) {
  case pool_dne:
    return ceph::errc::does_not_exist;
  case pool_exists:
    return ceph::errc::exists;
  case precondition_violated:
    return bs::errc::invalid_argument;
  case not_supported:
    return bs::errc::operation_not_supported;
  case snapshot_exists:
    return ceph::errc::exists;
  case snapshot_dne:
    return ceph::errc::does_not_exist;
  case timed_out:
    return bs::errc::timed_out;
  }

  return { ev, *this };
}

bool osdc_error_category::equivalent(int ev,
                                     const bs::error_condition& c) const noexcept {
  using namespace ::osdc_errc;
  if (ev == pool_dne) {
    if (c == bs::errc::no_such_file_or_directory) {
      return true;
    }
    if (c == ceph::errc::not_in_map) {
      return true;
    }
  }
  if (ev == pool_exists) {
    if (c == bs::errc::file_exists) {
      return true;
    }
  }
  if (ev == snapshot_exists) {
    if (c == bs::errc::file_exists) {
      return true;
    }
  }
  if (ev == snapshot_dne) {
    if (c == bs::errc::no_such_file_or_directory) {
      return true;
    }
    if (c == ceph::errc::not_in_map) {
      return true;
    }
  }

  return default_error_condition(ev) == c;
}

int osdc_error_category::from_code(int ev) const noexcept {
  using namespace ::osdc_errc;
  switch (ev) {
  case pool_dne:
    return -ENOENT;
  case pool_exists:
    return -EEXIST;
  case precondition_violated:
    return -EINVAL;
  case not_supported:
    return -EOPNOTSUPP;
  case snapshot_exists:
    return -EEXIST;
  case snapshot_dne:
    return -ENOENT;
  case timed_out:
    return -ETIMEDOUT;
  }
  return -EDOM;
}

const bs::error_category& osdc_category() noexcept {
  static const osdc_error_category c;
  return c;
}
