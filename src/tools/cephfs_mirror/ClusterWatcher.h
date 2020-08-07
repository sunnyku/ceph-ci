// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_CLUSTER_WATCHER_H
#define CEPHFS_MIRROR_CLUSTER_WATCHER_H

#include <map>

#include "common/ceph_mutex.h"
#include "common/async/context_pool.h"
#include "messages/MFSMap.h"
#include "msg/Dispatcher.h"
#include "Types.h"

class CephContext;
class MonClient;

namespace cephfs {
namespace mirror {

class ClusterWatcher : public Dispatcher {
public:
  struct Listener {
    virtual ~Listener() {
    }

    virtual void handle_mirroring_enabled(const FilesystemSpec &spec) = 0;
    virtual void handle_mirroring_disabled(const std::string &fs_name) = 0;

    virtual void handle_peers_added(const std::string &fs_name, const Peer &peer) = 0;
    virtual void handle_peers_removed(const std::string &fs_name, const Peer &peer) = 0;
  };

  ClusterWatcher(CephContext *cct, MonClient *monc, Listener &listener);
  ~ClusterWatcher();

  bool ms_can_fast_dispatch_any() const override {
    return true;
  }
  bool ms_can_fast_dispatch2(const cref_t<Message> &m) const override;
  void ms_fast_dispatch2(const ref_t<Message> &m) override;
  bool ms_dispatch2(const ref_t<Message> &m) override;

  void ms_handle_connect(Connection *c) override {
  }
  bool ms_handle_reset(Connection *c) override {
    return false;
  }
  void ms_handle_remote_reset(Connection *c) override {
  }
  bool ms_handle_refused(Connection *c) override {
    return false;
  }

  int init();
  void shutdown();

private:
  struct StringCmp {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const {
      return a < b;
    }
  };

  ceph::mutex m_lock = ceph::make_mutex("cephfs::mirror::cluster_watcher");
  MonClient *m_monc;
  Listener &m_listener;

  std::map<std::string, Peers, StringCmp> m_filesystem_peers;

  void handle_fsmap(const cref_t<MFSMap> &m);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_CLUSTER_WATCHER_H
