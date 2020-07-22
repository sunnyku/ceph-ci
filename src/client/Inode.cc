// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Client.h"
#include "Inode.h"
#include "Dentry.h"
#include "Dir.h"
#include "Fh.h"
#include "MetaSession.h"
#include "ClientSnapRealm.h"
#include "Delegation.h"

#include "mds/flock.h"

Inode::~Inode()
{
  delay_cap_item.remove_myself();
  dirty_cap_item.remove_myself(); 
  snaprealm_item.remove_myself();

  for (auto &m : open_by_mode)
    ceph_assert(!m);

  if (snapdir_parent) {
    snapdir_parent->flags &= ~I_SNAPDIR_OPEN;
    snapdir_parent.reset();
  }

  if (!oset.objects.empty()) {
    lsubdout(client->cct, client, 0) << __func__ << ": leftover objects on inode 0x"
      << std::hex << ino << std::dec << dendl;
    ceph_assert(oset.objects.empty());
  }

  if (!delegations.empty()) {
    lsubdout(client->cct, client, 0) << __func__ << ": leftover delegations on inode 0x"
      << std::hex << ino << std::dec << dendl;
    ceph_assert(delegations.empty());
  }
}

ostream& operator<<(ostream &out, const Inode &in)
{
  out << in.vino() << "("
      << "faked_ino=" << in.faked_ino
      << " ref=" << in._ref
      << " ll_ref=" << in.ll_ref
      << " cap_refs=" << in.cap_refs;
  out << " open=[";
  {
    bool first = true;
    for (auto &m : in.open_by_mode) {
      if (!first)
	out << ',';
      out << m;
      first = false;
    }
  }
  out << "]";
  out << " mode=" << oct << in.mode << dec
      << " size=" << in.size << "/" << in.max_size
      << " nlink=" << in.nlink
      << " btime=" << in.btime
      << " mtime=" << in.mtime
      << " ctime=" << in.ctime
      << " caps=" << ccap_string(in.caps_issued());
  if (!in.caps.empty()) {
    out << "(";
    bool first = true;
    for (const auto &pair : in.caps) {
      if (!first)
        out << ',';
      out << pair.first << '=' << ccap_string(pair.second.issued);
      first = false;
    }
    out << ")";
  }
  if (in.dirty_caps)
    out << " dirty_caps=" << ccap_string(in.dirty_caps);
  if (in.flushing_caps)
    out << " flushing_caps=" << ccap_string(in.flushing_caps);

  if (in.flags & I_COMPLETE)
    out << " COMPLETE";

  if (in.is_file())
    out << " " << in.oset;

  if (!in.dentries.empty())
    out << " parents=" << in.dentries;

  if (in.is_dir() && in.has_dir_layout())
    out << " has_dir_layout";

  if (in.quota.is_enable())
    out << " " << in.quota;

  out << ' ' << &in << ")";
  return out;
}


void Inode::make_long_path(filepath& p)
{
  if (!dentries.empty()) {
    Dentry *dn = get_first_parent();
    ceph_assert(dn->dir && dn->dir->parent_inode);
    dn->dir->parent_inode->make_long_path(p);
    p.push_dentry(dn->name);
  } else if (snapdir_parent) {
    snapdir_parent->make_nosnap_relative_path(p);
    string empty;
    p.push_dentry(empty);
  } else
    p = filepath(ino);
}

/*
 * make a filepath suitable for an mds request:
 *  - if we are non-snapped/live, the ino is sufficient, e.g. #1234
 *  - if we are snapped, make filepath relative to first non-snapped parent.
 */
void Inode::make_nosnap_relative_path(filepath& p)
{
  if (snapid == CEPH_NOSNAP) {
    p = filepath(ino);
  } else if (snapdir_parent) {
    snapdir_parent->make_nosnap_relative_path(p);
    string empty;
    p.push_dentry(empty);
  } else if (!dentries.empty()) {
    Dentry *dn = get_first_parent();
    ceph_assert(dn->dir && dn->dir->parent_inode);
    dn->dir->parent_inode->make_nosnap_relative_path(p);
    p.push_dentry(dn->name);
  } else {
    p = filepath(ino);
  }
}

void Inode::get_open_ref(int mode)
{
  if (snapid != CEPH_NOSNAP)
    return;

  int bits = (mode << 1) | 1;
  for (int i = 0; i < CEPH_FILE_MODE_BITS; i++) {
    if (bits & (1 << i))
	open_by_mode[i]++;
  }

  touch_open_ref(mode);

  break_deleg(!(mode & CEPH_FILE_MODE_WR));
}

bool Inode::put_open_ref(int mode)
{
  if (snapid != CEPH_NOSNAP)
    return false;

  bool last = false;
  int bits = (mode << 1) | 1;
  for (int i = 0; i < CEPH_FILE_MODE_BITS; i++) {
    if (bits & (1 << i)) {
      auto &ref = open_by_mode[i];
      ceph_assert(ref > 0);
      if (--ref == 0)
	last = true;
    }
  }
  return last;
}

void Inode::touch_open_ref(int mode)
{
  auto now = ceph_clock_now();
  if (mode & CEPH_FILE_MODE_RD)
    last_rd = now;
  if (mode & CEPH_FILE_MODE_WR)
    last_wr = now;

  if (snapid == CEPH_NOSNAP &&
      (!delay_cap_item.is_on_list() || hold_caps_until <= now))
    client->cap_delay_requeue(this);
}


void Inode::inc_cap_waiter(int cap)
{
  if (cap & CEPH_CAP_FILE_RD)
    open_by_mode[ffs(CEPH_FILE_MODE_RD)] += FMODE_WAIT_BIAS;
  if (cap & CEPH_CAP_FILE_WR)
    open_by_mode[ffs(CEPH_FILE_MODE_WR)] += FMODE_WAIT_BIAS;
}

void Inode::dec_cap_waiter(int cap)
{
  if (cap & CEPH_CAP_FILE_RD) {
    auto& ref = open_by_mode[ffs(CEPH_FILE_MODE_RD)];
    ceph_assert(ref >= FMODE_WAIT_BIAS);
    ref -= FMODE_WAIT_BIAS;
  }
  if (cap & CEPH_CAP_FILE_WR) {
    auto& ref = open_by_mode[ffs(CEPH_FILE_MODE_WR)];
    ceph_assert(ref >= FMODE_WAIT_BIAS);
    ref -= FMODE_WAIT_BIAS;
  }
}

void Inode::get_cap_ref(int cap)
{
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      cap_refs[c]++;
      //cout << "inode " << *this << " get " << cap_string(c) << " " << (cap_refs[c]-1) << " -> " << cap_refs[c] << std::endl;
    }
    cap >>= 1;
    n++;
  }
}

int Inode::put_cap_ref(int cap)
{
  int last = 0;
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      if (cap_refs[c] <= 0) {
	lderr(client->cct) << "put_cap_ref " << ccap_string(c) << " went negative on " << *this << dendl;
	ceph_assert(cap_refs[c] > 0);
      }
      if (--cap_refs[c] == 0)
        last |= c;
      //cout << "inode " << *this << " put " << cap_string(c) << " " << (cap_refs[c]+1) << " -> " << cap_refs[c] << std::endl;
    }
    cap >>= 1;
    n++;
  }
  return last;
}

bool Inode::is_any_caps()
{
  return !caps.empty() || snap_caps;
}

bool Inode::cap_is_valid(const Cap &cap) const
{
  /*cout << "cap_gen     " << cap->session-> cap_gen << std::endl
    << "session gen " << cap->gen << std::endl
    << "cap expire  " << cap->session->cap_ttl << std::endl
    << "cur time    " << ceph_clock_now(cct) << std::endl;*/
  if ((cap.session->cap_gen <= cap.gen)
      && (ceph_clock_now() < cap.session->cap_ttl)) {
    return true;
  }
  return false;
}

int Inode::caps_issued(int *implemented) const
{
  int c = snap_caps;
  int i = 0;
  for (const auto &pair : caps) {
    const Cap &cap = pair.second;
    if (cap_is_valid(cap)) {
      c |= cap.issued;
      i |= cap.implemented;
    }
  }
  // exclude caps issued by non-auth MDS, but are been revoking by
  // the auth MDS. The non-auth MDS should be revoking/exporting
  // these caps, but the message is delayed.
  if (auth_cap)
    c &= ~auth_cap->implemented | auth_cap->issued;

  if (implemented)
    *implemented = i;
  return c;
}

void Inode::try_touch_cap(mds_rank_t mds)
{
  auto it = caps.find(mds);
  if (it != caps.end()) {
    it->second.touch();
  }
}

/**
 * caps_issued_mask - check whether we have all of the caps in the mask
 * @mask: mask to check against
 * @allow_impl: whether the caller can also use caps that are implemented but not issued
 *
 * This is the bog standard "check whether we have the required caps" operation.
 * Typically, we only check against the capset that is currently "issued".
 * In other words, we ignore caps that have been revoked but not yet released.
 *
 * Some callers (particularly those doing attribute retrieval) can also make
 * use of the full set of "implemented" caps to satisfy requests from the
 * cache.
 *
 * Those callers should refrain from taking new references to implemented
 * caps!
 */
bool Inode::caps_issued_mask(unsigned mask, bool allow_impl)
{
  int c = snap_caps;
  int i = 0;

  if ((c & mask) == mask)
    return true;
  // prefer auth cap
  if (auth_cap &&
      cap_is_valid(*auth_cap) &&
      (auth_cap->issued & mask) == mask) {
    auth_cap->touch();
    return true;
  }
  // try any cap
  for (auto &pair : caps) {
    Cap &cap = pair.second;
    if (cap_is_valid(cap)) {
      if ((cap.issued & mask) == mask) {
        cap.touch();
	return true;
      }
      c |= cap.issued;
      i |= cap.implemented;
    }
  }

  if (allow_impl)
    c |= i;

  if ((c & mask) == mask) {
    // bah.. touch them all
    for (auto &pair : caps) {
      pair.second.touch();
    }
    return true;
  }
  return false;
}

int Inode::caps_used()
{
  int w = 0;
  for (map<int,int>::iterator p = cap_refs.begin();
       p != cap_refs.end();
       ++p)
    if (p->second)
      w |= p->first;
  return w;
}

int Inode::caps_file_wanted()
{
  const int PIN_SHIFT = ffs(CEPH_FILE_MODE_PIN);
  const int RD_SHIFT = ffs(CEPH_FILE_MODE_RD);
  const int WR_SHIFT = ffs(CEPH_FILE_MODE_WR);
  const int LAZY_SHIFT = ffs(CEPH_FILE_MODE_LAZY);
  auto now = ceph_clock_now();
  auto used_cutoff = now - client->get_caps_wanted_delay_max();
  auto idle_cutoff = now - client->get_caps_wanted_delay_min();

  if (is_dir()) {
    int want = 0;

    /* use used_cutoff here, to keep dir's wanted caps longer */
    if (open_by_mode[RD_SHIFT] > 0 || last_rd > used_cutoff)
      want |= CEPH_CAP_ANY_SHARED;

    if (open_by_mode[WR_SHIFT] > 0 || last_wr > used_cutoff) {
      want |= CEPH_CAP_ANY_SHARED | CEPH_CAP_FILE_EXCL;
      want |= CEPH_CAP_ANY_DIR_OPS;
    }

    if (want || open_by_mode[PIN_SHIFT] > 0)
      want |= CEPH_CAP_PIN;

    return want;
  } else {
    int bits = 0;

    if (open_by_mode[RD_SHIFT] > 0) {
      if (open_by_mode[RD_SHIFT] >= FMODE_WAIT_BIAS ||
	  last_rd > used_cutoff)
	bits |= 1 << RD_SHIFT;
    } else if (last_rd > idle_cutoff) {
      bits |= 1 << RD_SHIFT;
    }

    if (open_by_mode[WR_SHIFT] > 0) {
      if (open_by_mode[WR_SHIFT] >= FMODE_WAIT_BIAS ||
	  last_wr > used_cutoff)
	bits |= 1 << WR_SHIFT;
    } else if (last_wr > idle_cutoff) {
      bits |= 1 << WR_SHIFT;
    }

    /* check lazyio only when read/write is wanted */
    if ((bits & (CEPH_FILE_MODE_RDWR << 1)) &&
	open_by_mode[LAZY_SHIFT] > 0)
      bits |= 1 << LAZY_SHIFT;

    return bits ? ceph_caps_for_mode(bits >> 1) : 0;
  }
}

int Inode::caps_wanted()
{
  int want = caps_file_wanted();
  int used = caps_used();
  if (is_file() && (used & CEPH_CAP_FILE_BUFFER))
    want |= CEPH_CAP_FILE_EXCL;
  return want;
}

int Inode::caps_mds_wanted()
{
  int want = 0;
  for (const auto &pair : caps) {
    if (auth_cap == &pair.second)
      want |= pair.second.wanted;
    else
      want |= pair.second.wanted & ~CEPH_CAP_ANY_WR;
  }
  return want;
}

int Inode::caps_dirty()
{
  return dirty_caps | flushing_caps;
}

const UserPerm* Inode::get_best_perms()
{
  const UserPerm *perms = NULL;
  for (const auto &pair : caps) {
    const UserPerm& iperm = pair.second.latest_perms;
    if (!perms) { // we don't have any, take what's present
      perms = &iperm;
    } else if (iperm.uid() == uid) {
      if (iperm.gid() == gid) { // we have the best possible, return
	return &iperm;
      }
      if (perms->uid() != uid) { // take uid > gid every time
	perms = &iperm;
      }
    } else if (perms->uid() != uid && iperm.gid() == gid) {
      perms = &iperm; // a matching gid is better than nothing
    }
  }
  return perms;
}

bool Inode::have_valid_size()
{
  // RD+RDCACHE or WR+WRBUFFER => valid size
  if (caps_issued() & (CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL))
    return true;
  return false;
}

// open Dir for an inode.  if it's not open, allocated it (and pin dentry in memory).
Dir *Inode::open_dir()
{
  if (!dir) {
    dir = new Dir(this);
    lsubdout(client->cct, client, 15) << "open_dir " << dir << " on " << this << dendl;
    ceph_assert(dentries.size() < 2); // dirs can't be hard-linked
    if (!dentries.empty())
      get_first_parent()->get();      // pin dentry
    get();                  // pin inode
  }
  return dir;
}

bool Inode::check_mode(const UserPerm& perms, unsigned want)
{
  if (uid == perms.uid()) {
    // if uid is owner, owner entry determines access
    want = want << 6;
  } else if (perms.gid_in_groups(gid)) {
    // if a gid or sgid matches the owning group, group entry determines access
    want = want << 3;
  }

  return (mode & want) == want;
}

void Inode::get() {
  _ref++;
  lsubdout(client->cct, client, 15) << "inode.get on " << this << " " <<  ino << '.' << snapid
				    << " now " << _ref << dendl;
}

//private method to put a reference; see Client::put_inode()
int Inode::_put(int n) {
  _ref -= n;
  lsubdout(client->cct, client, 15) << "inode.put on " << this << " " << ino << '.' << snapid
				    << " now " << _ref << dendl;
  ceph_assert(_ref >= 0);
  return _ref;
}


void Inode::dump(Formatter *f) const
{
  f->dump_stream("ino") << ino;
  f->dump_stream("snapid") << snapid;
  if (rdev)
    f->dump_unsigned("rdev", rdev);
  f->dump_stream("ctime") << ctime;
  f->dump_stream("btime") << btime;
  f->dump_stream("mode") << '0' << std::oct << mode << std::dec;
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  f->dump_int("nlink", nlink);

  f->dump_unsigned("size", size);
  f->dump_unsigned("max_size", max_size);
  f->dump_unsigned("truncate_seq", truncate_seq);
  f->dump_unsigned("truncate_size", truncate_size);
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_unsigned("time_warp_seq", time_warp_seq);
  f->dump_unsigned("change_attr", change_attr);

  f->dump_object("layout", layout);
  if (is_dir()) {
    f->open_object_section("dir_layout");
    ::dump(dir_layout, f);
    f->close_section();

    f->dump_bool("complete", flags & I_COMPLETE);
    f->dump_bool("ordered", flags & I_DIR_ORDERED);

    /* FIXME when wip-mds-encoding is merged ***
    f->open_object_section("dir_stat");
    dirstat.dump(f);
    f->close_section();

    f->open_object_section("rstat");
    rstat.dump(f);
    f->close_section();
    */
  }

  f->dump_unsigned("version", version);
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_unsigned("flags", flags);

  if (is_dir()) {
    f->dump_int("dir_hashed", (int)dir_hashed);
    f->dump_int("dir_replicated", (int)dir_replicated);
  }

  f->open_array_section("caps");
  for (const auto &pair : caps) {
    f->open_object_section("cap");
    f->dump_int("mds", pair.first);
    if (&pair.second == auth_cap)
      f->dump_int("auth", 1);
    pair.second.dump(f);
    f->close_section();
  }
  f->close_section();
  if (auth_cap)
    f->dump_int("auth_cap", auth_cap->session->mds_num);

  f->dump_stream("dirty_caps") << ccap_string(dirty_caps);
  if (flushing_caps) {
    f->dump_stream("flushings_caps") << ccap_string(flushing_caps);
    f->open_object_section("flushing_cap_tid");
    for (map<ceph_tid_t, int>::const_iterator p = flushing_cap_tids.begin();
	 p != flushing_cap_tids.end();
	 ++p) {
      string n(ccap_string(p->second));
      f->dump_unsigned(n.c_str(), p->first);
    }
    f->close_section();
  }
  f->dump_int("shared_gen", shared_gen);
  f->dump_int("cache_gen", cache_gen);
  if (snap_caps) {
    f->dump_int("snap_caps", snap_caps);
    f->dump_int("snap_cap_refs", snap_cap_refs);
  }

  f->dump_stream("hold_caps_until") << hold_caps_until;

  if (snaprealm) {
    f->open_object_section("snaprealm");
    snaprealm->dump(f);
    f->close_section();
  }
  if (!cap_snaps.empty()) {
    for (const auto &p : cap_snaps) {
      f->open_object_section("cap_snap");
      f->dump_stream("follows") << p.first;
      p.second.dump(f);
      f->close_section();
    }
  }

  // open
  if (!is_opened()) {
    f->open_array_section("open_by_mode");
    for (int i = 0; i < CEPH_FILE_MODE_BITS; i++) {
      if (!open_by_mode[i])
	continue;
      f->open_object_section("ref");
      f->dump_int("mode", (1 << i) >> 1);
      f->dump_int("refs", open_by_mode[i]);
      f->close_section();
    }
    f->close_section();
  }
  if (!cap_refs.empty()) {
    f->open_array_section("cap_refs");
    for (map<int,int>::const_iterator p = cap_refs.begin(); p != cap_refs.end(); ++p) {
      f->open_object_section("cap_ref");
      f->dump_stream("cap") << ccap_string(p->first);
      f->dump_int("refs", p->second);
      f->close_section();
    }
    f->close_section();
  }

  f->dump_unsigned("reported_size", reported_size);
  if (wanted_max_size != max_size)
    f->dump_unsigned("wanted_max_size", wanted_max_size);
  if (requested_max_size != max_size)
    f->dump_unsigned("requested_max_size", requested_max_size);

  f->dump_int("ref", _ref);
  f->dump_int("ll_ref", ll_ref);

  if (!dentries.empty()) {
    f->open_array_section("parents");
    for (const auto &&dn : dentries) {
      f->open_object_section("dentry");
      f->dump_stream("dir_ino") << dn->dir->parent_inode->ino;
      f->dump_string("name", dn->name);
      f->close_section();
    }
    f->close_section();
  }
}

void Cap::dump(Formatter *f) const
{
  f->dump_int("mds", session->mds_num);
  f->dump_stream("ino") << inode.ino;
  f->dump_unsigned("cap_id", cap_id);
  f->dump_stream("issued") << ccap_string(issued);
  if (implemented != issued)
    f->dump_stream("implemented") << ccap_string(implemented);
  f->dump_stream("wanted") << ccap_string(wanted);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("issue_seq", issue_seq);
  f->dump_unsigned("mseq", mseq);
  f->dump_unsigned("gen", gen);
}

void CapSnap::dump(Formatter *f) const
{
  f->dump_stream("ino") << in->ino;
  f->dump_stream("issued") << ccap_string(issued);
  f->dump_stream("dirty") << ccap_string(dirty);
  f->dump_unsigned("size", size);
  f->dump_stream("ctime") << ctime;
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_int("time_warp_seq", time_warp_seq);
  f->dump_stream("mode") << '0' << std::oct << mode << std::dec;
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  if (!xattrs.empty()) {
    f->open_object_section("xattr_lens");
    for (map<string,bufferptr>::const_iterator p = xattrs.begin(); p != xattrs.end(); ++p)
      f->dump_int(p->first.c_str(), p->second.length());
    f->close_section();
  }
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_int("writing", (int)writing);
  f->dump_int("dirty_data", (int)dirty_data);
  f->dump_unsigned("flush_tid", flush_tid);
}

void Inode::set_async_err(int r)
{
  for (const auto &fh : fhs) {
    fh->async_err = r;
  }
}

bool Inode::has_recalled_deleg()
{
  if (delegations.empty())
    return false;

  // Either all delegations are recalled or none are. Just check the first.
  Delegation& deleg = delegations.front();
  return deleg.is_recalled();
}

void Inode::recall_deleg(bool skip_read)
{
  if (delegations.empty())
    return;

  // Issue any recalls
  for (list<Delegation>::iterator d = delegations.begin();
       d != delegations.end(); ++d) {

    Delegation& deleg = *d;
    deleg.recall(skip_read);
  }
}

bool Inode::delegations_broken(bool skip_read)
{
  if (delegations.empty()) {
    lsubdout(client->cct, client, 10) <<
	  __func__ << ": delegations empty on " << *this << dendl;
    return true;
  }

  if (skip_read) {
    Delegation& deleg = delegations.front();
    lsubdout(client->cct, client, 10) <<
	__func__ << ": read delegs only on " << *this << dendl;
    if (deleg.get_type() == CEPH_FILE_MODE_RD) {
	return true;
    }
  }
  lsubdout(client->cct, client, 10) <<
	__func__ << ": not broken" << *this << dendl;
  return false;
}

void Inode::break_deleg(bool skip_read)
{
  lsubdout(client->cct, client, 10) <<
	  __func__ << ": breaking delegs on " << *this << dendl;

  recall_deleg(skip_read);

  while (!delegations_broken(skip_read))
    client->wait_on_list(waitfor_deleg);
}

/**
 * set_deleg: request a delegation on an open Fh
 * @fh: filehandle on which to acquire it
 * @type: delegation request type
 * @cb: delegation recall callback function
 * @priv: private pointer to be passed to callback
 *
 * Attempt to acquire a delegation on an open file handle. If there are no
 * conflicts and we have the right caps, allocate a new delegation, fill it
 * out and return 0. Return an error if we can't get one for any reason.
 */
int Inode::set_deleg(Fh *fh, unsigned type, ceph_deleg_cb_t cb, void *priv)
{
  lsubdout(client->cct, client, 10) <<
	  __func__ << ": inode " << *this << dendl;

  /*
   * 0 deleg timeout means that they haven't been explicitly enabled. Don't
   * allow it, with an unusual error to make it clear.
   */
  if (!client->get_deleg_timeout())
    return -ETIME;

  // Just say no if we have any recalled delegs still outstanding
  if (has_recalled_deleg()) {
    lsubdout(client->cct, client, 10) << __func__ <<
	  ": has_recalled_deleg" << dendl;
    return -EAGAIN;
  }

  // check vs. currently open files on this inode
  switch (type) {
  case CEPH_DELEGATION_RD:
    if (is_opened_for_write()) {
      lsubdout(client->cct, client, 10) << __func__ <<
	    ": open for write" << dendl;
      return -EAGAIN;
    }
    break;
  case CEPH_DELEGATION_WR:
    if (is_opened()) {
      lsubdout(client->cct, client, 10) << __func__ << ": open" << dendl;
      return -EAGAIN;
    }
    break;
  default:
    return -EINVAL;
  }

  /*
   * A delegation is essentially a long-held container for cap references that
   * we delegate to the client until recalled. The caps required depend on the
   * type of delegation (read vs. rw). This is entirely an opportunistic thing.
   * If we don't have the necessary caps for the delegation, then we just don't
   * grant one.
   *
   * In principle we could request the caps from the MDS, but a delegation is
   * usually requested just after an open. If we don't have the necessary caps
   * already, then it's likely that there is some sort of conflicting access.
   *
   * In the future, we may need to add a way to have this request caps more
   * aggressively -- for instance, to handle WANT_DELEGATION for NFSv4.1+.
   */
  int need = ceph_deleg_caps_for_type(type);
  if (!caps_issued_mask(need)) {
    lsubdout(client->cct, client, 10) << __func__ << ": cap mismatch, have="
      << ccap_string(caps_issued()) << " need=" << ccap_string(need) << dendl;
    return -EAGAIN;
  }

  for (list<Delegation>::iterator d = delegations.begin();
       d != delegations.end(); ++d) {
    Delegation& deleg = *d;
    if (deleg.get_fh() == fh) {
      deleg.reinit(type, cb, priv);
      return 0;
    }
  }

  delegations.emplace_back(fh, type, cb, priv);
  return 0;
}

/**
 * unset_deleg - remove a delegation that was previously set
 * @fh: file handle to clear delegation of
 *
 * Unlink delegation from the Inode (if there is one), put caps and free it.
 */
void Inode::unset_deleg(Fh *fh)
{
  for (list<Delegation>::iterator d = delegations.begin();
       d != delegations.end(); ++d) {
    Delegation& deleg = *d;
    if (deleg.get_fh() == fh) {
      delegations.erase(d);
      client->signal_cond_list(waitfor_deleg);
      break;
    }
  }
}

/**
* mark_caps_dirty - mark some caps dirty
* @caps: the dirty caps
*
* note that if there is no dirty and flushing caps before, we need to pin this inode.
* it will be unpined by handle_cap_flush_ack when there are no dirty and flushing caps.
*/
void Inode::mark_caps_dirty(int caps)
{
  lsubdout(client->cct, client, 10) << __func__ << " " << *this << " " << ccap_string(dirty_caps) << " -> "
           << ccap_string(dirty_caps | caps) << dendl;
  if (caps && !caps_dirty())
    get();
  dirty_caps |= caps;
  client->get_dirty_list().push_back(&dirty_cap_item);
}

/**
* mark_caps_clean - only clean the dirty_caps and caller should start flushing the dirty caps.
*/
void Inode::mark_caps_clean()
{
  lsubdout(client->cct, client, 10) << __func__ << " " << *this << dendl;
  dirty_caps = 0;
  dirty_cap_item.remove_myself();
}


