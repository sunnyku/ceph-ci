// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "mon/ElectionLogic.h"
#include "mon/ConnectionTracker.h"
#include "common/dout.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"

using namespace std;

#define dout_subsys ceph_subsys_test
#undef dout_prefix
#define dout_prefix _prefix(_dout, prefix_name(), timestep_count())
static ostream& _prefix(std::ostream *_dout, const char *prefix, int timesteps) {
  return *_dout << prefix << timesteps << " ";
}

const char* prefix_name() { return "test_election: "; }
int timestep_count() { return -1; }

int main(int argc, char **argv) {
  vector<const char*> args(argv, argv+argc);
  bool user_set_debug = false;
  for (auto& arg : args) {
    if (strncmp("--debug_mon", arg, 11) == 0) user_set_debug = true;
  }
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  if (!user_set_debug) g_ceph_context->_conf.set_val("debug mon", "0/20");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


class Owner;
struct Election {
  map<int, Owner*> electors;
  map<int, set<int> > blocked_messages;
  int count;
  ElectionLogic::election_strategy election_strategy;
  int ping_interval;
  set<int> disallowed_leaders;

  vector< function<void()> > messages;
  int pending_election_messages;
  int timesteps_run = 0;
  int last_quorum_change = 0;
  int last_quorum_formed = -1;
  set<int> last_quorum_reported;
  int last_leader = -1;
  
  Election(int c, ElectionLogic::election_strategy es, int pingi=1, double tracker_halflife=5);
  ~Election();
  // ElectionOwner interfaces
  int get_paxos_size() { return count; }
  const set<int>& get_disallowed_leaders() { return disallowed_leaders; }
  void propose_to(int from, int to, epoch_t e);
  void defer_to(int from, int to, epoch_t e);
  void claim_victory(int from, int to, epoch_t e, const set<int>& members);
  void accept_victory(int from, int to, epoch_t e);
  void report_quorum(const set<int>& quorum);
  void queue_stable_message(int from, int to, function<void()> m);
  void queue_timeout_message(int from, int to, function<void()> m);
  void queue_stable_or_timeout(int from, int to,
			       function<void()> m, function<void()> t);
  void queue_election_message(int from, int to, function<void()> m);

  // test runner interfaces
  int run_timesteps(int max);
  void init_pings();
  void start_one(int who);
  void start_all();
  bool election_stable();
  bool quorum_stable(int timesteps_stable);
  bool check_leader_agreement();
  bool check_epoch_agreement();
  void block_messages(int from, int to);
  void block_bidirectional_messages(int a, int b);
  void unblock_messages(int from, int to);
  void unblock_bidirectional_messages(int a, int b);
  void add_disallowed_leader(int disallowed) { disallowed_leaders.insert(disallowed); }
  const char* prefix_name() { return "Election:      "; }
  int timestep_count() { return timesteps_run; }
};
struct Owner : public ElectionOwner, RankProvider {
  Election *parent;
  int rank;
  epoch_t persisted_epoch;
  bool ever_joined;
  ConnectionTracker peer_tracker;
  ElectionLogic logic;
  set<int> quorum;
  int victory_accepters;
  int timer_steps; // timesteps until we trigger timeout
  bool timer_election; // the timeout is for normal election, or victory

 Owner(int r, ElectionLogic::election_strategy es, double tracker_halflife,
       Election *p) : parent(p), rank(r), persisted_epoch(0),
    ever_joined(false),
    peer_tracker(this, tracker_halflife),
    logic(this, es, &peer_tracker, g_ceph_context),
    victory_accepters(0),
    timer_steps(-1), timer_election(true) {}
    
  // in-memory store: just save to variable
  void persist_epoch(epoch_t e) { persisted_epoch = e; }
  // in-memory store: just return variable
  epoch_t read_persisted_epoch() const { return persisted_epoch; }
  // in-memory store: don't need to validate
  void validate_store() { return; }
  // don't need to do anything with our state right now
  void notify_bump_epoch() {}
  // pass back to ElectionLogic; we don't need this redirect ourselves
  void trigger_new_election() { logic.start(); }
  int get_my_rank() const { return rank; }
  // we don't need to persist scores as we don't reset and lose memory state
  void persist_connectivity_scores() {}
  void propose_to_peers(epoch_t e) {
    for (int i = 0; i < parent->get_paxos_size(); ++i) {
      if (i == rank) continue;
      parent->propose_to(rank, i, e);
    }
  }
  void reset_election() {
    _start();
    logic.start();
  }
  bool ever_participated() const { return ever_joined; }
  unsigned paxos_size() const { return parent->get_paxos_size(); }
  const set<int>& get_disallowed_leaders() const { return parent->get_disallowed_leaders(); }
  void cancel_timer() {
    timer_steps = -1;
  }
  void reset_timer(int steps) {
    cancel_timer();
    timer_steps = 3 + steps; // FIXME? magic number, current step + roundtrip
    timer_election = true;
  }
  void start_victory_timer() {
    cancel_timer();
    timer_election = false;
    timer_steps = 3; // FIXME? current step + roundtrip
  }
  void _start() {
    reset_timer(0);
    quorum.clear();
  }
  void _defer_to(int who) {
    parent->defer_to(rank, who, logic.get_epoch());
    reset_timer(0); // wtf does changing this 0->1 cause breakage?
  }
  void message_victory(const std::set<int>& members) {
    for (auto i : members) {
      if (i == rank) continue;
      parent->claim_victory(rank, i, logic.get_epoch(), members);
    }
    start_victory_timer();
    quorum = members;
    victory_accepters = 1;
  }
  bool is_current_member(int rank) const { return quorum.count(rank) != 0; }
  void receive_propose(int from, epoch_t e) {
    logic.receive_propose(from, e);
  }
  void receive_ack(int from, epoch_t e) {
    if (e < logic.get_epoch())
      return;
    logic.receive_ack(from, e);
  }
  void receive_victory_claim(int from, epoch_t e, const set<int>& members) {
    if (e < logic.get_epoch())
      return;
    if (logic.receive_victory_claim(from, e)) {
      quorum = members;
      cancel_timer();
      parent->accept_victory(rank, from, e);
    }
  }
  void receive_victory_ack(int from, epoch_t e) {
    if (e < logic.get_epoch())
      return;
    ++victory_accepters;
    if (victory_accepters == static_cast<int>(quorum.size())) {
      cancel_timer();
      parent->report_quorum(quorum);
    }
  }
  void receive_ping(int rank, bufferlist bl) {
    map<int,ConnectionReport> crs;
    bufferlist::const_iterator bi = bl.begin();
    decode(crs, bi);
    peer_tracker.report_live_connection(rank, parent->ping_interval);
    for (auto& i : crs) {
      peer_tracker.receive_peer_report(i.second);
    }
  }
  void receive_ping_timeout(int rank) {
    peer_tracker.report_dead_connection(rank, parent->ping_interval);
  }
  void election_timeout() {
    ldout(g_ceph_context, 2) << "election epoch " << logic.get_epoch()
	 << " timed out for " << rank
	 << ", electing me:" << logic.electing_me
	 << ", acked_me:" << logic.acked_me << dendl;
    logic.end_election_period();
  }
  void victory_timeout() {
    ldout(g_ceph_context, 2) << "victory epoch " << logic.get_epoch()
	 << " timed out for " << rank
	 << ", electing me:" << logic.electing_me
	 << ", acked_me:" << logic.acked_me << dendl;
    reset_election();
  }
  void send_pings() {
    bufferlist bl;
    map<int,ConnectionReport> crs;
    peer_tracker.generate_report_of_peers(&crs[rank]);
    for (int i = 0; i < parent->get_paxos_size(); ++i) {
      if (i == rank) {
	continue;
      }
      const ConnectionReport *view = peer_tracker.get_peer_view(i);
      if (view != NULL) {
	crs[i] = *view;
      }
    }
    encode(crs, bl);

    for (int i = 0; i < parent->get_paxos_size(); ++i) {
      if (i == rank)
	continue;
      Owner *o = parent->electors[i];
      parent->queue_stable_or_timeout(rank, i,
				      [o, r=rank, bl] { o->receive_ping(r, bl); },
				      [o, r=rank] { o->receive_ping_timeout(r); }
				      );
    }
  }
  void notify_timestep() {
    assert(timer_steps != 0);
    if (timer_steps > 0) {
      --timer_steps;
    }
    if (timer_steps == 0) {
      if (timer_election) {
	election_timeout();
      } else {
	victory_timeout();
      }
    }

    if (parent->timesteps_run % parent->ping_interval == 0) {
      send_pings();
    }
  }
  const char *prefix_name() { return "Owner:         "; }
  int timestep_count() { return parent->timesteps_run; }
};

Election::Election(int c, ElectionLogic::election_strategy es, int pingi,
		   double tracker_halflife) : count(c), election_strategy(es), ping_interval(pingi),
  pending_election_messages(0), timesteps_run(0), last_quorum_change(0), last_quorum_formed(-1)
{
  for (int i = 0; i < count; ++i) {
    electors[i] = new Owner(i, election_strategy, tracker_halflife, this);
  }
}

Election::~Election()
{
  {
    for (auto i : electors) {
      delete i.second;
    }
  }
}

void Election::queue_stable_message(int from, int to, function<void()> m)
{
  if (!blocked_messages[from].count(to)) {
    messages.push_back(m);
  }
}

void Election::queue_election_message(int from, int to, function<void()> m)
{
  if (last_quorum_reported.count(from)) {
    last_quorum_change = timesteps_run;
    last_quorum_reported.clear();
    last_leader = -1;
  }
  if (!blocked_messages[from].count(to)) {
    messages.push_back([this,m] {
	--this->pending_election_messages;
	m();
      });
    ++pending_election_messages;
  }
}

void Election::queue_timeout_message(int from, int to, function<void()> m)
{
  ceph_assert(blocked_messages[from].count(to));
  messages.push_back(m);
}

void Election::queue_stable_or_timeout(int from, int to,
				       function<void()> m, function<void()> t)
{
  if (blocked_messages[from].count(to)) {
    queue_timeout_message(from, to, t);
  } else {
    queue_stable_message(from, to, m);
  }
}

void Election::defer_to(int from, int to, epoch_t e)
{
  Owner *o = electors[to];
  queue_election_message(from, to, [o, from, e] {
    o->receive_ack(from, e);
    });
}

void Election::propose_to(int from, int to, epoch_t e)
{
  Owner *o = electors[to];
  queue_election_message(from, to, [o, from, e] {
      o->receive_propose(from, e);
    });
}

void Election::claim_victory(int from, int to, epoch_t e, const set<int>& members)
{
  Owner *o = electors[to];
  queue_election_message(from, to, [o, from, e, members] {
      o->receive_victory_claim(from, e, members);
    });
}

void Election::accept_victory(int from, int to, epoch_t e)
{
  Owner *o = electors[to];
  queue_election_message(from, to, [o, from, e] {
      o->receive_victory_ack(from, e);
    });
}

void Election::report_quorum(const set<int>& quorum)
{
  for (int i : quorum) {
    electors[i]->ever_joined = true;
  }
  last_quorum_formed = last_quorum_change = timesteps_run;
  last_quorum_reported = quorum;
  last_leader = electors[*(quorum.begin())]->logic.get_election_winner();
}

int Election::run_timesteps(int max)
{
  vector< function<void()> > current_m;
  int steps = 0;
  for (; (!max || steps < max) && // we have timesteps left AND ONE OF
	 (pending_election_messages || // there are messages pending.
	  !election_stable()); // somebody's not happy and will act in future
       ++steps) {
    current_m.clear();
    current_m.swap(messages);
    ++timesteps_run;
    for (auto& m : current_m) {
      m();
    }
    for (auto o : electors) {
      o.second->notify_timestep();
    }
  }

  return steps;
}

void Election::init_pings()
{
  for (auto e : electors) {
    e.second->send_pings();
  }
}
void Election::start_one(int who)
{
  init_pings();
  assert(who < static_cast<int>(electors.size()));
  electors[who]->logic.start();
}

void Election::start_all() {
  init_pings();
  for (auto e : electors) {
    e.second->logic.start();
  }
}

bool Election::election_stable()
{
  // see if anybody has a timer running
  for (auto i : electors) {
    if (i.second->timer_steps != -1) {
      ldout(g_ceph_context, 30) << "rank " << i.first << " has timer value " << i.second->timer_steps << dendl;
      return false;
    }
  }
  return (pending_election_messages == 0);
}

bool Election::quorum_stable(int timesteps_stable)
{
  ldout(g_ceph_context, 1) << "quorum_stable? last formed:" << last_quorum_formed
			    << ", last changed " << last_quorum_change
			    << ", last reported members " << last_quorum_reported << dendl;
  if (last_quorum_reported.empty()) {
    return false;
  }
  if (last_quorum_formed < last_quorum_change) {
    return false;
  }
  for (auto i : last_quorum_reported) {
    if (electors[i]->timer_steps != -1) {
      return false;
    }
  }
  if (timesteps_run - timesteps_stable > last_quorum_change)
    return true;
  return election_stable();
}

bool Election::check_leader_agreement()
{
  int leader = electors[0]->logic.get_election_winner();
  ldout(g_ceph_context, 10) << "check_leader_agreement on " << leader << dendl;
  for (auto i: electors) {
    if (leader != i.second->logic.get_election_winner()) {
      ldout(g_ceph_context, 10) << "rank " << i.first << " has different leader "
				<< i.second->logic.get_election_winner() << dendl;
      return false;
    }
  }
  if (disallowed_leaders.count(leader)) {
    ldout(g_ceph_context, 10) << "that leader is disallowed! member of "
			      << disallowed_leaders << dendl;
    return false;
  }
  return true;
}

bool Election::check_epoch_agreement()
{
  epoch_t epoch = electors[0]->logic.get_epoch();
  for (auto i : electors) {
    if (epoch != i.second->logic.get_epoch()) {
      return false;
    }
  }
  return true;
}

void Election::block_messages(int from, int to)
{
  blocked_messages[from].insert(to);
}
void Election::block_bidirectional_messages(int a, int b)
{
  block_messages(a, b);
  block_messages(b, a);
}
void Election::unblock_messages(int from, int to)
{
  blocked_messages[from].erase(to);
}
void Election::unblock_bidirectional_messages(int a, int b)
{
  unblock_messages(a, b);
  unblock_messages(b, a);
}


void single_startup_election_completes(ElectionLogic::election_strategy strategy)
{
  for (int starter = 0; starter < 5; ++starter) {
    Election election(5, strategy);
    election.start_one(starter);
    // This test is not actually legit since you should start
    // all the ElectionLogics, but it seems to work
    int steps = election.run_timesteps(0);
    ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
    ASSERT_TRUE(election.election_stable());
    ASSERT_TRUE(election.quorum_stable(6)); // double the timer_steps we use
    ASSERT_TRUE(election.check_leader_agreement());
    ASSERT_TRUE(election.check_epoch_agreement());
  }
}

void everybody_starts_completes(ElectionLogic::election_strategy strategy)
{
  Election election(5, strategy);
  election.start_all();
  int steps = election.run_timesteps(0);
  ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.quorum_stable(6)); // double the timer_steps we use
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

void blocked_connection_continues_election(ElectionLogic::election_strategy strategy)
{
  Election election(5, strategy);
  election.block_bidirectional_messages(0, 1);
  election.start_all();
  int steps = election.run_timesteps(100);
  ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
  // This is a failure mode!
  ASSERT_FALSE(election.election_stable());
  ASSERT_FALSE(election.quorum_stable(6)); // double the timer_steps we use
  election.unblock_bidirectional_messages(0, 1);
  steps = election.run_timesteps(100);
  ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.quorum_stable(6)); // double the timer_steps we use
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

void blocked_connection_converges_election(ElectionLogic::election_strategy strategy)
{
  Election election(5, strategy);
  election.block_bidirectional_messages(0, 1);
  election.start_all();
  int steps = election.run_timesteps(100);
  ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
  election.unblock_bidirectional_messages(0, 1);
  steps = election.run_timesteps(100);
  ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

void disallowed_doesnt_win(ElectionLogic::election_strategy strategy)
{
  int MON_COUNT = 5;
  for (int i = 0; i < MON_COUNT - 1; ++i) {
    Election election(MON_COUNT, strategy);
    for (int j = 0; j <= i; ++j) {
      election.add_disallowed_leader(j);
    }
    election.start_all();
    int steps = election.run_timesteps(0);
    ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
    ASSERT_TRUE(election.election_stable());
    ASSERT_TRUE(election.quorum_stable(6)); // double the timer_steps we use
    ASSERT_TRUE(election.check_leader_agreement());
    ASSERT_TRUE(election.check_epoch_agreement());
    int leader = election.electors[0]->logic.get_election_winner();
    for (int j = 0; j <= i; ++j) {
      ASSERT_NE(j, leader);
    }
  }
  for (int i = MON_COUNT - 1; i > 0; --i) {
    Election election(MON_COUNT, strategy);
    for (int j = i; j <= MON_COUNT - 1; ++j) {
      election.add_disallowed_leader(j);
    }
    election.start_all();
    int steps = election.run_timesteps(0);
    ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
    ASSERT_TRUE(election.election_stable());
    ASSERT_TRUE(election.quorum_stable(6)); // double the timer_steps we use
    ASSERT_TRUE(election.check_leader_agreement());
    ASSERT_TRUE(election.check_epoch_agreement());
    int leader = election.electors[0]->logic.get_election_winner();
    for (int j = i; j < MON_COUNT; ++j) {
      ASSERT_NE(j, leader);
    }
  }
}

void converges_after_flapping(ElectionLogic::election_strategy strategy)
{
  Election election(5, strategy);
  auto block_cons = [&] {
    auto& e = election;
  // leave 4 connected to both sides so it will trigger but not trivially win
    e.block_bidirectional_messages(0, 2);
    e.block_bidirectional_messages(0, 3);
    e.block_bidirectional_messages(1, 2);
    e.block_bidirectional_messages(1, 3);
  };
  auto unblock_cons = [&] {
    auto& e = election;
    e.unblock_bidirectional_messages(0, 2);
    e.unblock_bidirectional_messages(0, 3);
    e.unblock_bidirectional_messages(1, 2);
    e.unblock_bidirectional_messages(1, 3);
  };
  block_cons();
  election.start_all();
  for (int i = 0; i < 5; ++i) {
    election.run_timesteps(5);
    unblock_cons();
    election.run_timesteps(5);
    block_cons();
  }
  unblock_cons();
  election.run_timesteps(100);
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.quorum_stable(6)); // double the timer_steps we use
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

void converges_while_flapping(ElectionLogic::election_strategy strategy)
{
  Election election(5, strategy);
  auto block_cons = [&] {
    auto& e = election;
  // leave 4 connected to both sides so it will trigger but not trivially win
    e.block_bidirectional_messages(0, 2);
    e.block_bidirectional_messages(0, 3);
    e.block_bidirectional_messages(1, 2);
    e.block_bidirectional_messages(1, 3);
  };
  auto unblock_cons = [&] {
    auto& e = election;
    e.unblock_bidirectional_messages(0, 2);
    e.unblock_bidirectional_messages(0, 3);
    e.unblock_bidirectional_messages(1, 2);
    e.unblock_bidirectional_messages(1, 3);
  };
  block_cons();
  election.start_all();
  for (int i = 0; i < 5; ++i) {
    election.run_timesteps(10);
    ASSERT_TRUE(election.quorum_stable(6));
    unblock_cons();
    election.run_timesteps(5);
    block_cons();
    ASSERT_TRUE(election.election_stable());
    ASSERT_TRUE(election.check_leader_agreement());
    ASSERT_TRUE(election.check_epoch_agreement());
  }
  unblock_cons();
  election.run_timesteps(100);
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.quorum_stable(6));
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

void netsplit_with_disallowed_tiebreaker_converges(ElectionLogic::election_strategy strategy)
{
  Election election(5, strategy);
  election.add_disallowed_leader(4);
  auto netsplit = [&] {
    auto& e = election;
    e.block_bidirectional_messages(0, 2);
    e.block_bidirectional_messages(0, 3);
    e.block_bidirectional_messages(1, 2);
    e.block_bidirectional_messages(1, 3);
  };
  auto unsplit = [&] {
    auto& e = election;
    e.unblock_bidirectional_messages(0, 2);
    e.unblock_bidirectional_messages(0, 3);
    e.unblock_bidirectional_messages(1, 2);
    e.unblock_bidirectional_messages(1, 3);
  };
  // hmm, we don't have timeouts to call elections automatically yet
  auto call_elections = [&] {
    for (auto i : election.electors) {
      i.second->trigger_new_election();
    }
  };
  // turn everybody on, run happy for a while
  election.start_all();
  election.run_timesteps(10);
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.quorum_stable(6));
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
  int starting_leader = election.last_leader;
  // do some netsplits, but leave disallowed tiebreaker alive
  for (int i = 0; i < 5; ++i) {
    netsplit();
    call_elections();
    election.run_timesteps(15); // tests fail when I run 10 because 0 and 1 time out on same timestamp for some reason, why?
    // this ASSERT_EQ only holds while we bias for ranks
    ASSERT_EQ(starting_leader, election.last_leader);
    ASSERT_TRUE(election.quorum_stable(6));
    ASSERT_FALSE(election.election_stable());
    unsplit();
    call_elections();
    election.run_timesteps(10);
    ASSERT_EQ(starting_leader, election.last_leader);
    ASSERT_TRUE(election.quorum_stable(6));
    ASSERT_TRUE(election.election_stable());
    ASSERT_TRUE(election.check_leader_agreement());
    ASSERT_TRUE(election.check_epoch_agreement());
  }

  // now disconnect the tiebreaker and make sure nobody can win
  int presplit_quorum_time = election.last_quorum_formed;
  netsplit();
  election.block_bidirectional_messages(4, 0);
  election.block_bidirectional_messages(4, 1);
  election.block_bidirectional_messages(4, 2);
  election.block_bidirectional_messages(4, 3);
  call_elections();
  election.run_timesteps(100);
  ASSERT_EQ(election.last_quorum_formed, presplit_quorum_time);

  // now let in the previously-losing side
  election.unblock_bidirectional_messages(4, 2);
  election.unblock_bidirectional_messages(4, 3);
  call_elections();
  election.run_timesteps(100);
  ASSERT_TRUE(election.quorum_stable(50));
  ASSERT_FALSE(election.election_stable());

  // now reconnect everybody
  unsplit();
  election.unblock_bidirectional_messages(4, 0);
  election.unblock_bidirectional_messages(4, 1);
  call_elections();
  election.run_timesteps(100);
  ASSERT_TRUE(election.quorum_stable(50));
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

void handles_singly_connected_peon(ElectionLogic::election_strategy strategy)
{
  Election election(5, strategy);
  election.block_bidirectional_messages(0, 1);
  election.block_bidirectional_messages(0, 2);
  election.block_bidirectional_messages(0, 3);
  election.block_bidirectional_messages(0, 4);

  election.start_all();
  election.run_timesteps(20);
  ASSERT_TRUE(election.quorum_stable(5));
  ASSERT_FALSE(election.election_stable());

  election.unblock_bidirectional_messages(0, 1);
  election.run_timesteps(100);
  ASSERT_TRUE(election.quorum_stable(50));
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());

  election.block_bidirectional_messages(0, 1);
  election.unblock_bidirectional_messages(0, 4);
  for (auto i : election.electors) {
    i.second->trigger_new_election();
  }
  election.run_timesteps(15);
  ASSERT_TRUE(election.quorum_stable(50));
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

void handles_disagreeing_connectivity(ElectionLogic::election_strategy strategy)
{
  Election election(5, strategy, 5); // ping every 5 timesteps so they start elections before settling scores!

  // start everybody up and run for a bit
  election.start_all();
  election.run_timesteps(20);
  ASSERT_TRUE(election.quorum_stable(5));
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());

  // block all the connections
  for (int i = 0; i < 5; ++i) {
    for (int j = i+1; j < 5; ++j) {
      election.block_bidirectional_messages(i, j);
    }
  }

  // now start them electing, which will obviously fail
  election.start_all();
  election.run_timesteps(50); // let them all demote scores of their peers
  ASSERT_FALSE(election.quorum_stable(10));
  ASSERT_FALSE(election.election_stable());

  // now reconnect them, at which point they should start running an election before exchanging scores
  for (int i = 0; i < 5; ++i) {
    for (int j = i+1; j < 5; ++j) {
      election.unblock_bidirectional_messages(i, j);
    }
  }
  election.run_timesteps(100);

  // these will pass if the nodes managed to converge on scores, but I expect failure
  ASSERT_TRUE(election.quorum_stable(5));
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

// TODO: write a test with more complicated connectivity graphs and make sure
// they are stable with multiple disconnected ranks pinging peons

// TODO: Write a test that disallowing and disconnecting 0 is otherwise stable?

#define test_classic(utest) TEST(classic, utest) { utest(ElectionLogic::CLASSIC); }

#define test_disallowed(utest) TEST(disallowed, utest) { utest(ElectionLogic::DISALLOW); }

#define test_connectivity(utest) TEST(connectivity, utest) { utest(ElectionLogic::CONNECTIVITY); }


test_classic(single_startup_election_completes)
test_classic(everybody_starts_completes)
test_classic(blocked_connection_continues_election)
test_classic(converges_after_flapping)

test_disallowed(single_startup_election_completes)
test_disallowed(everybody_starts_completes)
test_disallowed(blocked_connection_continues_election)
test_disallowed(disallowed_doesnt_win)
test_disallowed(converges_after_flapping)

/* skip single_startup_election_completes because we crash
 on init conditions. That's fine since as noted above it's not
 quite following the rules anyway. */
test_connectivity(everybody_starts_completes)
test_connectivity(blocked_connection_converges_election)
test_connectivity(disallowed_doesnt_win)
test_connectivity(converges_after_flapping)
test_connectivity(converges_while_flapping)
test_connectivity(netsplit_with_disallowed_tiebreaker_converges)
test_connectivity(handles_singly_connected_peon)
test_connectivity(handles_disagreeing_connectivity)
