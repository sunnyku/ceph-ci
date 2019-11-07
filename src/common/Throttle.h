// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_THROTTLE_H
#define CEPH_THROTTLE_H

#include <map>
#include <list>
#include <chrono>
#include <atomic>
#include <iostream>
#include <condition_variable>
#include <stdexcept>

#include "Cond.h"
#include "include/Context.h"
#include "common/Timer.h"

class CephContext;
class PerfCounters;

/**
 * @class Throttle
 * Throttles the maximum number of active requests.
 *
 * This class defines the maximum number of slots currently taken away. The
 * excessive requests for more of them are delayed, until some slots are put
 * back, so @p get_current() drops below the limit after fulfills the requests.
 */
class Throttle {
  CephContext *cct;
  const std::string name;
  PerfCounters *logger;
  std::atomic<int64_t> count = { 0 }, max = { 0 };
  Mutex lock;
  list<Cond*> cond;
  const bool use_perf;
  bool shutting_down = false;
  Cond shutdown_clear;

public:
  Throttle(CephContext *cct, const std::string& n, int64_t m = 0, bool _use_perf = true);
  ~Throttle();

private:
  void _reset_max(int64_t m);
  bool _should_wait(int64_t c) const {
    int64_t m = max;
    int64_t cur = count;
    return
      m &&
      ((c <= m && cur + c > m) || // normally stay under max
       (c >= m && cur > m));     // except for large c
  }

  bool _wait(int64_t c);

public:
  /**
   * gets the number of currently taken slots
   * @returns the number of taken slots
   */
  int64_t get_current() const {
    return count;
  }

  /**
   * get the max number of slots
   * @returns the max number of slots
   */
  int64_t get_max() const { return max; }

  /**
   * return true if past midpoint
   */
  bool past_midpoint() const {
    return count >= max / 2;
  }

  /**
   * set the new max number, and wait until the number of taken slots drains
   * and drops below this limit.
   *
   * @param m the new max number
   * @returns true if this method is blocked, false it it returns immediately
   */
  bool wait(int64_t m = 0);

  /**
   * take the specified number of slots from the stock regardless the throttling
   * @param c number of slots to take
   * @returns the total number of taken slots
   */
  int64_t take(int64_t c = 1);

  /**
   * get the specified amount of slots from the stock, but will wait if the
   * total number taken by consumer would exceed the maximum number.
   * @param c number of slots to get
   * @param m new maximum number to set, ignored if it is 0
   * @returns true if this request is blocked due to the throttling, false 
   * otherwise
   */
  bool get(int64_t c = 1, int64_t m = 0);

  /**
   * the unblocked version of @p get()
   * @returns true if it successfully got the requested amount,
   * or false if it would block.
   */
  bool get_or_fail(int64_t c = 1);

  /**
   * put slots back to the stock
   * @param c number of slots to return
   * @returns number of requests being hold after this
   */
  int64_t put(int64_t c = 1);
   /**
   * reset the zero to the stock
   */
  void reset();

  bool should_wait(int64_t c) const {
    return _should_wait(c);
  }
  void reset_max(int64_t m) {
    Mutex::Locker l(lock);
    _reset_max(m);
  }
};

/**
 * BackoffThrottle
 *
 * Creates a throttle which gradually induces delays when get() is called
 * based on params low_threshhold, high_threshhold, expected_throughput,
 * high_multiple, and max_multiple.
 *
 * In [0, low_threshhold), we want no delay.
 *
 * In [low_threshhold, high_threshhold), delays should be injected based
 * on a line from 0 at low_threshhold to
 * high_multiple * (1/expected_throughput) at high_threshhold.
 *
 * In [high_threshhold, 1), we want delays injected based on a line from
 * (high_multiple * (1/expected_throughput)) at high_threshhold to
 * (high_multiple * (1/expected_throughput)) +
 * (max_multiple * (1/expected_throughput)) at 1.
 *
 * Let the current throttle ratio (current/max) be r, low_threshhold be l,
 * high_threshhold be h, high_delay (high_multiple / expected_throughput) be e,
 * and max_delay (max_muliple / expected_throughput) be m.
 *
 * delay = 0, r \in [0, l)
 * delay = (r - l) * (e / (h - l)), r \in [l, h)
 * delay = e + (r - h)((m - e)/(1 - h))
 */
class BackoffThrottle {
  CephContext *cct;
  const std::string name;
  PerfCounters *logger;

  std::mutex lock;
  using locker = std::unique_lock<std::mutex>;

  unsigned next_cond = 0;

  /// allocated once to avoid constantly allocating new ones
  vector<std::condition_variable> conds;

  const bool use_perf;

  /// pointers into conds
  list<std::condition_variable*> waiters;

  std::list<std::condition_variable*>::iterator _push_waiter() {
    unsigned next = next_cond++;
    if (next_cond == conds.size())
      next_cond = 0;
    return waiters.insert(waiters.end(), &(conds[next]));
  }

  void _kick_waiters() {
    if (!waiters.empty())
      waiters.front()->notify_all();
  }

  /// see above, values are in [0, 1].
  double low_threshhold = 0;
  double high_threshhold = 1;

  /// see above, values are in seconds
  double high_delay_per_count = 0;
  double max_delay_per_count = 0;

  /// Filled in in set_params
  double s0 = 0; ///< e / (h - l), l != h, 0 otherwise
  double s1 = 0; ///< (m - e)/(1 - h), 1 != h, 0 otherwise

  /// max
  uint64_t max = 0;
  uint64_t current = 0;

  std::chrono::duration<double> _get_delay(uint64_t c) const;

public:
  /**
   * set_params
   *
   * Sets params.  If the params are invalid, returns false
   * and populates errstream (if non-null) with a user compreshensible
   * explanation.
   */
  bool set_params(
    double low_threshhold,
    double high_threshhold,
    double expected_throughput,
    double high_multiple,
    double max_multiple,
    uint64_t throttle_max,
    ostream *errstream);

  std::chrono::duration<double> get(uint64_t c = 1);
  std::chrono::duration<double> wait() {
    return get(0);
  }
  uint64_t put(uint64_t c = 1);
  uint64_t take(uint64_t c = 1);
  uint64_t get_current();
  uint64_t get_max();

  BackoffThrottle(CephContext *cct, const std::string& n,
    unsigned expected_concurrency, ///< [in] determines size of conds
    bool _use_perf = true);
  ~BackoffThrottle();
};


/**
 * @class SimpleThrottle
 * This is a simple way to bound the number of concurrent operations.
 *
 * It tracks the first error encountered, and makes it available
 * when all requests are complete. wait_for_ret() should be called
 * before the instance is destroyed.
 *
 * Re-using the same instance isn't safe if you want to check each set
 * of operations for errors, since the return value is not reset.
 */
class SimpleThrottle {
public:
  SimpleThrottle(uint64_t max, bool ignore_enoent);
  ~SimpleThrottle();
  void start_op();
  void end_op(int r);
  bool pending_error() const;
  int wait_for_ret();
private:
  mutable Mutex m_lock;
  Cond m_cond;
  uint64_t m_max;
  uint64_t m_current;
  int m_ret;
  bool m_ignore_enoent;
  uint32_t waiters = 0;
};


class OrderedThrottle;

class C_OrderedThrottle : public Context {
public:
  C_OrderedThrottle(OrderedThrottle *ordered_throttle, uint64_t tid)
    : m_ordered_throttle(ordered_throttle), m_tid(tid) {
  }

protected:
  void finish(int r) override;

private:
  OrderedThrottle *m_ordered_throttle;
  uint64_t m_tid;
};

/**
 * @class OrderedThrottle
 * Throttles the maximum number of active requests and completes them in order
 *
 * Operations can complete out-of-order but their associated Context callback
 * will completed in-order during invokation of start_op() and wait_for_ret()
 */
class OrderedThrottle {
public:
  OrderedThrottle(uint64_t max, bool ignore_enoent);
  ~OrderedThrottle();

  C_OrderedThrottle *start_op(Context *on_finish);
  void end_op(int r);

  bool pending_error() const;
  int wait_for_ret();

protected:
  friend class C_OrderedThrottle;

  void finish_op(uint64_t tid, int r);

private:
  struct Result {
    bool finished;
    int ret_val;
    Context *on_finish;

    Result(Context *_on_finish = NULL)
      : finished(false), ret_val(0), on_finish(_on_finish) {
    }
  };

  typedef std::map<uint64_t, Result> TidResult;

  mutable Mutex m_lock;
  Cond m_cond;
  uint64_t m_max;
  uint64_t m_current;
  int m_ret_val;
  bool m_ignore_enoent;

  uint64_t m_next_tid;
  uint64_t m_complete_tid;

  TidResult m_tid_result;

  void complete_pending_ops();
  uint32_t waiters = 0;
};


class TokenBucketThrottle {
  struct Bucket {
    CephContext *cct;
    const std::string name;

    uint64_t remain;
    uint64_t max;

    Bucket(CephContext *cct, const std::string &name, uint64_t m)
      : cct(cct), name(name), remain(m), max(m) {}

    uint64_t get(uint64_t c);
    uint64_t put(uint64_t c);
    void set_max(uint64_t m);
  };

  struct Blocker {
    uint64_t tokens_requested;
    Context *ctx;

    Blocker(uint64_t _tokens_requested, Context* _ctx)
      : tokens_requested(_tokens_requested), ctx(_ctx) {}
  };

  CephContext *m_cct;
  const std::string m_name;
  Bucket m_throttle;
  uint64_t m_avg = 0;
  uint64_t m_burst = 0;
  SafeTimer *m_timer;
  Mutex *m_timer_lock;
  FunctionContext *m_token_ctx = nullptr;
  list<Blocker> m_blockers;
  Mutex m_lock;

  // minimum of the filling period.
  uint64_t m_tick_min = 50;
  // tokens filling period, its unit is millisecond.
  uint64_t m_tick = 0;
  /**
   * These variables are used to calculate how many tokens need to be put into
   * the bucket within each tick.
   *
   * In actual use, the tokens to be put per tick(m_avg / m_ticks_per_second)
   * may be a floating point number, but we need an 'uint64_t' to put into the
   * bucket.
   *
   * For example, we set the value of rate to be 950, means 950 iops(or bps).
   *
   * In this case, the filling period(m_tick) should be 1000 / 950 = 1.052,
   * which is too small for the SafeTimer. So we should set the period(m_tick)
   * to be 50(m_tick_min), and 20 ticks in one second(m_ticks_per_second).
   * The tokens filled in bucket per tick is 950 / 20 = 47.5, not an integer.
   *
   * To resolve this, we use a method called tokens_filled(m_current_tick) to
   * calculate how many tokens will be put so far(until m_current_tick):
   *
   *   tokens_filled = m_current_tick / m_ticks_per_second * m_avg
   *
   * And the difference between two ticks will be the result we expect.
   *   tokens in tick 0: (1 / 20 * 950) - (0 / 20 * 950) =  47 -   0 = 47
   *   tokens in tick 1: (2 / 20 * 950) - (1 / 20 * 950) =  95 -  47 = 48
   *   tokens in tick 2: (3 / 20 * 950) - (2 / 20 * 950) = 142 -  95 = 47
   *
   * As a result, the tokens filled in one second will shown as this:
   *   tick    | 1| 2| 3| 4| 5| 6| 7| 8| 9|10|11|12|13|14|15|16|17|18|19|20|
   *   tokens  |47|48|47|48|47|48|47|48|47|48|47|48|47|48|47|48|47|48|47|48|
   */
  uint64_t m_ticks_per_second = 0;
  uint64_t m_current_tick = 0;

  // period for the bucket filling tokens, its unit is seconds.
  double m_schedule_tick = 1.0;

public:
  TokenBucketThrottle(CephContext *cct, const std::string &name,
                      uint64_t capacity, uint64_t avg,
                      SafeTimer *timer, Mutex *timer_lock);

  ~TokenBucketThrottle();

  const std::string &get_name() {
    return m_name;
  }

  template <typename T, typename I, void(T::*MF)(int, I*, uint64_t)>
  void add_blocker(uint64_t c, T *handler, I *item, uint64_t flag) {
    Context *ctx = new FunctionContext([handler, item, flag](int r) {
      (handler->*MF)(r, item, flag);
      });
    m_blockers.emplace_back(c, ctx);
  }

  template <typename T, typename I, void(T::*MF)(int, I*, uint64_t)>
  bool get(uint64_t c, T *handler, I *item, uint64_t flag) {
    bool wait = false;
    uint64_t got = 0;
    Mutex::Locker lock(m_lock);
    if (!m_blockers.empty()) {
      // Keep the order of requests, add item after previous blocked requests.
      wait = true;
    } else {
      if (0 == m_throttle.max || 0 == m_avg)
        return false;

      got = m_throttle.get(c);
      if (got < c) {
        // Not enough tokens, add a blocker for it.
        wait = true;
      }
    }

    if (wait)
      add_blocker<T, I, MF>(c - got, handler, item, flag);

    return wait;
  }

  int set_limit(uint64_t average, uint64_t burst);
  void set_schedule_tick_min(uint64_t tick);

private:
  uint64_t tokens_filled(double tick);
  uint64_t tokens_this_tick();
  void add_tokens();
  void schedule_timer();
  void cancel_timer();
};

#endif
