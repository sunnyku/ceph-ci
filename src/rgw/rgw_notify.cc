// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "rgw_notify.h"
#include "cls/2pc_queue/cls_2pc_queue_client.h"
#include "cls/lock/cls_lock_client.h"
#include <memory>
#include <boost/algorithm/hex.hpp>
#include <spawn/spawn.hpp>
#include "rgw_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_perf_counters.h"
#include "common/dout.h"
#include <chrono>

#define dout_subsys ceph_subsys_rgw

namespace rgw::notify {

struct record_with_endpoint_t {
  rgw_pubsub_s3_record record;
  std::string push_endpoint;
  std::string push_endpoint_args;
  std::string arn_topic;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(record, bl);
    encode(push_endpoint, bl);
    encode(push_endpoint_args, bl);
    encode(arn_topic, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(record, bl);
    decode(push_endpoint, bl);
    decode(push_endpoint_args, bl);
    decode(arn_topic, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(record_with_endpoint_t)

using queues_t = std::set<std::string>;

class Manager {
  const size_t max_queue_size;
  const uint32_t queues_update_period_ms;
  const uint32_t queues_update_retry_ms;
  const uint32_t queue_idle_sleep_us;
  const utime_t failover_time;
  CephContext* const cct;
  librados::IoCtx& rados_ioctx;
  static constexpr auto COOKIE_LEN = 16;
  const std::string lock_cookie;
  boost::asio::io_context io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard;
  const uint32_t worker_count;
  std::vector<std::thread> workers;
  const uint32_t stale_reservations_period_s;
  const uint32_t reservations_cleanup_period_s;
 
  const std::string Q_LIST_OBJECT_NAME = "queues_list_object";

  // read the list of queues from the queue list object
  int read_queue_list(queues_t& queues, optional_yield y) {
    constexpr auto max_chunk = 1024U;
    std::string start_after;
    bool more = true;
    int rval;
    while (more) {
      librados::ObjectReadOperation op;
      queues_t queues_chunk;
      op.omap_get_keys2(start_after, max_chunk, &queues_chunk, &more, &rval);
      const auto ret = rgw_rados_operate(rados_ioctx, Q_LIST_OBJECT_NAME, &op, nullptr, y);
      if (ret == -ENOENT) {
        // queue list object was not created - nothing to do
        return 0;
      }
      if (ret < 0) {
        // TODO: do we need to check on rval as well as ret?
        ldout(cct, 1) << "ERROR: failed to read queue list. error: " << ret << dendl;
        return ret;
      }
      queues.merge(queues_chunk);
    }
    return 0;
  }

  // set m1 to be the minimum between m1 and m2
  static int set_min_marker(std::string& m1, const std::string m2) {
    cls_queue_marker mr1;
    cls_queue_marker mr2;
    if (mr1.from_str(m1.c_str()) < 0 || mr2.from_str(m2.c_str()) < 0) {
      return -EINVAL;
    }
    if (mr2.gen <= mr1.gen && mr2.offset < mr1.offset) {
      m1 = m2;
    }
    return 0;
  }

  using Clock = ceph::coarse_mono_clock;
  using Executor = boost::asio::io_context::executor_type;
  using Timer = boost::asio::basic_waitable_timer<Clock,
        boost::asio::wait_traits<Clock>, Executor>;

  class tokens_waiter {
    const std::chrono::hours infinite_duration;
    size_t pending_tokens;
    Timer timer;
 
    struct token {
      tokens_waiter& waiter;
      token(tokens_waiter& _waiter) : waiter(_waiter) {
        ++waiter.pending_tokens;
      }
      
      ~token() {
        --waiter.pending_tokens;
        if (waiter.pending_tokens == 0) {
          waiter.timer.cancel();
        }   
      }   
    };
  
  public:

    tokens_waiter(boost::asio::io_context& io_context) :
      infinite_duration(1000),
      pending_tokens(0),
      timer(io_context) {}  
 
    void async_wait(spawn::yield_context yield) { 
      timer.expires_from_now(infinite_duration);
      boost::system::error_code ec; 
      timer.async_wait(yield[ec]);
      ceph_assert(ec == boost::system::errc::operation_canceled);
    }   
 
    token make_token() {    
      return token(*this);
    }   
  };

  // processing of a specific entry
  // return whether processing was successfull (true) or not (false)
  bool process_entry(const cls_queue_entry& entry, spawn::yield_context yield) {
    record_with_endpoint_t record_with_endpoint;
    auto iter = entry.data.cbegin();
    try {
      decode(record_with_endpoint, iter);
    } catch (buffer::error& err) {
      ldout(cct, 5) << "WARNING: failed to decode entry. error: " << err.what() << dendl;
      return false;
    }
    try {
      // TODO move endpoint creation to queue level
      const auto push_endpoint = RGWPubSubEndpoint::create(record_with_endpoint.push_endpoint, record_with_endpoint.arn_topic,
          RGWHTTPArgs(record_with_endpoint.push_endpoint_args), 
          cct);
      ldout(cct, 20) << "INFO: push endpoint created: " << record_with_endpoint.push_endpoint <<
        " for entry: " << entry.marker << dendl;
      const auto ret = push_endpoint->send_to_completion_async(cct, record_with_endpoint.record, optional_yield(io_context, yield));
      if (ret < 0) {
        ldout(cct, 5) << "WARNING: push entry: " << entry.marker << " to endpoint: " << record_with_endpoint.push_endpoint 
          << " failed. error: " << ret << " (will retry)" << dendl;
        return false;
      } else {
        ldout(cct, 20) << "INFO: push entry: " << entry.marker << " to endpoint: " << record_with_endpoint.push_endpoint 
          << " ok" <<  dendl;
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
        return true;
      }
    } catch (const RGWPubSubEndpoint::configuration_error& e) {
      ldout(cct, 5) << "WARNING: failed to create push endpoint: " 
          << record_with_endpoint.push_endpoint << " for entry: " << entry.marker << ". error: " << e.what() << " (will retry) " << dendl;
      return false;
    }
  }

  // clean stale reservation from queue
  void cleanup_queue(const std::string& queue_name, const bool& owned, spawn::yield_context yield) {
    while (owned) {
      ldout(cct, 20) << "INFO: trying to perform stale reservation cleanup for queue: " << queue_name << dendl;
      const auto now = ceph::coarse_real_time::clock::now();
      const auto stale_time = now - std::chrono::seconds(stale_reservations_period_s);
      librados::ObjectWriteOperation op;
      op.assert_exists();
      cls_2pc_queue_expire_reservations(op, stale_time);
      auto ret = rgw_rados_operate(rados_ioctx, queue_name, &op, optional_yield(io_context, yield));
      if (ret == -ENOENT) {
        // queue was deleted
        ldout(cct, 5) << "INFO: queue: " 
          << queue_name << ". was removed. cleanup will stop" << dendl;
        return;
      }
      if (ret < 0) {
        ldout(cct, 5) << "WARNING: failed to cleanup stale reservation from queue: " << queue_name
          << ". error: " << ret << dendl;
      }
      Timer timer(io_context);
      timer.expires_from_now(std::chrono::seconds(reservations_cleanup_period_s));
      boost::system::error_code ec;
	    timer.async_wait(yield[ec]);
    }
  }

  // processing of a specific queue
  void process_queue(const std::string& queue_name, const bool& owned, spawn::yield_context yield) {
    constexpr auto max_elements = 1024;
    auto is_idle = false;
    const std::string start_marker;

    // start a the cleanup coroutine for the queue
    spawn::spawn(io_context, [this, &owned, queue_name](spawn::yield_context yield) {
            cleanup_queue(queue_name, owned, yield);
            });
    
    while (owned) {
      // if queue was empty the last time, sleep for idle timeout
      if (is_idle) {
        Timer timer(io_context);
        timer.expires_from_now(std::chrono::microseconds(queue_idle_sleep_us));
        boost::system::error_code ec;
	      timer.async_wait(yield[ec]);
      }

      // get list of entries in the queue
      is_idle = true;
      bool truncated = false;
      std::string end_marker;
      std::vector<cls_queue_entry> entries;
      auto total_entries = 0U;
      {
        librados::ObjectReadOperation op;
        bufferlist obl;
        int rval;
        cls_2pc_queue_list_entries(op, start_marker, max_elements, &obl, &rval);
        auto ret = rgw_rados_operate(rados_ioctx, queue_name, &op, nullptr, optional_yield(io_context, yield));
        if (ret == -ENOENT) {
          // queue was deleted
          ldout(cct, 5) << "INFO: queue: " 
            << queue_name << ". was removed. processing will stop" << dendl;
          return;
        }
        if (ret < 0) {
          ldout(cct, 5) << "WARNING: failed to get list of entries in queue: " 
            << queue_name << ". error: " << ret << " (will retry)" << dendl;
          continue;
        }
        ret = cls_2pc_queue_list_entries_result(obl, entries, &truncated, end_marker);
        if (ret < 0) {
          ldout(cct, 5) << "WARNING: failed to parse list of entries in queue: " 
            << queue_name << ". error: " << ret << " (will retry)" << dendl;
          continue;
        }
      }
      total_entries = entries.size();
      if (total_entries == 0) {
        // nothing in the queue
        continue;
      }
      // log when queue is not idle
      ldout(cct, 20) << "INFO: found: " << total_entries << " entries in: " << queue_name <<
        ". end marker is: " << end_marker << dendl;
      
      is_idle = false;
      auto has_error = false;
      auto remove_entries = false;
      auto entry_idx = 1U;
      tokens_waiter waiter(io_context);
      for (auto& entry : entries) {
        if (has_error) {
          // bail out on first error
          break;
        }
        // TODO pass entry pointer instead of by-value
        spawn::spawn(yield, [this, &queue_name, entry_idx, total_entries, &end_marker, &remove_entries, &has_error, &waiter, entry](spawn::yield_context yield) {
            const auto token = waiter.make_token();
            if (process_entry(entry, yield)) {
              ldout(cct, 20) << "INFO: processing of entry: " << 
                entry.marker << " (" << entry_idx << "/" << total_entries << ") from: " << queue_name << " ok" << dendl;
              remove_entries = true;
            }  else {
              if (set_min_marker(end_marker, entry.marker) < 0) {
                ldout(cct, 1) << "ERROR: cannot determin minimum between malformed markers: " << end_marker << ", " << entry.marker << dendl;
              } else {
                ldout(cct, 20) << "INFO: new end marker for removal: " << end_marker << " from: " << queue_name << dendl;
              }
              has_error = true;
              ldout(cct, 20) << "INFO: processing of entry: " << 
                entry.marker << " (" << entry_idx << "/" << total_entries << ") from: " << queue_name << " failed" << dendl;
            } 
        });
        ++entry_idx;
      }

      // wait for all pending work to finish
      waiter.async_wait(yield);

      // delete all published entries from queue
      if (remove_entries) {
        librados::ObjectWriteOperation delete_op;
        cls_2pc_queue_remove_entries(delete_op, end_marker); 
        const auto ret = rgw_rados_operate(rados_ioctx, queue_name, &delete_op, optional_yield(io_context, yield)); 
        if (ret < 0) {
          ldout(cct, 1) << "ERROR: failed to remove entries up to: " << end_marker <<  " from queue: " 
            << queue_name << ". error: " << ret << dendl;
        } else {
          ldout(cct, 20) << "INFO: removed entries up to: " << end_marker <<  " from queue: " 
          << queue_name << dendl;
        }
      }

    }
  }

  // lits of queue names with an indication it is currently owned
  using owned_queues_t = std::unordered_map<std::string, bool>;

  // process all queues
  // find which of the queues is owned by this daemon and process it
  void process_queues(spawn::yield_context yield) {
    auto has_error = false;
    owned_queues_t owned_queues;

    // add randomness to the duration between queue checking
    // to make sure that different daemons are not synced
    std::random_device seed;
    std::mt19937 rnd_gen(seed());
    const auto min_jitter = 100; // ms
    const auto max_jitter = 500; // ms
    std::uniform_int_distribution<> duration_jitter(min_jitter, max_jitter);

    while (true) {
      Timer timer(io_context);
      const auto duration = (has_error ? 
        std::chrono::milliseconds(queues_update_retry_ms) : std::chrono::milliseconds(queues_update_period_ms)) + 
        std::chrono::milliseconds(duration_jitter(rnd_gen));
      timer.expires_from_now(duration);
      const auto tp = ceph::coarse_real_time::clock::to_time_t(ceph::coarse_real_time::clock::now() + duration);
      ldout(cct, 20) << "INFO: next queues processing will happen at: " << std::ctime(&tp)  << dendl;
      boost::system::error_code ec;
      timer.async_wait(yield[ec]);

      queues_t queues;
      auto ret = read_queue_list(queues, optional_yield(io_context, yield));
      if (ret < 0) {
        has_error = true;
        continue;
      }

      std::vector<std::string> queue_gc;
      std::mutex queue_gc_lock;
      for (const auto& queue_name : queues) {
        // try to lock the queue to check if it is owned by this rgw
        // or if ownershif needs to be taken
        librados::ObjectWriteOperation op;
        op.assert_exists();
        rados::cls::lock::lock(&op, queue_name+"_lock", 
              ClsLockType::EXCLUSIVE,
              lock_cookie, 
              "" /*no tag*/,
              "" /*no description*/,
              failover_time,
              LOCK_FLAG_MAY_RENEW);

        ret = rgw_rados_operate(rados_ioctx, queue_name, &op, optional_yield(io_context, yield));
        if (ret == -EBUSY) {
          // lock is already taken by another RGW
          ldout(cct, 20) << "INFO: queue: " << queue_name << " owned (locked) by another daemon" << dendl;
          // if queue is owned, processing should be stopped
          auto it = owned_queues.find(queue_name);
          if (it != owned_queues.end() && it->second) {
            it->second = false;
            ldout(cct, 5) << "WARNING: queue: " << queue_name << " ownership lost. processing will stop" << dendl;
          }
          continue;
        }
        if (ret == -ENOENT) {
          // queue is deleted - processing will stop the next time we try to read from the queue
          ldout(cct, 10) << "INFO: queue: " << queue_name << " should not be locked - already deleted" << dendl;
          continue;
        }
        if (ret < 0) {
          // failed to lock for another reason, continue to process other queues
          ldout(cct, 1) << "ERROR: failed to lock queue: " << queue_name << ". error: " << ret << dendl;
          has_error = true;
          continue;
        }
        // add queue to list of owned queues
        const auto insert_result = owned_queues.insert(std::make_pair(queue_name, true));
        if (insert_result.second) {
          ldout(cct, 10) << "INFO: queue: " << queue_name << " now owned (locked) by this daemon" << dendl;
          // start processing this queue
          const auto& owned = insert_result.first->second;
          spawn::spawn(io_context, [this, &queue_gc, &queue_gc_lock, &owned, queue_name](spawn::yield_context yield) {
            process_queue(queue_name, owned, yield);
            // if queue processing ended, it measn that the queue was removed or not owned anymore
            // mark it for deletion
            std::lock_guard lock_guard(queue_gc_lock);
            queue_gc.push_back(queue_name);
            ldout(cct, 10) << "INFO: queue: " << queue_name << " marked for removal" << dendl;
          });
        } else {
          ldout(cct, 20) << "INFO: queue: " << queue_name << " ownership (lock) renewed" << dendl;
        }
      }
      // erase all queue that were deleted
      {
        std::lock_guard lock_guard(queue_gc_lock);
        std::for_each(queue_gc.begin(), queue_gc.end(), [this, &owned_queues](const std::string& queue_name) {
          owned_queues.erase(queue_name);
          ldout(cct, 20) << "INFO: queue: " << queue_name << " removed" << dendl;
        });
        queue_gc.clear();
      }
    }
  }

public:

  ~Manager() {
    work_guard.reset();
    io_context.stop();
    std::for_each(workers.begin(), workers.end(), [] (auto& worker) { worker.join(); });
  }

  // ctor: start all threads
  Manager(CephContext* _cct, uint32_t _max_queue_size, uint32_t _queues_update_period_ms, 
          uint32_t _queues_update_retry_ms, uint32_t _queue_idle_sleep_us, u_int32_t failover_time_ms, 
          uint32_t _stale_reservations_period_s, uint32_t _reservations_cleanup_period_s,
          uint32_t _worker_count, rgw::sal::RGWRadosStore* store) : 
    max_queue_size(_max_queue_size),
    queues_update_period_ms(_queues_update_period_ms),
    queues_update_retry_ms(_queues_update_retry_ms),
    queue_idle_sleep_us(_queue_idle_sleep_us),
    failover_time(std::chrono::milliseconds(failover_time_ms)),
    cct(_cct),
    rados_ioctx(store->getRados()->get_notif_pool_ctx()),
    lock_cookie(gen_rand_alphanumeric(cct, COOKIE_LEN)),
    work_guard(boost::asio::make_work_guard(io_context)),
    worker_count(_worker_count),
    stale_reservations_period_s(_stale_reservations_period_s),
    reservations_cleanup_period_s(_reservations_cleanup_period_s)
    {
      spawn::spawn(io_context, [this](spawn::yield_context yield) {
            process_queues(yield);
          });

      // start the worker threads to do the actual queue processing
      const std::string WORKER_THREAD_NAME = "notif-worker";
      for (auto worker_id = 0U; worker_id < worker_count; ++worker_id) {
        workers.emplace_back([this]() { io_context.run(); });
        const auto rc = ceph_pthread_setname(workers.back().native_handle(), 
            (WORKER_THREAD_NAME+std::to_string(worker_id)).c_str());
        ceph_assert(rc == 0);
      }
      ldout(cct, 10) << "Started notification manager with: " << worker_count << " workers" << dendl;
    }

  int add_persistent_topic(const std::string& topic_name, optional_yield y) {
    if (topic_name == Q_LIST_OBJECT_NAME) {
      ldout(cct, 1) << "ERROR: topic name cannot be: " << Q_LIST_OBJECT_NAME << " (conflict with queue list object name)" << dendl;
      return -EINVAL;
    }
    librados::ObjectWriteOperation op;
    op.create(true);
    cls_2pc_queue_init(op, topic_name, max_queue_size);
    auto ret = rgw_rados_operate(rados_ioctx, topic_name, &op, y);
    if (ret == -EEXIST) {
      // queue already exists - nothing to do
      ldout(cct, 20) << "INFO: queue for topic: " << topic_name << " already exists. nothing to do" << dendl;
      return 0;
    }
    if (ret < 0) {
      // failed to create queue
      ldout(cct, 1) << "ERROR: failed to create queue for topic: " << topic_name << ". error: " << ret << dendl;
      return ret;
    }
   
    bufferlist empty_bl;
    std::map<std::string, bufferlist> new_topic{{topic_name, empty_bl}};
    op.omap_set(new_topic);
    ret = rgw_rados_operate(rados_ioctx, Q_LIST_OBJECT_NAME, &op, y);
    if (ret < 0) {
      ldout(cct, 1) << "ERROR: failed to add queue: " << topic_name << " to queue list. error: " << ret << dendl;
      return ret;
    } 
    ldout(cct, 20) << "INFO: queue: " << topic_name << " added to queue list"  << dendl;
    return 0;
  }
  
  int remove_persistent_topic(const std::string& topic_name, optional_yield y) {
    librados::ObjectWriteOperation op;
    op.remove();
    auto ret = rgw_rados_operate(rados_ioctx, topic_name, &op, y);
    if (ret == -ENOENT) {
      // queue already removed - nothing to do
      ldout(cct, 20) << "INFO: queue for topic: " << topic_name << " already removed. nothing to do" << dendl;
      return 0;
    }
    if (ret < 0) {
      // failed to remove queue
      ldout(cct, 1) << "ERROR: failed to remove queue for topic: " << topic_name << ". error: " << ret << dendl;
      return ret;
    }
  
    std::set<std::string> topic_to_remove{{topic_name}};
    op.omap_rm_keys(topic_to_remove);
    ret = rgw_rados_operate(rados_ioctx, Q_LIST_OBJECT_NAME, &op, y);
    if (ret < 0) {
      ldout(cct, 1) << "ERROR: failed to remove queue: " << topic_name << " from queue list. error: " << ret << dendl;
      return ret;
    } 
    ldout(cct, 20) << "INFO: queue: " << topic_name << " removed from queue list"  << dendl;
    return 0;
  }
};

// singleton manager
// note that the manager itself is not a singleton, and multiple instances may co-exist
// TODO make the pointer atomic in allocation and deallocation to avoid race conditions
static Manager* s_manager = nullptr;

constexpr size_t MAX_QUEUE_SIZE = 128*1000*1000; // 128MB
constexpr uint32_t Q_LIST_UPDATE_MSEC = 1000*30;     // check queue list every 30seconds
constexpr uint32_t Q_LIST_RETRY_MSEC = 1000;         // retry every second if queue list update failed
constexpr uint32_t IDLE_TIMEOUT_USEC = 100*1000;     // idle sleep 100ms
constexpr uint32_t FAILOVER_TIME_MSEC = 3*Q_LIST_UPDATE_MSEC; // FAILOVER TIME 3x renew time
constexpr uint32_t WORKER_COUNT = 1;                 // 1 worker thread
constexpr uint32_t STALE_RESERVATIONS_PERIOD_S = 120;   // cleanup reservations that are more than 2 minutes old
constexpr uint32_t RESERVATIONS_CLEANUP_PERIOD_S = 30; // reservation cleanup every 30 seconds

bool init(CephContext* cct, rgw::sal::RGWRadosStore* store) {
  if (s_manager) {
    return false;
  }
  // TODO: take conf from CephContext
  s_manager = new Manager(cct, MAX_QUEUE_SIZE, 
      Q_LIST_UPDATE_MSEC, Q_LIST_RETRY_MSEC, 
      IDLE_TIMEOUT_USEC, FAILOVER_TIME_MSEC, 
      STALE_RESERVATIONS_PERIOD_S, RESERVATIONS_CLEANUP_PERIOD_S,
      WORKER_COUNT,
      store);
  return true;
}

void shutdown() {
  delete s_manager;
  s_manager = nullptr;
}

int add_persistent_topic(const std::string& topic_name, optional_yield y) {
  if (!s_manager) {
    return -EAGAIN;
  }
  return s_manager->add_persistent_topic(topic_name, y);
}

int remove_persistent_topic(const std::string& topic_name, optional_yield y) {
  if (!s_manager) {
    return -EAGAIN;
  }
  return s_manager->remove_persistent_topic(topic_name, y);
}

// populate record from request
void populate_record_from_request(const req_state *s, 
        const rgw::sal::RGWObject* obj,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        rgw_pubsub_s3_record& record) { 
  record.eventTime = mtime;
  record.eventName = to_string(event_type);
  record.userIdentity = s->user->get_id().id;    // user that triggered the change
  record.x_amz_request_id = s->req_id;          // request ID of the original change
  record.x_amz_id_2 = s->host_id;               // RGW on which the change was made
  // configurationId is filled from notification configuration
  record.bucket_name = s->bucket_name;
  record.bucket_ownerIdentity = s->bucket_owner.get_id().id;
  record.bucket_arn = to_string(rgw::ARN(s->bucket->get_bi()));
  record.object_key = obj->get_name();
  record.object_size = size;
  record.object_etag = etag;
  record.object_versionId = obj->get_instance();
  // use timestamp as per key sequence id (hex encoded)
  const utime_t ts(real_clock::now());
  boost::algorithm::hex((const char*)&ts, (const char*)&ts + sizeof(utime_t), 
          std::back_inserter(record.object_sequencer));
  set_event_id(record.id, etag, ts);
  record.bucket_id = s->bucket->get_bucket_id();
  // pass meta data
  record.x_meta_map = s->info.x_meta_map;
  // pass tags
  record.tags = s->tagset.get_tags();
  // opaque data will be filled from topic configuration
}

bool match(const rgw_pubsub_topic_filter& filter, const req_state* s, const rgw::sal::RGWObject* obj, EventType event) {
  if (!::match(filter.events, event)) { 
    return false;
  }
  if (!::match(filter.s3_filter.key_filter, obj->get_name())) {
    return false;
  }
  if (!::match(filter.s3_filter.metadata_filter, s->info.x_meta_map)) {
    return false;
  }
  if (!::match(filter.s3_filter.tag_filter, s->tagset.get_tags())) {
    return false;
  }
  return true;
}

int publish_reserve(EventType event_type,
      reservation_t& res) 
{
  RGWUserPubSub ps_user(res.store, res.s->user->get_id());
  RGWUserPubSub::Bucket ps_bucket(&ps_user, res.s->bucket->get_bi());
  rgw_pubsub_bucket_topics bucket_topics;
  auto rc = ps_bucket.get_topics(&bucket_topics);
  if (rc < 0) {
    // failed to fetch bucket topics
    return rc;
  }
  for (const auto& bucket_topic : bucket_topics.topics) {
    const rgw_pubsub_topic_filter& topic_filter = bucket_topic.second;
    const rgw_pubsub_topic& topic_cfg = topic_filter.topic;
    if (!match(topic_filter, res.s, res.object, event_type)) {
      // topic does not apply to req_state
      continue;
    }
    ldout(res.s->cct, 20) << "INFO: notification: '" << topic_filter.s3_id << 
        "' on topic: '" << topic_cfg.dest.arn_topic << 
        "' and bucket: '" << res.s->bucket->get_name() << 
        "' (unique topic: '" << topic_cfg.name <<
        "') apply to event of type: '" << to_string(event_type) << "'" << dendl;

    cls_2pc_reservation::id_t res_id;
    if (topic_cfg.dest.persistent) {
      // TODO: take default reservation size from conf
      constexpr auto DEFAULT_RESERVATION = 4*1024U; // 4K
      res.size = DEFAULT_RESERVATION;
      librados::ObjectWriteOperation op;
      bufferlist obl;
      int rval;
      const auto& queue_name = topic_cfg.dest.arn_topic;
      cls_2pc_queue_reserve(op, res.size, 1, &obl, &rval);
      auto ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(), 
          queue_name, &op, librados::OPERATION_RETURNVEC, res.s->yield);
      if (ret < 0) {
        ldout(res.s->cct, 1) << "ERROR: failed to reserve notification on queue: " << queue_name 
          << ". error: " << ret << dendl;
        // if no space is left in queue we ask client to slow down
        return (ret == -ENOSPC) ? -ERR_RATE_LIMITED : ret;
      }
      ret = cls_2pc_queue_reserve_result(obl, res_id);
      if (ret < 0) {
        ldout(res.s->cct, 1) << "ERROR: failed to parse reservation id. error: " << ret << dendl;
        return ret;
      }
    }
    res.topics.emplace_back(topic_filter.s3_id, topic_cfg, res_id);
  }
  return 0;
}

int publish_commit(const rgw::sal::RGWObject* obj,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        reservation_t& res) 
{
  for (auto& topic : res.topics) {
    if (topic.cfg.dest.persistent && topic.res_id == cls_2pc_reservation::NO_ID) {
      // nothing to commit or already committed/aborted
      continue;
    }
    record_with_endpoint_t record_with_endpoint;
    populate_record_from_request(res.s, obj, size, mtime, etag, event_type, record_with_endpoint.record);
    record_with_endpoint.record.configurationId = topic.configurationId;
    record_with_endpoint.record.opaque_data = topic.cfg.opaque_data;
    if (topic.cfg.dest.persistent) { 
      record_with_endpoint.push_endpoint = std::move(topic.cfg.dest.push_endpoint);
      record_with_endpoint.push_endpoint_args = std::move(topic.cfg.dest.push_endpoint_args);
      record_with_endpoint.arn_topic = std::move(topic.cfg.dest.arn_topic);
      bufferlist bl;
      encode(record_with_endpoint, bl);
      const auto& queue_name = topic.cfg.dest.arn_topic;
      if (bl.length() > res.size) {
        // try to make a larger reservation, fail only if this is not possible
        ldout(res.s->cct, 5) << "WARNING: committed size: " << bl.length() << " exceeded reserved size: " << res.size <<
          " . trying to make a larger reservation on queue:" << queue_name << dendl;
        // first cancel the existing reservation
        librados::ObjectWriteOperation op;
        cls_2pc_queue_abort(op, topic.res_id);
        auto ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(),
            topic.cfg.dest.arn_topic, &op,
            res.s->yield);
        if (ret < 0) {
          ldout(res.s->cct, 1) << "ERROR: failed to abort reservation: " << topic.res_id << 
            " when trying to make a larger reservation on queue: " << queue_name
            << ". error: " << ret << dendl;
          return ret;
        }
        // now try to make a bigger one
        bufferlist obl;
        int rval;
        cls_2pc_queue_reserve(op, bl.length(), 1, &obl, &rval);
        ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(), 
          queue_name, &op, librados::OPERATION_RETURNVEC, res.s->yield);
        if (ret < 0) {
          ldout(res.s->cct, 1) << "ERROR: failed to reserve extra space on queue: " << queue_name
            << ". error: " << ret << dendl;
          return (ret == -ENOSPC) ? -ERR_RATE_LIMITED : ret;
        }
        ret = cls_2pc_queue_reserve_result(obl, topic.res_id);
        if (ret < 0) {
          ldout(res.s->cct, 1) << "ERROR: failed to parse reservation id for extra space. error: " << ret << dendl;
          return ret;
        }
      }
      std::vector<bufferlist> bl_data_vec{std::move(bl)};
      librados::ObjectWriteOperation op;
      cls_2pc_queue_commit(op, bl_data_vec, topic.res_id);
      const auto ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(),
            queue_name, &op,
            res.s->yield);
      topic.res_id = cls_2pc_reservation::NO_ID;
      if (ret < 0) {
        ldout(res.s->cct, 1) << "ERROR: failed to commit reservation to queue: " << queue_name
          << ". error: " << ret << dendl;
        return ret;
      }
    } else {
      try {
        // TODO add endpoint LRU cache
        const auto push_endpoint = RGWPubSubEndpoint::create(topic.cfg.dest.push_endpoint, 
                topic.cfg.dest.arn_topic,
                RGWHTTPArgs(topic.cfg.dest.push_endpoint_args), 
                res.s->cct);
        ldout(res.s->cct, 20) << "INFO: push endpoint created: " << topic.cfg.dest.push_endpoint << dendl;
        const auto ret = push_endpoint->send_to_completion_async(res.s->cct, record_with_endpoint.record, res.s->yield);
        if (ret < 0) {
          ldout(res.s->cct, 1) << "ERROR: push to endpoint " << topic.cfg.dest.push_endpoint << " failed. error: " << ret << dendl;
          if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
          return ret;
        }
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
      } catch (const RGWPubSubEndpoint::configuration_error& e) {
        ldout(res.s->cct, 1) << "ERROR: failed to create push endpoint: " 
            << topic.cfg.dest.push_endpoint << ". error: " << e.what() << dendl;
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
        return -EINVAL;
      }
    }
  }
  return 0;
}

int publish_abort(reservation_t& res) {
  for (auto& topic : res.topics) {
    if (!topic.cfg.dest.persistent || topic.res_id == cls_2pc_reservation::NO_ID) {
      // nothing to abort or already committed/aborted
      continue;
    }
    const auto& queue_name = topic.cfg.dest.arn_topic;
    librados::ObjectWriteOperation op;
    cls_2pc_queue_abort(op, topic.res_id);
    const auto ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(),
      queue_name, &op,
      res.s->yield);
    if (ret < 0) {
      ldout(res.s->cct, 1) << "ERROR: failed to abort reservation: " << topic.res_id << 
        " from queue: " << queue_name << ". error: " << ret << dendl;
      return ret;
    }
    topic.res_id = cls_2pc_reservation::NO_ID;
  }
  return 0;
}

reservation_t::~reservation_t() {
  publish_abort(*this);
}

}

