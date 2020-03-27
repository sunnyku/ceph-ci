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

// TODO: why decode/encode can't resolve that?
//using queues_t = std::unordered_set<std::string>;

struct queues_t {
  std::unordered_set<std::string> list;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(list, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(list, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(queues_t)

class Manager {
  const size_t max_queue_size;
  const long queues_update_period_ms;
  const long queues_update_retry_ms;
  const long queue_idle_sleep_us;
  const utime_t failover_time;
  CephContext* const cct;
  librados::IoCtx& rados_ioctx;
  RGWUserPubSub ps_user;
  std::unordered_set<std::string> owned_queues;
  std::atomic_bool list_of_queues_object_created;
  const std::string lock_cookie;
  boost::asio::io_context io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard;
  const size_t worker_count;
  std::vector<std::thread> workers;
 
  const std::string Q_LIST_OBJECT_NAME = "queues_list_object";

  // try create the object holding the list of queues if not created so far
  // note that in case of race from two threads, or if another rgw
  // already created the object, one would get -EEXIST, and the function would return 0 (success)
  int try_to_create_queue_list() {
    if (!list_of_queues_object_created) {
      // create the object holding the list of queues
      const auto ret = rados_ioctx.create(Q_LIST_OBJECT_NAME, false);
      if (ret < 0 && ret != -EEXIST) {
        ldout(cct, 1) << "ERROR: failed to create queue list. error: " << ret << dendl;
        return ret;
      }
      list_of_queues_object_created = true;
    }
    return 0;
  }

  // read the list of queues from the queue list object
  int read_queue_list(queues_t& queues) {
    bufferlist bl;
    constexpr auto chunk_size = 1024U;
    auto start_offset = 0U;

    int ret;
    do {
      bufferlist chunk_bl;
      // TODO: add yield ?
      ret = rados_ioctx.read(Q_LIST_OBJECT_NAME, chunk_bl, chunk_size, start_offset);
      if (ret < 0) {
        ldout(cct, 1) << "ERROR: failed to read queue list. error: " << ret << dendl;
        return ret;
      }
      start_offset += ret;
      bl.claim_append(chunk_bl);
    } while (ret > 0);

    if (bl.length() == 0) {
      // nothing in the list
      return 0;
    }

    auto iter = bl.cbegin();
    try {
      decode(queues, iter);
    } catch (buffer::error& err) {
      ldout(cct, 1) << "ERROR: failed to decode queue list. error: " << err.what() << dendl;
      return -EINVAL;
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

   class tokens_waiter {
     const boost::posix_time::time_duration infinite_duration = boost::posix_time::hours(1000);
     size_t pending_tokens = 0;
     boost::asio::deadline_timer timer;
 
     struct token {
       tokens_waiter& waiter;
       token(tokens_waiter& _waiter) : waiter(_waiter){
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

     tokens_waiter(boost::asio::io_context& io_context) : timer(io_context) {}  
 
     void async_wait(spawn::yield_context yield) { 
       timer.expires_from_now(infinite_duration);
       boost::system::error_code ec; 
       timer.async_wait(yield[ec]);
       // TODO: make sure that the timer was cancelled and not rexpired
     }   
 
     token make_token() {    
       return token(*this);
     }   
  };


  // processing of a specific queue
  // TODO: use string_view for queue_name
  void process_queue(spawn::yield_context yield, std::string queue_name) {
    const auto max_elements = 1024;
    const std::string start_marker;
    bool truncated = false;
    std::string end_marker;
    std::vector<cls_queue_entry> entries;
    auto is_idle = true;
    auto total_entries = 0U;
    auto pending_work = 0U;
    tokens_waiter waiter(io_context);

    const auto ret = cls_2pc_queue_list_entries(rados_ioctx, queue_name, start_marker, max_elements, entries, &truncated, end_marker);
    if (ret < 0) {
      ldout(cct, 5) << "WARNING: failed to get list of entries in queue: " 
        << queue_name << ". error: " << ret << " (will retry)" << dendl;
      goto sched_next;
    }
    total_entries = entries.size();
    ldout(cct, 20) << "INFO: found: " << total_entries << " entries from: " << queue_name << dendl;
    if (total_entries == 0) {
      // nothing in the queue
      goto sched_next;
    }
    
    is_idle = false;
    {
      auto has_error = false;
      auto remove_entries = false;
      auto entry_idx = 0U;
      for (auto& entry : entries) {
        if (has_error) {
          // bail out on first error
          break;
        }
        // TODO pass entry pointer instead of by-value
        spawn::spawn(io_context, [this, entry_idx, total_entries, &end_marker, &remove_entries, &has_error, &waiter, entry](spawn::yield_context yield) {
          const auto token = waiter.make_token();
          record_with_endpoint_t record_with_endpoint;
          auto iter = entry.data.cbegin();
          try {
            decode(record_with_endpoint, iter);
          } catch (buffer::error& err) {
            ldout(cct, 5) << "WARNING: failed to decode entry. error: " << err.what() << dendl;
            has_error = true;
            return;
          }
          try {
            // TODO move endpoint creation outside the entries loop
            const auto push_endpoint = RGWPubSubEndpoint::create(record_with_endpoint.push_endpoint, record_with_endpoint.arn_topic,
                RGWHTTPArgs(record_with_endpoint.push_endpoint_args), 
                cct);
            ldout(cct, 20) << "INFO: push endpoint created: " << record_with_endpoint.push_endpoint <<
              " for entry: " << entry_idx << "/" << total_entries << " (marker: " << entry.marker << ")" << dendl;
            const auto ret = push_endpoint->send_to_completion_async(cct, record_with_endpoint.record, optional_yield(io_context, yield));
            if (ret < 0) {
              ldout(cct, 5) << "WARNING: push entry: " << entry_idx << "/" << total_entries << " (marker: " << entry.marker << ") to endpoint: " << record_with_endpoint.push_endpoint 
                << " failed. error: " << ret << " (will retry)" << dendl;
              if (set_min_marker(end_marker, entry.marker) < 0) {
                ldout(cct, 1) << "ERROR: cannot determin minimum between malformed markers: " << end_marker << ", " << entry.marker << dendl;
              } else {
                ldout(cct, 20) << "INFO: end marker for removal: " << end_marker << dendl;
              }
              has_error = true;
              return;
            } else {
              ldout(cct, 20) << "INFO: push entry: " << entry_idx << "/" << total_entries << " (marker: " << entry.marker << ") to endpoint: " << record_with_endpoint.push_endpoint 
                << " OK" <<  dendl;
              if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
              // there is at least one entry to remove (was successfully published)
              remove_entries = true;
            }
          } catch (const RGWPubSubEndpoint::configuration_error& e) {
            ldout(cct, 5) << "WARNING: failed to create push endpoint: " 
                << record_with_endpoint.push_endpoint << ". error: " << e.what() << " (will retry) " << dendl;
            if (set_min_marker(end_marker, entry.marker) < 0) {
              ldout(cct, 1) << "ERROR: cannot determin minimum between malformed markers: " << end_marker << ", " << entry.marker << dendl;
            } else {
              ldout(cct, 20) << "INFO: end marker for removal: " << end_marker << dendl;
            }
            has_error = true;
            return;
          }
        });
        ++entry_idx;
      }

      // wait for all pending work to finish
      waiter.async_wait(yield);

      // delete all published entries from queue
      if (remove_entries) {
        librados::ObjectWriteOperation op;
        cls_2pc_queue_remove_entries(op, end_marker); 
        // TODO call operate async with yield
        const auto ret = rados_ioctx.operate(queue_name, &op);
        if (ret < 0) {
          ldout(cct, 1) << "ERROR: failed to remove entries up to: " << end_marker  << " from queue: " 
            << queue_name << ". error: " << ret << dendl;
        } else {
          ldout(cct, 20) << "INFO: removed entries up to: " << end_marker <<  " from queue: " 
            << queue_name << dendl;
        }
      }
      // TODO: cleanup expired reservations
    }

sched_next:
    if (is_idle) {
      boost::asio::deadline_timer timer(io_context);
      timer.expires_from_now(boost::posix_time::microseconds(queue_idle_sleep_us));
	    ldout(cct, 20) << "INFO: queue " << queue_name << " is idle, next processing will happen in: " << queue_idle_sleep_us << " usec" << dendl;
      boost::system::error_code ec;
	    timer.async_wait(yield[ec]);
      // TODO check ec
	    spawn::spawn(io_context, [this, &queue_name](spawn::yield_context yield) {
          process_queue(yield, queue_name);
          });
    } else {
	    ldout(cct, 20) << "INFO: queue " << queue_name << " is will be processed again. pending work for this queue: " << pending_work << dendl;
	    spawn::spawn(io_context, [this, &queue_name](spawn::yield_context yield) {
        process_queue(yield, queue_name);
        });
    }
  }

  // process all queues
  // find which of the queues is owned by this daemon and process it
  void process_queues(spawn::yield_context yield) {
    auto has_error = false;
    auto ret = try_to_create_queue_list();
    if (ret < 0) {
      // failed to create the queue list object
      has_error = true;
      goto sched_next;
    }
    {
      queues_t queues;
      ret = read_queue_list(queues);
      if (ret < 0) {
        // failed to create the queue list object
        has_error = true;
        goto sched_next;
      }

      for (const auto& queue_name : queues.list) {
        // try to lock the queue to check if it is owned by this rgw
        // or if ownershif needs to be taken
        ret = rados::cls::lock::lock(&rados_ioctx, queue_name, queue_name+"_lock", 
              LOCK_EXCLUSIVE,
              lock_cookie, 
              "" /*no tag*/,
              "" /*no description*/,
              failover_time,
              LOCK_FLAG_MAY_RENEW);

        if (ret == -EBUSY) {
          // lock is already taken by another RGW
          ldout(cct, 20) << "INFO: queue: " << queue_name << " owned (locked) by another daemon" << dendl;
          continue;
        }
        if (ret < 0) {
          // failed to lock for another reason, continue to process other queues
          ldout(cct, 1) << "ERROR: failed renew lock on queue: " << queue_name << ". error: " << ret << dendl;
          has_error = true;
        }
        // add queue to list of owned queues
        if (owned_queues.insert(queue_name).second) {
          ldout(cct, 20) << "INFO: queue: " << queue_name << " now owned (locked) by this daemon" << dendl;
          // start processing this queue
          spawn::spawn(io_context, [this, &queue_name](spawn::yield_context yield) {
              process_queue(yield, queue_name);
              });
        } else {
          ldout(cct, 20) << "INFO: queue: " << queue_name << " ownership (lock) renewed" << dendl;
        }
      }
    }

sched_next:
    // schedule next time queues are processed
    boost::asio::deadline_timer timer(io_context);
    const auto duration = has_error ? 
      boost::posix_time::milliseconds(queues_update_retry_ms) : boost::posix_time::milliseconds(queues_update_period_ms);
    timer.expires_from_now(duration);
	  ldout(cct, 20) << "INFO: next queue processing will happen in: " << duration.seconds() << " ms" << dendl;
    boost::system::error_code ec;
	  timer.async_wait(yield[ec]);
    // TODO: check on ec
	  spawn::spawn(io_context, [this](spawn::yield_context yield) {
        process_queues(yield);
        });
  }

  std::string make_lock_cookie() const {
    // create a lock cookie for the manager
    // this is needed so that other RGWs won't be able to renew this lock
    // and will take over only after it expires
    constexpr auto COOKIE_LEN = 16;
    char buf[COOKIE_LEN + 1];
    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    return std::string(buf, sizeof(buf));
  }

public:

  ~Manager() {
    work_guard.reset();
    io_context.stop();
    std::for_each(workers.begin(), workers.end(), [] (auto& worker) { worker.join(); });
  }

  // ctor: start all threads
  Manager(CephContext* _cct, long _max_queue_size, long _queues_update_period_ms, 
          long _queues_update_retry_ms, long _queue_idle_sleep_us, long failover_time_sec, rgw::sal::RGWRadosStore* store) : 
    max_queue_size(_max_queue_size),
    queues_update_period_ms(_queues_update_period_ms),
    queues_update_retry_ms(_queues_update_retry_ms),
    queue_idle_sleep_us(_queue_idle_sleep_us),
    failover_time(std::chrono::seconds(failover_time_sec)),
    cct(_cct),
    rados_ioctx(store->getRados()->get_notif_pool_ctx()),
    // TODO: how to get user name
    ps_user(store, rgw_user("user")),  
    list_of_queues_object_created(false),
    lock_cookie(make_lock_cookie()),
    work_guard(boost::asio::make_work_guard(io_context)),
    // TODO: get from conf
    worker_count(1)
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
   
    ret = try_to_create_queue_list();
    if (ret < 0) {
      return ret;
    }

    // update the new queue in the list of queues
    // TODO: make read-modify-write atomic
    bufferlist bl;
    constexpr auto chunk_size = 1024U;
    auto start_offset = 0U;
    do {
      // TODO: add yield ?
      bufferlist chunk_bl;
      ret = rados_ioctx.read(Q_LIST_OBJECT_NAME, chunk_bl, chunk_size, start_offset);
      if (ret < 0) {
        ldout(cct, 1) << "ERROR: failed to read queue list. error: " << ret << dendl;
        return ret;
      }
      start_offset += ret;
      bl.claim_append(chunk_bl);
    } while (ret > 0);

    queues_t queues;
    if (bl.length() > 0) {
      auto iter = bl.cbegin();
      try {
        decode(queues, iter);
      } catch (buffer::error& err) {
        ldout(cct, 1) << "ERROR: failed to decode queue list. error: " << err.what() << dendl;
        return -EINVAL;
      }
      bl.clear();
    }

    // no need to check for duplicate names
    queues.list.insert(topic_name);
    encode(queues, bl);
    ret = rados_ioctx.write_full(Q_LIST_OBJECT_NAME, bl);
    if (ret < 0) {
      ldout(cct, 1) << "ERROR: failed to write queue list. error: " << ret << dendl;
      return ret;
    } 
    ldout(cct, 20) << "INFO: queue: " << topic_name << " added to queue list (has: " << queues.list.size() << " queues)"  << dendl;
    return 0;
  }
};

// singleton manager
// note that the manager itself is not a singleton, and multiple instances may co-exist
// TODO make the pointer atomic in allocation and deallocation to avoid race conditions
static Manager* s_manager = nullptr;

constexpr size_t MAX_QUEUE_SIZE = 128*1024*1024; // 128MB
constexpr long Q_LIST_UPDATE_MSEC = 1000*30;     // check queue list every 30seconds
constexpr long Q_LIST_RETRY_MSEC = 1000;         // retry every second if queue list update failed
constexpr long IDLE_TIMEOUT_USEC = 100*1000;     // idle sleep 100ms
constexpr long FAILOVER_TIME_SEC = 30;           // FAILOVER TIME 30 SEC

bool init(CephContext* cct, rgw::sal::RGWRadosStore* store) {
  if (s_manager) {
    return false;
  }
  // TODO: take conf from CephContext
  s_manager = new Manager(cct, MAX_QUEUE_SIZE, Q_LIST_UPDATE_MSEC, Q_LIST_RETRY_MSEC,
          IDLE_TIMEOUT_USEC, FAILOVER_TIME_SEC, store);
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

// populate record from request
void populate_record_from_request(const req_state *s, 
        const rgw_obj_key& key,
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
  record.bucket_arn = to_string(rgw::ARN(s->bucket));
  record.object_key = key.name;
  record.object_size = size;
  record.object_etag = etag;
  record.object_versionId = key.instance;
  // use timestamp as per key sequence id (hex encoded)
  const utime_t ts(real_clock::now());
  boost::algorithm::hex((const char*)&ts, (const char*)&ts + sizeof(utime_t), 
          std::back_inserter(record.object_sequencer));
  set_event_id(record.id, etag, ts);
  record.bucket_id = s->bucket.bucket_id;
  // pass meta data
  record.x_meta_map = s->info.x_meta_map;
  // pass tags
  record.tags = s->tagset.get_tags();
  // opaque data will be filled from topic configuration
}

bool match(const rgw_pubsub_topic_filter& filter, const req_state* s, EventType event) {
  if (!::match(filter.events, event)) { 
    return false;
  }
  if (!::match(filter.s3_filter.key_filter, s->object.name)) {
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
      reservation_t& res) {
  RGWUserPubSub ps_user(res.store, res.s->user->get_id());
  RGWUserPubSub::Bucket ps_bucket(&ps_user, res.s->bucket);
  rgw_pubsub_bucket_topics bucket_topics;
  auto rc = ps_bucket.get_topics(&bucket_topics);
  if (rc < 0) {
    // failed to fetch bucket topics
    return rc;
  }
  for (const auto& bucket_topic : bucket_topics.topics) {
    const rgw_pubsub_topic_filter& topic_filter = bucket_topic.second;
    const rgw_pubsub_topic& topic_cfg = topic_filter.topic;
    if (!match(topic_filter, res.s, event_type)) {
      // topic does not apply to req_state
      continue;
    }
    ldout(res.s->cct, 20) << "INFO: notification: '" << topic_filter.s3_id << 
        "' on topic: '" << topic_cfg.dest.arn_topic << 
        "' and bucket: '" << res.s->bucket.name << 
        "' (unique topic: '" << topic_cfg.name <<
        "') apply to event of type: '" << to_string(event_type) << "'" << dendl;

    cls_2pc_reservation::id_t res_id;
    if (topic_cfg.dest.persistent) {
      librados::ObjectWriteOperation op;
      // TODO: calculate based on max strings sizes?
      const auto size_to_reserve = 1024;
      const auto ret = cls_2pc_queue_reserve(res.store->getRados()->get_notif_pool_ctx(),
            topic_cfg.dest.arn_topic, op, size_to_reserve, 1, res_id);
      if (ret < 0) {
        ldout(res.s->cct, 1) << "ERROR: failed to reserve notification on queue. error: " << ret << dendl;
        // if no space is left in queue we ask client to slow down
        return (ret == -ENOSPC) ? -ERR_RATE_LIMITED : ret;
      }
    }
    res.topics.emplace_back(topic_filter.s3_id, topic_cfg, res_id);
  }
  return 0;
}


int publish_commit(const rgw_obj_key& key,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        reservation_t& res) {
  for (auto& topic : res.topics) {
    if (topic.cfg.dest.persistent && topic.res_id == cls_2pc_reservation::NO_ID) {
      // nothing to commit or already committed/aborted
      continue;
    }
    record_with_endpoint_t record_with_endpoint;
    populate_record_from_request(res.s, key, size, mtime, etag, event_type, record_with_endpoint.record);
    record_with_endpoint.record.configurationId = topic.configurationId;
    record_with_endpoint.record.opaque_data = topic.cfg.opaque_data;
    if (topic.cfg.dest.persistent) { 
      record_with_endpoint.push_endpoint = std::move(topic.cfg.dest.push_endpoint);
      record_with_endpoint.push_endpoint_args = std::move(topic.cfg.dest.push_endpoint_args);
      record_with_endpoint.arn_topic = std::move(topic.cfg.dest.arn_topic);
      bufferlist bl;
      encode(record_with_endpoint, bl);
      std::vector<bufferlist> bl_data_vec{std::move(bl)};
      // TODO: check bl size
      librados::ObjectWriteOperation op;
      cls_2pc_queue_commit(op, bl_data_vec, topic.res_id);
      const auto ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(),
            topic.cfg.dest.arn_topic, &op,
            res.s->yield);
      topic.res_id = cls_2pc_reservation::NO_ID;
      if (ret < 0) {
        ldout(res.s->cct, 1) << "ERROR: failed to commit reservation to queue. error: " << ret << dendl;
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
    librados::ObjectWriteOperation op;
    cls_2pc_queue_abort(op,  topic.res_id);
    const auto ret = rgw_rados_operate(res.store->getRados()->get_notif_pool_ctx(),
      topic.cfg.dest.arn_topic, &op,
      res.s->yield);
    if (ret < 0) {
      ldout(res.s->cct, 1) << "ERROR: failed to abort reservation: " << topic.res_id <<
          ". error: " << ret << dendl;
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

