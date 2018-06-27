#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_pubsub.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"
#include "rgw_op.h"
#include "rgw_pubsub.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw


#define PS_NUM_PUB_SHARDS_DEFAULT 64
#define PS_NUM_PUB_SHARDS_MIN     16

#define PS_NUM_TOPIC_SHARDS_DEFAULT 16
#define PS_NUM_TOPIC_SHARDS_MIN     8

struct PSSubConfig { /* subscription config */
  string name;
  string topic;
  string push_endpoint;

  void init(CephContext *cct, const JSONFormattable& config) {
    name = config["name"];
    topic = config["topic"];
    push_endpoint = config["push_endpoint"];
  }
};

struct PSTopicConfig {
  string name;
};

struct PSNotificationConfig {
  string path; /* a path or a path prefix that would trigger the event (prefix: if ends with a wildcard) */
  string topic;

  uint64_t id{0};
  bool is_prefix{false};

  void init(CephContext *cct, const JSONFormattable& config) {
    path = config["path"];
    if (!path.empty() && path[path.size() - 1] == '*') {
      path = path.substr(0, path.size() - 1);
      is_prefix = true;
    }
    topic = config["topic"];
  }
};


struct PSConfig {
  string id{"pubsub"};
  uint64_t sync_instance{0};
  uint32_t num_pub_shards{0};
  uint32_t num_topic_shards{0};
  uint64_t max_id{0};

  /* FIXME: no hard coded buckets, we'll have configurable topics */
  vector<PSSubConfig> subscriptions;
  map<string, PSTopicConfig> topics;
  multimap<string, PSNotificationConfig> notifications;

  void init(CephContext *cct, const JSONFormattable& config) {
    num_pub_shards = config["num_pub_shards"](PS_NUM_PUB_SHARDS_DEFAULT);
    if (num_pub_shards < PS_NUM_PUB_SHARDS_MIN) {
      num_pub_shards = PS_NUM_PUB_SHARDS_MIN;
    }

    num_topic_shards = config["num_topic_shards"](PS_NUM_TOPIC_SHARDS_DEFAULT);
    if (num_topic_shards < PS_NUM_TOPIC_SHARDS_MIN) {
      num_topic_shards = PS_NUM_TOPIC_SHARDS_MIN;
    }
    /* FIXME: this will be dynamically configured */
    for (auto& c : config["notifications"].array()) {
      PSNotificationConfig nc;
      nc.id = ++max_id;
      nc.init(cct, c);
      notifications.insert(std::make_pair(nc.path, nc));

      PSTopicConfig topic_config = { .name = nc.topic };
      topics[nc.topic] = topic_config;
    }
    for (auto& c : config["subscriptions"].array()) {
      PSSubConfig sc;
      sc.init(cct, c);
      subscriptions.push_back(sc);
    }
  }

  void init_instance(RGWRealm& realm, uint64_t instance_id) {
    sync_instance = instance_id;
  }

  void get_notifs(const RGWBucketInfo& bucket_info, const rgw_obj_key& key, vector<PSNotificationConfig *> *notifs) {
    string path = bucket_info.bucket.name + "/" + key.name;

    notifs->clear();

    auto iter = notifications.upper_bound(path);
    if (iter == notifications.begin()) {
      return;
    }

    --iter;
    do {
      if (iter->first.size() > path.size()) {
        break;
      }
      if (path.compare(0, iter->first.size(), iter->first) != 0) {
        break;
      }

      PSNotificationConfig *target = &iter->second;

      if (!target->is_prefix &&
          path.size() != iter->first.size()) {
        continue;
      }

      notifs->push_back(target);
    } while (iter != notifications.begin());
  }
};

using PSConfigRef = std::shared_ptr<PSConfig>;

class RGWPSInitConfigCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  PSConfigRef conf;
public:
  RGWPSInitConfigCBCR(RGWDataSyncEnv *_sync_env,
                          PSConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                                    sync_env(_sync_env),
                                                    conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": init pubsub config zone=" << sync_env->source_zone << dendl;

      /* nothing to do here right now */

      return set_cr_done();
    }
    return 0;
  }
};


static string topic_shard_meta_oid(const string& topic, int shard_id)
{
  char buf[64 + topic.size()];
  snprintf(buf, sizeof(buf), "pubsub.topic.meta/%s.%d", topic.c_str(), shard_id);
  return string(buf);
}

static string topic_shard_oid(const string& topic, int shard_id, uint64_t index)
{
  char buf[64 + topic.size()];
  snprintf(buf, sizeof(buf), "pubsub.topic/%s.%d.%lld", topic.c_str(), shard_id, (long long)index);
  return string(buf);
}


class PSTopicWriteCurIndexMeta : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string topic;
  int shard_id;
  PSConfigRef conf;
  string meta_oid;
public:
  PSTopicShardPrepare(RGWDataSyncEnv *_sync_env,
                     const string& _topic,
                     int _shard_id,
                     PSConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               topic(_topic),
                                               shard_id(_shard_id),
                                               conf(_conf) {
  }

  int operate() override {
    reenter(this) {

      return set_cr_done();
    }
    return 0;
  }
};

struct topic_shard_meta_entry_info {
  ceph::real_time timestamp;
  uint64_t id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(timestamp, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(timestamp, bl);
    decode(id, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(topic_shard_meta_entry_info)


static void prepare_timelog_entry(uint64_t id, cls_log_entry *entry)
{
  cls_log_entry entry;
  string section; /* unused */
  string name; /* unused */

  topic_shard_meta_entry_info info;

  info.id = id;
  info.timestamp = real_clock::now();
  
  bufferlist bl;
  encode(info, bl);
  store->time_log_prepare_entry(*entry, info.timestamp, section, name, bl);
}


class PSTopicShardGetCurIndexMeta : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string topic;
  int shard_id;
  PSConfigRef conf;
  string meta_oid;
  cls_log_header log_header;
  RGWRados *store;
public:
  PSTopicShardGetCurIndex(RGWDataSyncEnv *_sync_env,
                     const string& _topic,
                     int _shard_id,
                     PSConfigRef _conf,
                     int *cur_index) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               topic(_topic),
                                               shard_id(_shard_id),
                                               conf(_conf) {
    store = sync_env->store;
  }

  int operate() override {
    reenter(this) {

      yield {
        call (new RGWRadosTimeLogInfoCR(store,
                                        rgw_raw_obj(store->get_zone_params().log_pool, topic_shard_meta_oid(topic, shard_id)),
                                        &log_header));
      }
      if (retcode < 0 && ret != -ENOENT) {
        return set_cr_error(retcode);
      }

      if (ret != -ENOENT) {
        yield {
          cls_log_entry entry;

          prepare_timelog_entry(0, &entry);

          call (new RGWRadosTimeLogAddCR(store,
                                          rgw_raw_obj(store->get_zone_params().log_pool, topic_shard_meta_oid(topic, shard_id)),
                                          entry));
        }

        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        *cur_index = 0;

        return set_cr_done();
      }

     ... 

      return set_cr_done();
    }
    return 0;
  }
};

class PSTopicShardAddLogEntry : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  string topic;
  int shard_id;
  std::shared_ptr<rgw_pubsub_event> event;
  PSConfigRef conf;
  string meta_oid;
public:
  PSTopicShardAddLogEntry(RGWDataSyncEnv *_sync_env,
                          const string& _topic,
                          int _shard_id,
                          std::shared_ptr<rgw_pubsub_event>& _event,
                          PSConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               topic(_topic),
                                               shard_id(_shard_id),
                                               event(_event),
                                               conf(_conf) {
  }

  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": init pubsub config zone=" << sync_env->source_zone << dendl;

#warning implement me      

      return set_cr_done();
    }
    return 0;
  }
};

class PSTopicAddLogEntry : public RGWCoroutine {
  static std::atomic<int> counter;
  RGWDataSyncEnv *sync_env;
  string topic;
  std::shared_ptr<rgw_pubsub_event> event;
  PSConfigRef conf;
public:
  PSTopicAddLogEntry(RGWDataSyncEnv *_sync_env,
                     const string& _topic,
                     std::shared_ptr<rgw_pubsub_event>& _event,
                     PSConfigRef _conf) : RGWCoroutine(_sync_env->cct),
                                               sync_env(_sync_env),
                                               topic(_topic),
                                               event(_event),
                                               conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 20) << "PSTopicAddLogEntry: " << sync_env->source_zone << dendl;

      
      yield {
        int shard_id = ++counter % conf->num_topic_shards;
        call(new PSTopicShardAddLogEntry(sync_env, topic, shard_id, event, conf));
      }
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: PSTopicShardAddLogEntry() returned " << retcode << dendl;
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  PSConfigRef conf;
  uint64_t versioned_epoch;
  vector<PSNotificationConfig *> notifs;
  vector<PSNotificationConfig *>::iterator niter;
public:
  RGWPSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          PSConfigRef _conf, uint64_t _versioned_epoch) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key), conf(_conf),
                                                                               versioned_epoch(_versioned_epoch) {
#warning this will need to change obviously
    conf->get_notifs(_bucket_info, _key, &notifs);
  }
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": stat of remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                               << " attrs=" << attrs << dendl;


      for (niter = notifs.begin(); niter != notifs.end(); ++niter) {
        yield {
          ldout(sync_env->cct, 10) << ": notification for " << bucket_info.bucket << "/" << key << ": id=" << (*niter)->id << " path=" << (*niter)->path << ", topic=" << (*niter)->topic << dendl;

#warning publish notification
#if 0
        string path = conf->get_obj_path(bucket_info, key);
        es_obj_metadata doc(sync_env->cct, conf, bucket_info, key, mtime, size, attrs, versioned_epoch);

        call(new RGWPutRESTResourceCR<es_obj_metadata, int>(sync_env->cct, conf->conn.get(),
                                                            sync_env->http_manager,
                                                            path, nullptr /* params */,
                                                            doc, nullptr /* result */));
#endif
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  PSConfigRef conf;
  uint64_t versioned_epoch;
public:
  RGWPSHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        PSConfigRef _conf, uint64_t _versioned_epoch) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           conf(_conf), versioned_epoch(_versioned_epoch) {
  }

  ~RGWPSHandleRemoteObjCR() override {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
#warning things need to change
    /* FIXME: we need to create a pre_callback coroutine that decides whether object should
     * actually be handled. Otherwise we fetch info from remote zone about every object, even
     * if we don't intend to handle it.
     */
    return new RGWPSHandleRemoteObjCBCR(sync_env, bucket_info, key, conf, versioned_epoch);
  }
};

class RGWPSRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  PSConfigRef conf;
public:
  RGWPSRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          PSConfigRef _conf) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": remove remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
#if 0
        string path = conf->get_obj_path(bucket_info, key);

        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf->conn.get(),
                                         sync_env->http_manager,
                                         path, nullptr /* params */));
#endif
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWPSDataSyncModule : public RGWDataSyncModule {
  PSConfigRef conf;
public:
  RGWPSDataSyncModule(CephContext *cct, const JSONFormattable& config) : conf(std::make_shared<PSConfig>()) {
    conf->init(cct, config);
  }
  ~RGWPSDataSyncModule() override {}

  void init(RGWDataSyncEnv *sync_env, uint64_t instance_id) override {
    conf->init_instance(sync_env->store->get_realm(), instance_id);
  }

  RGWCoroutine *init_sync(RGWDataSyncEnv *sync_env) override {
    ldout(sync_env->cct, 5) << conf->id << ": init" << dendl;
    return new RGWPSInitConfigCBCR(sync_env, conf);
  }
  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
#warning this should be done correctly
#if 0
    if (!conf->should_handle_operation(bucket_info)) {
      ldout(sync_env->cct, 10) << conf->id << ": skipping operation (bucket not approved)" << dendl;
      return nullptr;
    }
#endif
    return new RGWPSHandleRemoteObjCR(sync_env, bucket_info, key, conf, versioned_epoch);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    /* versioned and versioned epoch params are useless in the elasticsearch backend case */
    ldout(sync_env->cct, 10) << conf->id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
#warning this should be done correctly
#if 0
    if (!conf->should_handle_operation(bucket_info)) {
      ldout(sync_env->cct, 10) << conf->id << ": skipping operation (bucket not approved)" << dendl;
      return nullptr;
    }
#endif
    return new RGWPSRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, conf);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
#warning requests should be filtered correctly
#if 0
    ldout(sync_env->cct, 10) << conf->id << ": skipping operation (not handled)" << dendl;
#endif
#warning delete markers need to be handled too
    return NULL;
  }
};

RGWPSSyncModuleInstance::RGWPSSyncModuleInstance(CephContext *cct, const JSONFormattable& config)
{
  data_handler = std::unique_ptr<RGWPSDataSyncModule>(new RGWPSDataSyncModule(cct, config));
}

RGWDataSyncModule *RGWPSSyncModuleInstance::get_data_handler()
{
  return data_handler.get();
}

RGWRESTMgr *RGWPSSyncModuleInstance::get_rest_filter(int dialect, RGWRESTMgr *orig) {
#warning REST filter implementation missing
#if 0
  if (dialect != RGW_REST_S3) {
    return orig;
  }
  delete orig;
  return new RGWRESTMgr_MDSearch_S3();
#endif
  return orig;
}

int RGWPSSyncModule::create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) {
  instance->reset(new RGWPSSyncModuleInstance(cct, config));
  return 0;
}

