// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_cache.h"
#include "rgw_rados.h"

#include <errno.h>

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>


#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_op.h"
#include "rgw_common.h"
#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_op.h"

#include <cpp_redis/cpp_redis>

#include <iostream>
#include <fstream> // writing to file
#include <sstream> // writing to memory (a string)
#include <openssl/hmac.h>
#include <ctime>

class RGWGetObj_CB;

#define dout_subsys ceph_subsys_rgw

static const std::string base64_chars =
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz"
"0123456789+/";


static inline bool is_base64(unsigned char c) {
		return (isalnum(c) || (c == '+') || (c == '/'));
}
std::string base64_encode(unsigned char const* bytes_to_encode, unsigned int in_len) {
		std::string ret;
		int i = 0;
		int j = 0;
		unsigned char char_array_3[3];
		unsigned char char_array_4[4];
		while (in_len--) {
				char_array_3[i++] = *(bytes_to_encode++);
				if (i == 3) {
						char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
						char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
						char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
						char_array_4[3] = char_array_3[2] & 0x3f;

						for(i = 0; (i <4) ; i++)
								ret += base64_chars[char_array_4[i]];
						i = 0;
				}
		}

		if (i)
		{
				for(j = i; j < 3; j++)
						char_array_3[j] = '\0';

				char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
				char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
				char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
				char_array_4[3] = char_array_3[2] & 0x3f;

				for (j = 0; (j < i + 1); j++)
						ret += base64_chars[char_array_4[j]];

				while((i++ < 3))
						ret += '=';
		}
		return ret;
}



int ObjectCache::get(const string& name, ObjectCacheInfo& info, uint32_t mask, rgw_cache_entry_info *cache_info)
{
		RWLock::RLocker l(lock);

		if (!enabled) {
				return -ENOENT;
		}

		auto iter = cache_map.find(name);
		if (iter == cache_map.end()) {
				ldout(cct, 10) << "cache get: name=" << name << " : miss" << dendl;
				if (perfcounter)
						perfcounter->inc(l_rgw_cache_miss);
				return -ENOENT;
		}
		if (expiry.count() &&
						(ceph::coarse_mono_clock::now() - iter->second.info.time_added) > expiry) {
				ldout(cct, 10) << "cache get: name=" << name << " : expiry miss" << dendl;
				lock.unlock();
				lock.get_write();
				// check that wasn't already removed by other thread
				iter = cache_map.find(name);
				if (iter != cache_map.end()) {
						for (auto &kv : iter->second.chained_entries)
								kv.first->invalidate(kv.second);
						remove_lru(name, iter->second.lru_iter);
						cache_map.erase(iter);
				}
				if(perfcounter)
						perfcounter->inc(l_rgw_cache_miss);
				return -ENOENT;
		}

		ObjectCacheEntry *entry = &iter->second;

		if (lru_counter - entry->lru_promotion_ts > lru_window) {
				ldout(cct, 20) << "cache get: touching lru, lru_counter=" << lru_counter
						<< " promotion_ts=" << entry->lru_promotion_ts << dendl;
				lock.unlock();
				lock.get_write(); /* promote lock to writer */

				/* need to redo this because entry might have dropped off the cache */
				iter = cache_map.find(name);
				if (iter == cache_map.end()) {
						ldout(cct, 10) << "lost race! cache get: name=" << name << " : miss" << dendl;
						if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
						return -ENOENT;
				}

				entry = &iter->second;
				/* check again, we might have lost a race here */
				if (lru_counter - entry->lru_promotion_ts > lru_window) {
						touch_lru(name, *entry, iter->second.lru_iter);
				}
		}

		ObjectCacheInfo& src = iter->second.info;
		if ((src.flags & mask) != mask) {
				ldout(cct, 10) << "cache get: name=" << name << " : type miss (requested=0x"
						<< std::hex << mask << ", cached=0x" << src.flags
						<< std::dec << ")" << dendl;
				if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
				return -ENOENT;
		}
		ldout(cct, 10) << "cache get: name=" << name << " : hit (requested=0x"
				<< std::hex << mask << ", cached=0x" << src.flags
				<< std::dec << ")" << dendl;

		info = src;
		if (cache_info) {
				cache_info->cache_locator = name;
				cache_info->gen = entry->gen;
		}
		if(perfcounter) perfcounter->inc(l_rgw_cache_hit);

		return 0;
}

bool ObjectCache::chain_cache_entry(std::initializer_list<rgw_cache_entry_info*> cache_info_entries,
				RGWChainedCache::Entry *chained_entry)
{
		RWLock::WLocker l(lock);

		if (!enabled) {
				return false;
		}

		std::vector<ObjectCacheEntry*> entries;
		entries.reserve(cache_info_entries.size());
		/* first verify that all entries are still valid */
		for (auto cache_info : cache_info_entries) {
				ldout(cct, 10) << "chain_cache_entry: cache_locator="
						<< cache_info->cache_locator << dendl;
				auto iter = cache_map.find(cache_info->cache_locator);
				if (iter == cache_map.end()) {
						ldout(cct, 20) << "chain_cache_entry: couldn't find cache locator" << dendl;
						return false;
				}

				auto entry = &iter->second;

				if (entry->gen != cache_info->gen) {
						ldout(cct, 20) << "chain_cache_entry: entry.gen (" << entry->gen
								<< ") != cache_info.gen (" << cache_info->gen << ")"
								<< dendl;
						return false;
				}
				entries.push_back(entry);
		}


		chained_entry->cache->chain_cb(chained_entry->key, chained_entry->data);

		for (auto entry : entries) {
				entry->chained_entries.push_back(make_pair(chained_entry->cache,
										chained_entry->key));
		}

		return true;
}

void ObjectCache::put(const string& name, ObjectCacheInfo& info, rgw_cache_entry_info *cache_info)
{
		RWLock::WLocker l(lock);

		if (!enabled) {
				return;
		}

		ldout(cct, 10) << "cache put: name=" << name << " info.flags=0x"
				<< std::hex << info.flags << std::dec << dendl;

		auto [iter, inserted] = cache_map.emplace(name, ObjectCacheEntry{});
		ObjectCacheEntry& entry = iter->second;
		entry.info.time_added = ceph::coarse_mono_clock::now();
		if (inserted) {
				entry.lru_iter = lru.end();
		}
		ObjectCacheInfo& target = entry.info;

		invalidate_lru(entry);

		entry.chained_entries.clear();
		entry.gen++;

		touch_lru(name, entry, entry.lru_iter);

		target.status = info.status;

		if (info.status < 0) {
				target.flags = 0;
				target.xattrs.clear();
				target.data.clear();
				return;
		}

		if (cache_info) {
				cache_info->cache_locator = name;
				cache_info->gen = entry.gen;
		}

		target.flags |= info.flags;

		if (info.flags & CACHE_FLAG_META)
				target.meta = info.meta;
		else if (!(info.flags & CACHE_FLAG_MODIFY_XATTRS))
				target.flags &= ~CACHE_FLAG_META; // non-meta change should reset meta

		if (info.flags & CACHE_FLAG_XATTRS) {
				target.xattrs = info.xattrs;
				map<string, bufferlist>::iterator iter;
				for (iter = target.xattrs.begin(); iter != target.xattrs.end(); ++iter) {
						ldout(cct, 10) << "updating xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
				}
		} else if (info.flags & CACHE_FLAG_MODIFY_XATTRS) {
				map<string, bufferlist>::iterator iter;
				for (iter = info.rm_xattrs.begin(); iter != info.rm_xattrs.end(); ++iter) {
						ldout(cct, 10) << "removing xattr: name=" << iter->first << dendl;
						target.xattrs.erase(iter->first);
				}
				for (iter = info.xattrs.begin(); iter != info.xattrs.end(); ++iter) {
						ldout(cct, 10) << "appending xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
						target.xattrs[iter->first] = iter->second;
				}
		}

		if (info.flags & CACHE_FLAG_DATA)
				target.data = info.data;

		if (info.flags & CACHE_FLAG_OBJV)
				target.version = info.version;
}

bool ObjectCache::remove(const string& name)
{
		RWLock::WLocker l(lock);

		if (!enabled) {
				return false;
		}

		auto iter = cache_map.find(name);
		if (iter == cache_map.end())
				return false;

		ldout(cct, 10) << "removing " << name << " from cache" << dendl;
		ObjectCacheEntry& entry = iter->second;

		for (auto& kv : entry.chained_entries) {
				kv.first->invalidate(kv.second);
		}

		remove_lru(name, iter->second.lru_iter);
		cache_map.erase(iter);
		return true;
}

void ObjectCache::touch_lru(const string& name, ObjectCacheEntry& entry,
				std::list<string>::iterator& lru_iter)
{
		while (lru_size > (size_t)cct->_conf->rgw_cache_lru_size) {
				auto iter = lru.begin();
				if ((*iter).compare(name) == 0) {
						/*
						 * if the entry we're touching happens to be at the lru end, don't remove it,
						 * lru shrinking can wait for next time
						 */
						break;
				}
				auto map_iter = cache_map.find(*iter);
				ldout(cct, 10) << "removing entry: name=" << *iter << " from cache LRU" << dendl;
				if (map_iter != cache_map.end()) {
						ObjectCacheEntry& entry = map_iter->second;
						invalidate_lru(entry);
						cache_map.erase(map_iter);
				}
				lru.pop_front();
				lru_size--;
		}

		if (lru_iter == lru.end()) {
				lru.push_back(name);
				lru_size++;
				lru_iter--;
				ldout(cct, 10) << "adding " << name << " to cache LRU end" << dendl;
		} else {
				ldout(cct, 10) << "moving " << name << " to cache LRU end" << dendl;
				lru.erase(lru_iter);
				lru.push_back(name);
				lru_iter = lru.end();
				--lru_iter;
		}

		lru_counter++;
		entry.lru_promotion_ts = lru_counter;
}

void ObjectCache::remove_lru(const string& name,
				std::list<string>::iterator& lru_iter)
{
		if (lru_iter == lru.end())
				return;

		lru.erase(lru_iter);
		lru_size--;
		lru_iter = lru.end();
}

void ObjectCache::invalidate_lru(ObjectCacheEntry& entry)
{
		for (auto iter = entry.chained_entries.begin();
						iter != entry.chained_entries.end(); ++iter) {
				RGWChainedCache *chained_cache = iter->first;
				chained_cache->invalidate(iter->second);
		}
}

void ObjectCache::set_enabled(bool status)
{
		RWLock::WLocker l(lock);

		enabled = status;

		if (!enabled) {
				do_invalidate_all();
		}
}

void ObjectCache::invalidate_all()
{
		RWLock::WLocker l(lock);

		do_invalidate_all();
}

void ObjectCache::do_invalidate_all()
{
		cache_map.clear();
		lru.clear();

		lru_size = 0;
		lru_counter = 0;
		lru_window = 0;

		for (auto& cache : chained_cache) {
				cache->invalidate_all();
		}
}

void ObjectCache::chain_cache(RGWChainedCache *cache) {
		RWLock::WLocker l(lock);
		chained_cache.push_back(cache);
}

int cacheAioWriteRequest::create_io(bufferlist& bl, unsigned int len, string oid) {
		std::string location = cct->_conf->rgw_datacache_persistent_path + oid;
		int r = 0;

		cb = new struct aiocb;
		mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
		memset(cb, 0, sizeof(struct aiocb));
		r = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
		if (fd < 0)
		{
				ldout(cct, 0) << "ERROR: create_aio_write_request: open file failed, " << errno << "\tlocation: " << location.c_str() <<dendl;
				goto done;
		}
		cb->aio_fildes = fd;

		data = malloc(len);
		if(!data)
		{
				ldout(cct, 0) << "ERROR: create_aio_write_request: memory allocation failed" << dendl;
				goto close_file;
		}
		cb->aio_buf = data;
		memcpy((void *)data, bl.c_str(), len);
		cb->aio_nbytes = len;
		goto done;	
free_buf:
		cb->aio_buf = NULL;
		free(data);
		data = NULL;

close_file:
		::close(fd);
done:
		return r;
}

DataCache::DataCache ()
		: index(0), lock("DataCache"), cache_lock("DataCache::Mutex"), req_lock("DataCache::req"), eviction_lock("DataCache::EvictionMutex"), cct(NULL), io_type(ASYNC_IO), free_data_cache_size(0), outstanding_write_size (0)
{
		tp = new L2CacheThreadPool(32);
		writecache_tp = new L2CacheThreadPool(32);
}

int DataCache::io_write(bufferlist& bl ,unsigned int len, std::string oid) {

		ChunkDataInfo*  chunk_info = new ChunkDataInfo;

		std::string location = cct->_conf->rgw_datacache_persistent_path + oid; /* replace tmp with the correct path from config file*/
		FILE *cache_file = 0;
		int r = 0;

		cache_file = fopen(location.c_str(),"w+");
		if (cache_file <= 0)
		{
				ldout(cct, 0) << "ERROR: DataCache::open file has return error " << r << dendl;
				return -1;
		}

		r = fwrite(bl.c_str(), 1, len, cache_file);
		if (r < 0) {
				ldout(cct, 0) << "ERROR: DataCache::write: write in file has return error " << r << dendl;
		}

		fclose(cache_file);

		/*update cahce_map entries for new chunk in cache*/
		cache_lock.Lock();
		chunk_info->oid = oid;
		chunk_info->set_ctx(cct);
		chunk_info->size = len;
		cache_map.insert(pair<string, ChunkDataInfo*>(oid, chunk_info));
		cache_lock.Unlock();

		return r;
}

void _cache_aio_write_completion_cb(sigval_t sigval) {

		cacheAioWriteRequest *c = (cacheAioWriteRequest *)sigval.sival_ptr;
		c->priv_data->cache_aio_write_completion_cb(c);
}


void DataCache::cache_aio_write_completion_cb(cacheAioWriteRequest *c){

		ChunkDataInfo  *chunk_info = NULL;

		ldout(cct, 0) << "engage: cache_aio_write_completion_cb oid:" << c->oid <<dendl;

		/*update cahce_map entries for new chunk in cache*/
		cache_lock.Lock();
		outstanding_write_list.remove(c->oid);
		chunk_info = new ChunkDataInfo;
		chunk_info->oid = c->oid;
		chunk_info->set_ctx(cct);
		chunk_info->size = c->cb->aio_nbytes;
		cache_map.insert(pair<string, ChunkDataInfo*>(c->oid, chunk_info));
		cache_lock.Unlock();

		/*update free size*/
		eviction_lock.Lock();
		free_data_cache_size -= c->cb->aio_nbytes;
		outstanding_write_size -=  c->cb->aio_nbytes;
		lru_insert_head(chunk_info);
		eviction_lock.Unlock();
		c->release();
}

int DataCache::create_aio_write_request(bufferlist& bl, unsigned int len, std::string oid){

		struct cacheAioWriteRequest *wr= new struct cacheAioWriteRequest(cct);
		int r=0;
		if (wr->create_io(bl, len, oid) < 0) {
				ldout(cct, 0) << "engage: Error create_aio_write_request" << dendl;
				goto done;
		}
		wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
		wr->cb->aio_sigevent.sigev_notify_function = _cache_aio_write_completion_cb;
		wr->cb->aio_sigevent.sigev_notify_attributes = NULL;
		wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;
		wr->oid = oid;
		wr->priv_data = this;

		if((r= ::aio_write(wr->cb)) != 0) {
				ldout(cct, 0) << "ERROR: aio_write "<< r << dendl;
				goto error;
		}
		return 0;

error:
		wr->release();
done:
		return r;
}
void DataCache::DeleteObjWB(RGWRados *store){

		RGWObjectCtx obj_ctx(store);
		RGWBucketInfo src_bucket_info;
		map<string, bufferlist> src_attrs;
		const string src_tenant_name = "";
		const string src_bucket_name = "mytest";
		const string src_obj_name="file.txt";
		rgw_user user_id("testuser");
		int ret = store->get_bucket_info(obj_ctx, src_tenant_name, src_bucket_name, src_bucket_info, NULL, &src_attrs);
		rgw_obj src_obj(src_bucket_info.bucket, src_obj_name);
		//Check here whether object exists or not
		///* check if obj exists, read orig attrs */
		ldout(cct, 20) << "Engage1: We are in DataCache::remove() obj: " << src_bucket_name << " object "<< src_obj_name <<dendl;	
		RGWRados::Object src_op_target(store, src_bucket_info, obj_ctx, src_obj);
		RGWRados::Object::Read read_op(&src_op_target);
		read_op.params.attrs = &src_attrs;
		ret = read_op.prepare();
		//	RGWObjectCtx *obj_ctx2 = static_cast<RGWObjectCtx *>(obj_ctx);
		obj_ctx.obj.set_atomic(src_obj);
		RGWRados::Object del_target(store, src_bucket_info, obj_ctx, src_obj);
		RGWRados::Object::Delete del_op(&del_target);
		del_op.params.versioning_status = src_bucket_info.versioning_status();
		del_op.params.bucket_owner = src_bucket_info.owner;
		int op_ret = del_op.delete_obj();
		if (op_ret >= 0) {
				//delete_marker = del_op.result.delete_marker;
				return;
		}
}

int DataCache::get_s3_credentials(RGWRados *store, string userid){
		ldout(cct, 20) << "get_s3_ :ugur access_key " << userid <<dendl;
		bufferlist bl;
		RGWObjVersionTracker objv_tracker;
		RGWObjectCtx obj_ctx(store);
		RGWUserInfo info;
		rgw_user user_id("testuser");
		string oid="testuser";
		real_time *pmtime;
		int ret = rgw_get_system_obj(store, obj_ctx, store->get_zone_params().user_uid_pool, oid, bl, &objv_tracker, pmtime, NULL, NULL);
		if (ret == 0){return -1;}
		ret = rgw_get_user_info_by_uid(store, user_id , info, &objv_tracker, pmtime, NULL, NULL);
		if (ret == 0){return -1;}
		map<string, RGWAccessKey>::iterator kiter;
		for (kiter = info.access_keys.begin(); kiter != info.access_keys.end(); ++kiter) {
				RGWAccessKey& k = kiter->second;
				const char *sep = (k.subuser.empty() ? "" : ":");
				const char *subuser = (k.subuser.empty() ? "" : k.subuser.c_str());
				string s;
				info.user_id.to_str(s);
				ldout(cct, 20) << "Engage2:ugur access_key " << k.id <<dendl;
				ldout(cct, 20) << "Engage2:ugur secret_key " << k.key <<dendl;	
		}
		ldout(cct, 20) << "Bitti" <<dendl;	
		return 0;
}

void DataCache::DiscardObjWB(RGWRados *store, string userid){

		//int reti = get_s3_credentials(store, "testuser"); 
		bufferlist bl;
		RGWObjVersionTracker objv_tracker;
		RGWObjectCtx obj_ctx2(store);
		RGWUserInfo info;
		rgw_user user_id(userid);
		string oid = userid;
		real_time *pmtime;
		std::string YourSecretAccessKeyID;
		std::string AWSAccessKeyId;
		int ret = rgw_get_system_obj(store, obj_ctx2, store->get_zone_params().user_uid_pool, oid, bl, &objv_tracker, pmtime, NULL, NULL);
		ret = rgw_get_user_info_by_uid(store, user_id , info, &objv_tracker, pmtime, NULL, NULL);
		map<string, RGWAccessKey>::iterator kiter;
		for (kiter = info.access_keys.begin(); kiter != info.access_keys.end(); ++kiter) {
				RGWAccessKey& k = kiter->second;
				const char *sep = (k.subuser.empty() ? "" : ":");
				const char *subuser = (k.subuser.empty() ? "" : k.subuser.c_str());
				string s;
				info.user_id.to_str(s);
				AWSAccessKeyId = k.id;
				YourSecretAccessKeyID = k.key;
		}

		RGWAccessKey accesskey(AWSAccessKeyId,YourSecretAccessKeyID);
		RGWBucketInfo src_bucket_info;
		RGWBucketInfo dest_bucket_info;
		RGWObjectCtx obj_ctx(store);
		map<string, bufferlist> dest_attrs;
		map<string, bufferlist> src_attrs;
		const string src_tenant_name = "";
		const string src_bucket_name = "mytest";
		const string src_obj_name="file.txt";
		string url = "http://172.10.5.41:80";
		//  string url ="http://" +  cct->_conf->rgw_l2_hosts;
		string etag;
		real_time *mtime;
		HostStyle host_style = PathStyle;
		ret = store->get_bucket_info(obj_ctx, src_tenant_name, src_bucket_name, src_bucket_info, NULL, &src_attrs);
		rgw_obj src_obj(src_bucket_info.bucket, src_obj_name);
		dest_bucket_info = src_bucket_info;
		dest_attrs = src_attrs;
		rgw_bucket dest_bucket = dest_bucket_info.bucket;
		rgw_obj dest_obj(dest_bucket, src_obj_name);
		uint64_t obj_size;

		/*Create Bucket*/
		ldout(cct, 20) << "ugur flush create bucket: " << src_bucket_name << dendl;
		param_vec_t bucket_headers;
		bucket_headers.push_back(pair<string, string>("Content-Length", "0"));
		RGWRESTStreamS3PutObj *bucket_wr = new RGWRESTStreamS3PutObj(cct, "PUT", url, &bucket_headers, NULL, host_style);
		const string s_bucket_name = "";
		map<string, bufferlist> bucket_attrs;
		rgw_obj dest_bucket_obj(dest_bucket, s_bucket_name); 
		ret = bucket_wr->put_obj_init(accesskey, dest_bucket_obj, 0, bucket_attrs, true);
		if (ret < 0) {
				delete bucket_wr;
		}
		ret = bucket_wr->complete_request(&etag, nullptr);

		/*Create Object*/
		ldout(cct, 20) << "ugur flush object: " << src_obj_name << dendl;
		RGWRados::Object src_op_target(store, src_bucket_info, obj_ctx, src_obj);
		RGWRados::Object::Read read_op(&src_op_target); 
		read_op.params.attrs = &src_attrs;
		read_op.params.obj_size = &obj_size;
		ret = read_op.prepare(); 
		if (ret < 0) { return;}

		RGWObjState *astate = NULL;
		ret = store->get_obj_state(&obj_ctx, src_bucket_info, src_obj, &astate, NULL);
		param_vec_t headers;
		headers.push_back(pair<string, string>("Content-Length", std::to_string(astate->size)));
		RGWRESTStreamS3PutObj *wr = new RGWRESTStreamS3PutObj(cct, "PUT", url, &headers, NULL, host_style);
		ret = wr->put_obj_init(accesskey, dest_obj, astate->size, src_attrs, true);
		if (ret < 0) {
				delete wr;
		}

		ret = read_op.iterate(0, astate->size - 1, wr->get_out_cb());
		if (ret < 0) {
				delete wr;
		}
		ret = wr->complete_request(&etag, nullptr);

}

void DataCache::put(bufferlist& bl, unsigned int len, std::string oid){

		int r = 0;
		long long freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;

		ldout(cct, 20) << "Engage1: We are in DataCache::put() and oid is: " << oid <<dendl;
		cache_lock.Lock();
		map<string, ChunkDataInfo *>::iterator iter = cache_map.find(oid);
		if (iter != cache_map.end()) {
				cache_lock.Unlock();
				ldout(cct, 10) << "Engage1: Warning: data already cached, no rewirte" << dendl;
				return;
		}
		std::list<std::string>::iterator it = std::find(outstanding_write_list.begin(), outstanding_write_list.end(),oid);
		if (it != outstanding_write_list.end()) {
				cache_lock.Unlock();
				ldout(cct, 10) << "Engage1: Warning: data put in cache already issued, no rewrite" << dendl;
				return;
		}
		outstanding_write_list.push_back(oid);
		cache_lock.Unlock();

		eviction_lock.Lock();
		_free_data_cache_size = free_data_cache_size;
		_outstanding_write_size = outstanding_write_size;
		eviction_lock.Unlock();

		ldout(cct, 20) << "Engage1: Before eviction _free_data_cache_size:" << _free_data_cache_size << ", _outstanding_write_size:" << _outstanding_write_size << "freed_size:" << freed_size << dendl;
		while (len >= (_free_data_cache_size - _outstanding_write_size + freed_size)){
				ldout(cct, 20) << "Engage1: enter eviction, r=" << r << dendl;
				r = lru_eviction();
				if(r < 0)
						return;
				freed_size += r;
		}
		r = create_aio_write_request(bl, len, oid);
		if (r < 0) {
				cache_lock.Lock();
				outstanding_write_list.remove(oid);
				cache_lock.Unlock();
				ldout(cct, 1) << "Engage1: create_aio_wirte_request fail, r=" << r << dendl;
				return;
		}

		eviction_lock.Lock();
		free_data_cache_size += freed_size;
		outstanding_write_size += len;
		eviction_lock.Unlock();
}

bool DataCache::get(string oid) { 

		bool exist = false;
		string location = cct->_conf->rgw_datacache_persistent_path + oid; 
		cache_lock.Lock();
		map<string, ChunkDataInfo*>::iterator iter = cache_map.find(oid);
		if (!(iter == cache_map.end())){
				// check inside cache whether file exists or not!!!! then make exist true;
				struct ChunkDataInfo *chdo = iter->second;
				if(access(location.c_str(), F_OK ) != -1 ) { // file exists
						exist = true;
						{ /*LRU*/

								/*get ChunkDataInfo*/
								eviction_lock.Lock();
								lru_remove(chdo);
								lru_insert_head(chdo);
								eviction_lock.Unlock();
						}
				} else {	
						cache_map.erase(oid);
						lru_remove(chdo);
						exist = false;
				}
		}
		cache_lock.Unlock();
		return exist;
}

size_t DataCache::random_eviction(){

		int n_entries = 0;
		int random_index = 0;
		size_t freed_size = 0;
		ChunkDataInfo *del_entry;
		string del_oid, location;

		cache_lock.Lock();
		n_entries = cache_map.size();
		if (n_entries <= 0){
				cache_lock.Unlock();
				return -1;
		}
		srand (time(NULL));
		random_index = rand()%n_entries;
		map<string, ChunkDataInfo*>::iterator iter = cache_map.begin();
		std::advance(iter, random_index);
		del_oid = iter->first;
		del_entry =  iter->second;
		ldout(cct, 20) << "INFO::random_eviction index:"<< random_index << ", free size:0x" << std::hex << del_entry->size << dendl;
		freed_size = del_entry->size;
		free(del_entry);
		del_entry = NULL;
		cache_map.erase(del_oid); // oid
		cache_lock.Unlock();

		location = cct->_conf->rgw_datacache_persistent_path + del_oid; /*replace tmp with the correct path from config file*/
		remove(location.c_str());
		return freed_size;
}

size_t DataCache::lru_eviction(){

		int n_entries = 0;
		//  int random_index = 0;
		size_t freed_size = 0;
		ChunkDataInfo *del_entry;
		string del_oid, location;

		eviction_lock.Lock();
		del_entry = tail;
		lru_remove(del_entry);
		eviction_lock.Unlock();

		cache_lock.Lock();
		n_entries = cache_map.size();
		if (n_entries <= 0){
				cache_lock.Unlock();
				return -1;
		}
		del_oid = del_entry->oid;
		ldout(cct, 20) << "Engage1: lru_eviction: oid to remove" << del_oid << dendl;
		map<string, ChunkDataInfo*>::iterator iter = cache_map.find(del_entry->oid);
		if (iter != cache_map.end()) {
				cache_map.erase(del_oid); // oid
		}
		cache_lock.Unlock();
		freed_size = del_entry->size;
		free(del_entry);
		location = cct->_conf->rgw_datacache_persistent_path + del_oid; /*replace tmp with the correct path from config file*/
		remove(location.c_str());
		return freed_size;
}


void DataCache::remote_io(struct librados::L2CacheRequest *l2request ) {
		ldout(cct, 20) << "Engage1: Add task to remote IO" << dendl;
		tp->addTask(new HttpL2Request(l2request, cct));
}


std::vector<string> split(const std::string &s, char * delim) {
		stringstream ss(s);
		std::string item;
		std::vector<string> tokens;
		while (getline(ss, item, (*delim))) {
				tokens.push_back(item);
		}
		return tokens;
}

void DataCache::push_l2_request(struct librados::L2CacheRequest *l2request ) {
		tp->addTask(new HttpL2Request(l2request, cct));
}

static size_t _l2_response_cb(void *ptr, size_t size, size_t nmemb, void* param) {
		librados::L2CacheRequest *req = static_cast<librados::L2CacheRequest *>(param);
		req->pbl->append((char *)ptr, size*nmemb);
		return size*nmemb;
}
/*Ugur*/
void DataCache::push_wb_request(struct librados::L2CacheRequest *wbrequest ) {
		tp->addTask(new HttpL2Request(wbrequest, cct));
}


void HttpL2Request::run() {

		ldout(cct, 10) << "ugur HTTP RUN req_wb value "<< dendl;
		if (req->op == "PUT"){
				ldout(cct, 10) << "ugur PUT HTTP SUBMIT " << dendl;
				int n_retries =  cct->_conf->rgw_l2_request_thread_num;
				int r = 0;
				for (int i=0; i<n_retries; i++ ){
						if(!(r = submit_http_put_request())){
								req->finish();
								return;
						}
				}}
		if (req->op == "read"){  
				ldout(cct, 10) << "ugur GET HTTP SUBMIT " << dendl;
				get_obj_data *d = (get_obj_data *)req->op_data;
				int n_retries =  cct->_conf->rgw_l2_request_thread_num;
				int r = 0;
				for (int i=0; i<n_retries; i++ ){
						if(!(r = submit_http_get_request_s3())){
								d->cache_aio_completion_cb(req);
								return;}
						if (r == ECANCELED) {return;}
				}
		}
}
static size_t _l2_writeback_cb(void *data, size_t size, size_t nmemb, void *cb_data)
{
		size_t nread = size*nmemb;
		librados::L2CacheRequest *req = (librados::L2CacheRequest *)cb_data;
		nread = (req->len < (req->read_ofs + nread)) ? (req->len - req->read_ofs) : nread;
		memcpy(data, req->bl.c_str() + req->read_ofs, nread);
		req->read_ofs += nread;
		return nread;
}

string HttpL2Request::get_date(){
		std::string zone=" GMT";
		time_t now = time(0);
		char* dt = ctime(&now);
		tm *gmtm = gmtime(&now);
		dt = asctime(gmtm);
		std::string date(dt);
		char buffer[80];
		std::strftime(buffer,80,"%a, %d %b %Y %X %Z",gmtm);
		puts(buffer);
		date = buffer;
		return date;
}
string HttpL2Request::sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId){
		std::string Content_Type = "application/x-www-form-urlencoded; charset=utf-8";
		std::string Content_MD5 ="";	
		std::string CanonicalizedResource = uri.c_str();
		std::string StringToSign = HTTP_Verb + "\n" + Content_MD5 + "\n" + Content_Type + "\n" + date + "\n" +CanonicalizedResource;
		char key[YourSecretAccessKeyID.length()+1] ;
		strcpy(key, YourSecretAccessKeyID.c_str()); 
		const char * data = StringToSign.c_str();
		unsigned char* digest;
		digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
		std::string signature = base64_encode(digest, 20);
		return signature;	
}

int HttpL2Request::submit_http_get_request_s3() {
		string range = std::to_string(req->ofs + req->read_ofs)+ "-"+ std::to_string(req->ofs + req->read_ofs + req->len - 1);
		get_obj_data *d = (get_obj_data *)req->op_data;
		string auth_token;
		string req_uri, uri,dest;
		if (req->dest == cct->_conf->rgw_wb_cache_location){
				std::string YourSecretAccessKeyID=cct->_conf->rgw_wb_cache_secret_key;
				std::string AWSAccessKeyId=cct->_conf->rgw_wb_cache_access_key;
				uri ="/wb_cache/"+req->oid;
		} else {
				((RGWGetObj_CB *)(d->client_cb))->get_req_info(dest, req_uri, auth_token);
				std::string YourSecretAccessKeyID="";
				std::string AWSAccessKeyId="";
				uri = req_uri;
		}
		CURLcode res;
		//	std::string uri = "/wb_cache/"+req->oid;
		std::string date = get_date();
		std::string AWSAccessKeyId=cct->_conf->rgw_wb_cache_access_key;	
		std::string YourSecretAccessKeyID=cct->_conf->rgw_wb_cache_secret_key;	
		std::string signature = sign_s3_request("GET", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
		std::string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
		std::string  loc = "http://" + req->dest + uri;		
		//	std::string header = "Authorization: " +Authorization+  ", -H Date:" + date +", -H User-Agent: aws-sdk-java/1.7.4 Linux/3.10.0-514.6.1.el7.x86_64 OpenJDK_64-Bit_Server_VM/24.131-b00/1.7.0_131, -H Content-Type: application/x-www-form-urlencoded; charset=utf-8";
		std::string auth="Authorization: " + Authorization;
		std::string timestamp="Date: " + date;
		std::string user_agent="User-Agent: aws-sdk-java/1.7.4 Linux/3.10.0-514.6.1.el7.x86_64 OpenJDK_64-Bit_Server_VM/24.131-b00/1.7.0_131";
		std::string content_type="Content-Type: application/x-www-form-urlencoded; charset=utf-8";
		curl_handle = curl_easy_init();
		if(curl_handle) {
				struct curl_slist *chunk = NULL;
				chunk = curl_slist_append(chunk, auth.c_str());
				chunk = curl_slist_append(chunk, timestamp.c_str());
				chunk = curl_slist_append(chunk, user_agent.c_str());
				chunk = curl_slist_append(chunk, content_type.c_str());
				if(req->dest != cct->_conf->rgw_wb_cache_location){
						curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
				}
				res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
				curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
				curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _l2_response_cb);
				curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
				res = curl_easy_perform(curl_handle); //run the curl command
				curl_easy_reset(curl_handle);
				curl_slist_free_all(chunk);	
		}if(res != CURLE_OK){
				ldout(cct,10) << "Ugur: curl_easy_perform() failed " << curl_easy_strerror(res) << " oid " << req->oid << dendl;
				return -1;
		}else{return 0;}
}

string HttpL2Request::sign_s3_request2(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId, string len){
		std::string Content_Type = "text/plain";
		std::string Content_MD5 ="";
		std::string Content_Length = len;	
		std::string CanonicalizedResource = uri.c_str();
		std::string StringToSign = HTTP_Verb + "\n" + Content_MD5 + "\n" + Content_Type + "\n" + Content_Length +"\n"+ date + "\n" +CanonicalizedResource;
		char key[YourSecretAccessKeyID.length()+1] ;
		strcpy(key, YourSecretAccessKeyID.c_str()); 
		const char * data = StringToSign.c_str();
		unsigned char* digest;
		digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
		std::string signature = base64_encode(digest, 20);
		return signature;	
}
int HttpL2Request::submit_http_put_request_s3() {
		//string auth_token;
		// string req_uri, uri,dest;
		CURLcode res;
		std::string date = get_date();
		std::string uri ="/wb_cache/"+req->oid;
		std::string AWSAccessKeyId=cct->_conf->rgw_wb_cache_access_key;
		std::string YourSecretAccessKeyID=cct->_conf->rgw_wb_cache_secret_key;
		std::string signature = sign_s3_request2("PUT", uri, date, YourSecretAccessKeyID, AWSAccessKeyId, std::to_string(req->len));
		std::string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
		std::string  loc = "http://" + req->dest + uri;	
		std::string auth = "Authorization: " + Authorization;
		std::string timestamp="Date: " + date;
		//    std::string user_agent="User-Agent: aws-sdk-java/1.7.4 Linux/3.10.0-514.6.1.el7.x86_64 OpenJDK_64-Bit_Server_VM/24.131-b00/1.7.0_131";
		std::string content_length = "Content-Length: " +std::to_string(req->len); 
		std::string content_type="Content-Type: text/plain";
		librados::L2CacheRequest *l2_req = (librados::L2CacheRequest *)req;
		curl_handle = curl_easy_init();
		long response_code;
		if(curl_handle) {
				struct curl_slist *chunk = NULL;
				chunk = curl_slist_append(chunk, auth.c_str());
				chunk = curl_slist_append(chunk, timestamp.c_str());
				chunk = curl_slist_append(chunk, content_length.c_str());
				chunk = curl_slist_append(chunk, content_type.c_str());
				res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk);
				curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_READFUNCTION, _l2_writeback_cb);
				curl_easy_setopt(curl_handle, CURLOPT_READDATA, req);
				curl_easy_setopt(curl_handle, CURLOPT_PUT, 1L);
				curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
				curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
				curl_easy_setopt(curl_handle, CURLOPT_INFILESIZE_LARGE, (curl_off_t)l2_req->len);
				res = curl_easy_perform(curl_handle);

				curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &response_code);
				curl_easy_reset(curl_handle);
				curl_slist_free_all(chunk);
		}
		if (res == CURLE_OK){
				return 0;
		}else {
				ldout(cct, 10) << "Engage1: curl_easy_perform() failed " << response_code<<dendl;
				return -1;
		}	
}
int HttpL2Request::submit_http_request() {
		CURLcode res;
		string auth_token;
		string range = std::to_string(req->ofs + req->read_ofs)+ "-"+ std::to_string(req->ofs + req->read_ofs + req->len - 1);
		get_obj_data *d = (get_obj_data *)req->op_data;

		string req_uri, uri,dest;
		if (req->dest == cct->_conf->rgw_wb_cache_location){
				uri ="http://" + req->dest+"/swift/v1/wb_cache/"+req->oid;
				auth_token="X-Auth-Token: AUTH_rgwtk0e00000074657374757365723a7377696674702b3837190921e73571175e9c34072fe3015177bc3a5e8db688fd6b609df3cb557091c2";
		} else { 
				((RGWGetObj_CB *)(d->client_cb))->get_req_info(dest, req_uri, auth_token);
				uri = "http://" + req->dest + req_uri;
		}
		ldout(cct, 10) << "ugur uri " << uri  << " auth "<< auth_token <<dendl;
		/*struct req_state *s;
		  ((RGWGetObj_CB *)(d->client_cb))->get_req_info(req->dest, s->info.request_uri, auth_token);

		  if (s->dialect == "s3") {
		  RGWEnv env;
		  req_info info(cct, &env);
		  memcpy(&info, &s->info, sizeof(info));
		  memcpy(&env, s->info.env, sizeof(env));
		  info.env = &env;
		  std::string access_key_id;
		  if (true) { //(!s->http_auth || !(*s->http_auth)) { FIXME: The S3 Authentication has been changed! FIX IT
		  access_key_id = s->info.args.get("AWSAccessKeyId");
		  } else {
		//string auth_str(s->http_auth + 4); FIXME: The S3 Authentication has been changed! FIX IT
		string auth_str("auth");
		int pos = auth_str.rfind(':');
		if (pos < 0)
		return -EINVAL;
		access_key_id = auth_str.substr(0, pos);
		}
		map<string, RGWAccessKey>::iterator iter = s->user->access_keys.find(access_key_id);
		if (iter == s->user->access_keys.end()) {
		ldout(cct, 1) << "ERROR: access key not encoded in user info" << dendl;
		return -EPERM;
		}
		RGWAccessKey& key = iter->second;
		sign_request(key, env, info);
		} 
		else if (s->dialect == "swift")*/
		struct curl_slist *header = NULL;
		header = curl_slist_append(header, auth_token.c_str());
		curl_handle = curl_easy_init();
		if(curl_handle) {
				if(req->dest != cct->_conf->rgw_wb_cache_location){
						curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
						ldout(cct, 10) << "ugur range :  "<< range.c_str() << dendl;
				}
				curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, header); 
				curl_easy_setopt(curl_handle, CURLOPT_HTTPAUTH, auth_token.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_URL, uri.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); 
				curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
				curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _l2_response_cb);
				curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
				res = curl_easy_perform(curl_handle); 
				curl_easy_reset(curl_handle);
				curl_slist_free_all(header);
		}
		if(res == CURLE_OK) {
				return 0;
		} else {
				ldout(cct, 10) << "Engage1: curl_easy_perform() failed " << curl_easy_strerror(res) << " oid " << req->oid << " offset " << req->ofs + req->read_ofs  <<dendl;
				return -1;
		}

}

/*FIXME: This function should be changed the authentication for S3 has been changed*/
int HttpL2Request::sign_request(RGWAccessKey& key, RGWEnv& env, req_info& info)
{
		// don't sign if no key is provided 
		if (key.key.empty()) {
				return 0;
		}

		if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
				for (const auto& i: env.get_map()) {
						//FIXME: ditto ldout(cct, 20) << "> " << i.first << " -> " << rgw::crypt_sanitize::x_meta_map{i.first, i.second} << dendl; 
				}
		}

		std::string canonical_header;
		if (!rgw_create_s3_canonical_header(info, NULL, canonical_header, false)) {
				ldout(cct, 0) << "failed to create canonical s3 header" << dendl;
				return -EINVAL;
		}

		ldout(cct, 20) << "generated canonical header: " << canonical_header << dendl;

		std::string digest;
		int ret = 0; // FIXME ditto Commented rgw_get_s3_header_digest(canonical_header, key.key, digest);
		if (ret < 0) {
				return ret;
		}

		string auth_hdr = "AWS " + key.id + ":" + digest;
		ldout(cct, 15) << "generated auth header: " << auth_hdr << dendl;

		env.set("AUTHORIZATION", auth_hdr);

		return 0;
}

std::string DataCache::get_key(string key, bool wb_cache){

		if(!wb_cache){
				cpp_redis::client client;
				client.connect();
				cpp_redis::reply answer;
				client.send({"smembers", key}, [&answer](cpp_redis::reply &reply) {
								answer = std::move(reply);
								});
				client.sync_commit();
				vector<string> list;	
				for(auto &temp : answer.as_array()) {
						list.push_back(std::move(temp.as_string()));
				}
				if (list.empty()){return "";}
				else{return list[rand() % list.size()];}
		}else{
				std::size_t found = key.find_last_of("_");
				std:: string prefix = key.substr(0,found)+"_";
				ldout(cct, 10) << "ugur check wb_cache key " << key << " prefix "<< prefix << dendl;	
				cpp_redis::client client;
				client.connect();
				cpp_redis::reply answer;
				client.get(prefix, [&answer](cpp_redis::reply& reply) {
								answer = std::move(reply);
								});
				client.sync_commit();
				if (answer.is_string()){return answer.as_string();}
				else{return "";}
		}
}



int DataCache::set_key(string key, string location, bool wb_cache){
		if(wb_cache){
				cpp_redis::client client;
				client.connect();
				location=rgw_wb_cache_location;
				client.set(key, location,[] (cpp_redis::reply& reply){
								if (reply.is_string() && reply.as_string() == "OK"){return 0;}
								else {return -1;}
								});
				client.sync_commit();
		}else{
				cpp_redis::client client;
				client.connect();	
				vector<string> list;
				list.push_back(location);
				client.sadd(key, list,[] (cpp_redis::reply& reply){
								if (reply.is_string() && reply.as_string() == "OK"){return  0;}
								else {return -1;}
								});
				client.sync_commit();
		}
}

int DataCache::remove_value(string key, string location){
		cpp_redis::client client;
		client.connect();
		cpp_redis::reply answer;
		client.send({"srem", key, location}, [&answer](cpp_redis::reply &reply) {
						answer = std::move(reply);
						});
		client.sync_commit();
		if (answer.is_string() && answer.as_string() == "OK"){
				return 0;
		}else {
				return -1;
		}
}

std::string DataCache::get_s3_key(string key){
		cpp_redis::client client;
		std::size_t found = key.find_last_of("_");
		std:: string prefix = key.substr(0,found) + "*";
		client.connect();
		cpp_redis::reply answer;
		client.send({"keys", prefix}, [&answer](cpp_redis::reply &reply) {
						answer = std::move(reply);
						});
		client.sync_commit();
		if (answer.is_string()){
				return answer.as_string();
		}else if (answer.is_array()){
				std:: string pattern = "";
				for(auto &temp : answer.as_array()) {
						pattern = pattern + " " + temp.as_string();
				}
				return pattern;
		}else {
				return "";
		}
}

int DataCache::remove_s3_key(string keys){
		cpp_redis::client client;
		client.connect();
		cpp_redis::reply answer;
		client.send({"del", keys}, [&answer](cpp_redis::reply &reply) {
						answer = std::move(reply);
						});
		client.sync_commit();
		if (answer.is_string() && answer.as_string() == "OK"){
				return 0;
		}else {
				return -1;
		}

}


/*ugur*/
int HttpL2Request::submit_http_put_request() {

		string bucket="wb_cache/";
		string req_uri = "/swift/v1/wb_cache/"+req->oid;
		req->dest =  cct->_conf->rgw_wb_cache_location;
		string uri = "http://" + req->dest + req_uri;

		CURLcode res;
		string auth_token="X-Auth-Token: AUTH_rgwtk0e00000074657374757365723a7377696674702b3837190921e73571175e9c34072fe3015177bc3a5e8db688fd6b609df3cb557091c2";
		struct curl_slist *header = NULL;
		curl_handle = curl_easy_init();

		librados::L2CacheRequest *l2_req = (librados::L2CacheRequest *)req;
		if(curl_handle) {
				header = curl_slist_append(header, auth_token.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, header);
				curl_easy_setopt(curl_handle, CURLOPT_READFUNCTION, _l2_writeback_cb);
				curl_easy_setopt(curl_handle, CURLOPT_READDATA, req);
				curl_easy_setopt(curl_handle, CURLOPT_PUT, 1L);
				curl_easy_setopt(curl_handle, CURLOPT_URL, uri.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
				curl_easy_setopt(curl_handle, CURLOPT_HTTPAUTH, auth_token.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
				curl_easy_setopt(curl_handle, CURLOPT_INFILESIZE_LARGE, (curl_off_t)l2_req->len);

				res = curl_easy_perform(curl_handle);
				curl_easy_reset(curl_handle);
				curl_slist_free_all(header);
		}
		if (res == CURLE_OK){
				return 0;
		}else { 
				ldout(cct, 10) << "Engage1: curl_easy_perform() failed " << dendl;
				return -1;
		}
}

static size_t _copy_cb(void *data, size_t size, size_t nmemb, void *cb_data)
{
		size_t nread = size*nmemb;
		librados::L2CacheRequest *req = (librados::L2CacheRequest *)cb_data;
		nread = (req->len < (req->read_ofs + nread)) ? (req->len - req->read_ofs) : nread;
		memcpy(data, req->bl.c_str() + req->read_ofs, nread);
		req->read_ofs += nread;
		return nread;
}

int HttpL2Request::submit_http_copy_request() {

		string bucket="wb_cache/";
		string req_uri = "/swift/v1/wb_cache/myfile";
		string dest = "172.10.5.41:80";
		//  req->dest =  cct->_conf->rgw_wb_cache_location;
		string uri = "http://" + dest + req_uri;

		CURLcode res;
		string auth_token="X-Auth-Token: AUTH_rgwtk0e00000074657374757365723a73776966743496a32208d84fb8c1e9185ecd8f042a487bc333ddbb96a5433521a9d34c0890ca5090d5";
		struct curl_slist *header = NULL;
		curl_handle = curl_easy_init();

		librados::L2CacheRequest *l2_req = (librados::L2CacheRequest *)req; 
		if(curl_handle) {
				curl_off_t uploadsize = 20971520;
				header = curl_slist_append(header, auth_token.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, header);
				curl_easy_setopt(curl_handle, CURLOPT_READFUNCTION, _copy_cb);
				curl_easy_setopt(curl_handle, CURLOPT_READDATA, req);
				curl_easy_setopt(curl_handle, CURLOPT_PUT, 1L);
				curl_easy_setopt(curl_handle, CURLOPT_URL, uri.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
				curl_easy_setopt(curl_handle, CURLOPT_HTTPAUTH, auth_token.c_str());
				curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
				curl_easy_setopt(curl_handle, CURLOPT_INFILESIZE_LARGE, uploadsize);


				res = curl_easy_perform(curl_handle);
				curl_easy_reset(curl_handle);
				curl_slist_free_all(header);
		}
		if (res == CURLE_OK){
				return 0;
		}else { 
				ldout(cct, 10) << "Engage1: curl_easy_perform() failed " << dendl;
				return -1;
		}
}

/*
   int DataCache::test_librados_handler(){
   int ret = 0;
   const char *pool_name = "ugur_pool2";
   std::string hello("hello world!");
   std::string object_name("hello_object2");
   librados::IoCtx io_ctx;
   librados::Rados rados;

   {
   ret = rados.init("admin"); // just use the client.admin keyring
   if (ret < 0) { // let's handle any error that might have come back
   ldout(cct, 15)  << "couldn't initialize rados! error " << ret << dendl;
   ret = EXIT_FAILURE;
   }
   ldout(cct, 15)  << "we just set up a rados cluster object " << ret << dendl;
   }

   ret = rados.conf_read_file("/etc/ceph/ceph.conf");
   if (ret < 0) {
   ldout(cct, 0) <<"error" << dendl;
   }

   {
   ret = rados.connect();
   if (ret < 0) {
//  std::cerr << "couldn't connect to cluster! error " << ret << std::endl;
ret = EXIT_FAILURE;
}
ldout(cct, 15)  << "we just connected to the rados cluster " << ret << dendl;
}


{
ret = rados.pool_create(pool_name);
if (ret < 0) {
//    std::cerr << "couldn't create pool! error " << ret << std::endl;
rados.shutdown();
}
}
}



*/

