
#include "rgw_rados.h"
#include "rgw_directory.h"
#include <cpp_redis/cpp_redis>

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include <iostream>
#include <fstream> // writing to file
#include <sstream> // writing to memory (a string)
#include <openssl/hmac.h>
#include <ctime>


std::string DataCache::getValue(string key){

	cpp_redis::client client;
	client.connect();
	cpp_redis::reply answer;
	vector<string> list;	

	client.send({"get", key}, [&answer](cpp_redis::reply &reply) {
				answer = std::move(reply);
	});
	client.sync_commit();

	for(auto &temp : answer.as_array()) {
		list.push_back(std::move(temp.as_string()));
	}

	if (list.empty()){
		return "";
	}
	else{
		return list;
	}
}

std::string DataCache::setValue(string key, string Value){

	cpp_redis::client client;
	client.connect();
	vector<string> list;	
	cpp_redis::reply answer;

	client.send({"set", key, value}, [&answer](cpp_redis::reply &reply) {
		answer = std::move(reply);
	});
	client.sync_commit();
	if (answer.is_string() && answer.as_string() == "OK"){
		return 0;
	}
	else {
		return -1;
	}
}





int DataCache::delKey(string key){
	cpp_redis::client client;
	client.connect();
	cpp_redis::reply answer;
	client.send({"del", key, [&answer](cpp_redis::reply &reply) {
		answer = std::move(reply);
	});
	client.sync_commit();
	if (answer.is_string() && answer.as_string() == "OK"){
		return 0;
	}else {
		return -1;
	}
}




