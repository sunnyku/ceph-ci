#include <gtest/gtest.h>
#include "common/ceph_context.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_process.h"
#include "rgw/rgw_sal_rados.h"
#include "rgw/rgw_lua_s3.h"

using namespace rgw;

class CctCleaner {
  CephContext* cct;
public:
  CctCleaner(CephContext* _cct) : cct(_cct) {}
  ~CctCleaner() { 
#ifdef WITH_SEASTAR
    delete cct; 
#else
    cct->put(); 
#endif
  }
};

class TestRGWUser : public sal::RGWUser {
public:
  virtual int list_buckets(const string&, const string&, uint64_t, bool, sal::RGWBucketList&) override {
    return 0;
  }

  virtual sal::RGWBucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time) override {
    return nullptr;
  }

  virtual int load_by_id(optional_yield y) override {
    return 0;
  }

  virtual ~TestRGWUser() = default;
};

class TestAccounter : public io::Accounter, public io::BasicClient {
  RGWEnv env;

protected:
  virtual int init_env(CephContext *cct) override {
    return 0;
  }

public:
  ~TestAccounter() = default;

  virtual void set_account(bool enabled) override {
  }

  virtual uint64_t get_bytes_sent() const override {
    return 0;
  }

  virtual uint64_t get_bytes_received() const override {
    return 0;
  }
  
  virtual RGWEnv& get_env() noexcept override {
    return env;
  }
  
  virtual size_t complete_request() override {
    return 0;
  }
};

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

CctCleaner cleaner(cct);

TEST(TestRGWLua, EmptyScript)
{
  const std::string script;

  RGWEnv e;
  uint64_t id = 0;
  req_state s(cct, &e, id); 

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

#define DEFINE_REQ_STATE RGWEnv e; req_state s(cct, &e, 0);

TEST(TestRGWLua, SyntaxError)
{
  const std::string script = R"(
    if 3 < 5 then
      RGWDebugLog("missing 'end'")
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, Hello)
{
  const std::string script = R"(
    RGWDebugLog("hello from lua")
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, URI)
{
  const std::string script = R"(
    msg = "URI is: " .. S3Request.DecodedURI
    RGWDebugLog(msg)
    print(msg)
  )";

  DEFINE_REQ_STATE;
  s.decoded_uri = "http://hello.world/";

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Response)
{
  const std::string script = R"(
    print(S3Request.Response.Message)
    print(S3Request.Response.HTTPStatus)
    print(S3Request.Response.RGWCode)
    print(S3Request.Response.HTTPStatusCode)
  )";

  DEFINE_REQ_STATE;
  s.err.http_ret = 400;
  s.err.ret = 4000;
  s.err.err_code = "Bad Request";
  s.err.message = "This is a bad request";

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, SetResponse)
{
  const std::string script = R"(
    print(S3Request.Response.Message)
    S3Request.Response.Message = "this is a good request"
    print(S3Request.Response.Message)
  )";

  DEFINE_REQ_STATE;
  s.err.message = "this is a bad request";

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, SetRGWId)
{
  const std::string script = R"(
    print(S3Request.RGWId)
    S3Request.RGWId = "bar"
    print(S3Request.RGWId)
  )";

  DEFINE_REQ_STATE;
  s.host_id = "foo";

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLua, InvalidField)
{
  const std::string script = R"(
    RGWDebugLog(S3Request.Kaboom)
  )";

  DEFINE_REQ_STATE;
  s.host_id = "foo";

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, InvalidSubField)
{
  const std::string script = R"(
    RGWDebugLog(S3Request.Error.Kaboom)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, Bucket)
{
  const std::string script = R"(
    if S3Request.Bucket then
      msg = "Bucket Id: " .. S3Request.Bucket.Id
      RGWDebugLog(msg)
      print(msg)
      print("Bucket Marker: " .. S3Request.Bucket.Marker)
      print("Bucket Name: " .. S3Request.Bucket.Name)
      print("Bucket Tenant: " .. S3Request.Bucket.Tenant)
      print("Bucket Count: " .. S3Request.Bucket.Count)
      print("Bucket Size: " .. S3Request.Bucket.Size)
      print("Bucket ZoneGroupId: " .. S3Request.Bucket.ZoneGroupId)
      print("Bucket Creation Time: " .. S3Request.Bucket.CreationTime)
      print("Bucket MTime: " .. S3Request.Bucket.MTime)
      print("Bucket Quota Max Size: " .. S3Request.Bucket.Quota.MaxSize)
      print("Bucket Quota Max Objects: " .. S3Request.Bucket.Quota.MaxObjects)
      print("Bucket Quota Enabled: " .. tostring(S3Request.Bucket.Quota.Enabled))
      print("Bucket Quota Rounded: " .. tostring(S3Request.Bucket.Quota.Rounded))
      print("Bucket User Id: " .. S3Request.Bucket.User.Id)
      print("Bucket User Tenant: " .. S3Request.Bucket.User.Tenant)
    else
      print("No bucket")
    end
  )";

  DEFINE_REQ_STATE;

  rgw_bucket b;
  b.tenant = "mytenant";
  b.name = "myname";
  b.marker = "mymarker";
  b.bucket_id = "myid"; 
  s.bucket.reset(new sal::RGWRadosBucket(nullptr, b));

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, GenericAttributes)
{
  const std::string script = R"(
    print("hello  = " .. (S3Request.GenericAttributes["hello"] or "nil"))
    print("foo    = " .. (S3Request.GenericAttributes["foo"] or "nil"))
    print("kaboom = " .. (S3Request.GenericAttributes["kaboom"] or "nil"))
    print("number of attributes is: " .. #S3Request.GenericAttributes)
    for k, v in pairs(S3Request.GenericAttributes) do
      print("key=" .. k .. ", " .. "value=" .. v)
    end
  )";

  DEFINE_REQ_STATE;
  s.generic_attrs["hello"] = "world";
  s.generic_attrs["foo"] = "bar";
  s.generic_attrs["goodbye"] = "cruel world";
  s.generic_attrs["ka"] = "boom";

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Environment)
{
  const std::string script = R"(
    print("number of env entries is: " .. #S3Request.Environment)
    for k, v in pairs(S3Request.Environment) do
      print("key=" .. k .. ", " .. "value=" .. v)
    end
  )";

  DEFINE_REQ_STATE;
  s.env[""] = "world";
  s.env[""] = "bar";
  s.env["goodbye"] = "cruel world";
  s.env["ka"] = "boom";

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Tags)
{
  const std::string script = R"(
    print("number of tags is: " .. #S3Request.Tags)
    for k, v in pairs(S3Request.Tags) do
      print("key=" .. k .. ", " .. "value=" .. v)
    end
  )";

  DEFINE_REQ_STATE;
  s.tagset.add_tag("hello", "world");
  s.tagset.add_tag("foo", "bar");
  s.tagset.add_tag("goodbye", "cruel world");
  s.tagset.add_tag("ka", "boom");

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Acl)
{
  const std::string script = R"(
    function print_grant(g)
      print("Type: " .. g.Type)
      print("GroupType: " .. g.GroupType)
      print("Permission: " .. g.Permission)
      print("Referer: " .. g.Referer)
      if (g.User) then
        print("User.Tenant: " .. g.User.Tenant)
        print("User.Id: " .. g.User.Id)
      end
    end

    print(S3Request.UserAcl.Owner.DisplayName)
    print(S3Request.UserAcl.Owner.User.Id)
    print(S3Request.UserAcl.Owner.User.Tenant)
    print("number of grants is: " .. #S3Request.UserAcl.Grants)
    for k, v in pairs(S3Request.UserAcl.Grants) do
      print("grant key=" .. k)
      print("grant values=")
      print_grant(v)
    end
  )";

  DEFINE_REQ_STATE;
  ACLOwner owner;
  owner.set_id(rgw_user("john", "doe"));
  owner.set_name("john doe");
  s.user_acl.reset(new RGWAccessControlPolicy(cct));
  s.user_acl->set_owner(owner);
  ACLGrant grant1, grant2, grant3, grant4, grant5;
  grant1.set_canon(rgw_user("jane", "doe"), "her grant", 1);
  grant2.set_referer("http://localhost/ref1", 2);
  grant3.set_referer("http://localhost/ref2", 3);
  grant4.set_canon(rgw_user("john", "doe"), "his grant", 4);
  grant5.set_group(ACL_GROUP_AUTHENTICATED_USERS, 5);
  s.user_acl->get_acl().add_grant(&grant1);
  s.user_acl->get_acl().add_grant(&grant2);
  s.user_acl->get_acl().add_grant(&grant3);
  s.user_acl->get_acl().add_grant(&grant4);
  s.user_acl->get_acl().add_grant(&grant5);
  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, UseFunction)
{
	const std::string script = R"(
		function print_owner(owner)
  		print("Owner Dispaly Name: " .. owner.DisplayName)
  		print("Owner Id: " .. owner.User.Id)
  		print("Owner Tenanet: " .. owner.User.Tenant)
		end

		print_owner(S3Request.ObjectOwner)
    
    function print_acl(acl_type)
      index = acl_type .. "ACL"
      acl = S3Request[index]
      if acl then
        print(acl_type .. "ACL Owner")
        print_owner(acl.Owner)
      else
        print("no " .. acl_type .. " ACL in request: " .. S3Request.Id)
      end 
    end

    print_acl("User")
    print_acl("Bucket")
    print_acl("Object")
	)";

  DEFINE_REQ_STATE;
  s.owner.set_name("user two");
  s.owner.set_id(rgw_user("tenant2", "user2"));
  s.user_acl.reset(new RGWAccessControlPolicy());
  s.user_acl->get_owner().set_name("user three");
  s.user_acl->get_owner().set_id(rgw_user("tenant3", "user3"));
  s.bucket_acl.reset(new RGWAccessControlPolicy());
  s.bucket_acl->get_owner().set_name("user four");
  s.bucket_acl->get_owner().set_id(rgw_user("tenant4", "user4"));
  s.object_acl.reset(new RGWAccessControlPolicy());
  s.object_acl->get_owner().set_name("user five");
  s.object_acl->get_owner().set_id(rgw_user("tenant5", "user5"));

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, WithLib)
{
  const std::string script = R"(
    print("bucket name split:")
    for i in string.gmatch(S3Request.Bucket.Name, "%a+") do
      print("lua print: part: " .. i)
    end
  )";

  DEFINE_REQ_STATE;

  rgw_bucket b;
  b.name = "my-bucket-name-is-fish";
  s.bucket.reset(new sal::RGWRadosBucket(nullptr, b));

  const auto rc = lua::s3::execute(nullptr, nullptr, nullptr, &s, script);
  ASSERT_EQ(rc, 0);
}

#include <sys/socket.h>
#include <stdlib.h>

void unix_socket_client(const std::string& path) {
  int fd;
  // create the socket
  if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    std::cout << "unix socket error: " << errno << std::endl;
    return;
  }
  // set the path
  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path)-1);

	// let the socket be created by the "rgw" side
	std::this_thread::sleep_for(std::chrono::seconds(2));
	if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
		std::cout << "unix socket connect error: " << errno << std::endl;
		return;
 	}

  std::cout << "unix socket connected to: " << path << std::endl;
  char buff[256];
	int rc;
 	while((rc=read(fd, buff, sizeof(buff))) > 0) {
		std::cout << std::string(buff, rc);
  }
}

TEST(TestRGWLua, OpsLog)
{
	const std::string unix_socket_path = "./aSocket.sock";
	unlink(unix_socket_path.c_str());

	std::thread unix_socket_thread(unix_socket_client, unix_socket_path);

  const std::string script = R"(
		if S3Request.Response.HTTPStatusCode == 200 then
			print("request is good, just log to lua: " .. S3Request.Response.Message)
		else 
			print("request is bad, use ops log:")
      if S3Request.Bucket then
    	  rc = S3Request.Log()
    	  print("ops log return code: " .. rc)
      else
        print("no bucket, ops log wasn't called")
      end
		end
  )";

  auto store = std::unique_ptr<sal::RGWRadosStore>(new sal::RGWRadosStore);
  store->setRados(new RGWRados);
  auto olog = std::unique_ptr<OpsLogSocket>(new OpsLogSocket(cct, 1024));
  ASSERT_TRUE(olog->init(unix_socket_path));

  DEFINE_REQ_STATE;
  s.err.http_ret = 200;
  s.err.ret = 0;
  s.err.err_code = "200OK";
  s.err.message = "Life is great";
  rgw_bucket b;
  b.tenant = "tenant";
  b.name = "name";
  b.marker = "marker";
  b.bucket_id = "id"; 
  s.bucket.reset(new sal::RGWRadosBucket(nullptr, b));
  s.bucket_name = "name";
	s.enable_ops_log = true;
	s.enable_usage_log = false;
	s.user.reset(new TestRGWUser());
  TestAccounter ac;
  s.cio = &ac; 
	s.cct->_conf->rgw_ops_log_rados	= false;

  auto rc = lua::s3::execute(store.get(), nullptr, olog.get(), &s, script);
  EXPECT_EQ(rc, 0);
 
	s.err.http_ret = 400;
  rc = lua::s3::execute(store.get(), nullptr, olog.get(), &s, script);
  EXPECT_EQ(rc, 0);

	// give the socket client time to read
	std::this_thread::sleep_for(std::chrono::seconds(2));
	unix_socket_thread.detach();
}

