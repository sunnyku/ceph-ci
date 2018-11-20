#include <boost/format.hpp>

#include "include/health.h"
#include "include/types.h"
#include "OSDHealthMetricCollector.h"


using namespace std;

ostream& operator<<(ostream& os,
                    const OSDHealthMetricCollector::DaemonKey& daemon) {
  return os << daemon.first << "." << daemon.second;
}

namespace {

class PendingPGs final : public OSDHealthMetricCollector {
  bool _is_relevant(osd_metric type) const override {
    return type == osd_metric::PENDING_CREATING_PGS;
  }
  health_check_t& _get_check(health_check_map_t& cm) const override {
    return cm.get_or_add("PENDING_CREATING_PGS", HEALTH_WARN, "");
  }
  bool _update(const DaemonKey& osd,
               const OSDHealthMetric& metric) override {
    value.n += metric.get_n();
    if (metric.get_n()) {
      osds.push_back(osd);
      return true;
    } else {
      return false;
    }
  }
  void _summarize(health_check_t& check) const override {
    if (osds.empty()) {
      return;
    }
    static const char* fmt = "%1% PGs pending on creation";
    check.summary = boost::str(boost::format(fmt) % value.n);
    ostringstream ss;
    if (osds.size() > 1) {
      ss << "osds " << osds << " have pending PGs.";
    } else {
      ss << osds.front() << " has pending PGs";
    }
    check.detail.push_back(ss.str());
  }
  vector<DaemonKey> osds;
};

class EioObjects final : public OSDHealthMetricCollector {
  bool _is_relevant(osd_metric type) const override {
    return type == osd_metric::EIO_OBJECTS;
  }
  health_check_t& _get_check(health_check_map_t& cm) const override {
    return cm.get_or_add("EIO_OBJECTS", HEALTH_WARN, "");
  }
  bool _update(const DaemonKey& osd,
               const OSDHealthMetric& metric) override {
    if (metric.get_n()) {
      eio_objects[osd] = metric;
      return true;
    }
    return false;
  }
  void _summarize(health_check_t& check) const override {
    auto num = eio_objects.size();
    if (num == 0)
      return;
    ostringstream summary;
    if (num == 1)
      summary << "1 osd has eio object(s)";
    else
      summary << num << " osds have eio object(s)";
    check.summary = summary.str();
    for (auto & e : eio_objects) {
      ostringstream detail;
      detail << e.first << " has " << e.second.get_n() << " eio object(s)";
      check.detail.push_back(detail.str());
    }
  }
  map<DaemonKey, OSDHealthMetric> eio_objects;
};


} // anonymous namespace

unique_ptr<OSDHealthMetricCollector>
OSDHealthMetricCollector::create(osd_metric m)
{
  switch (m) {
  case osd_metric::PENDING_CREATING_PGS:
    return unique_ptr<OSDHealthMetricCollector>{new PendingPGs};
  case osd_metric::EIO_OBJECTS:
    return unique_ptr<OSDHealthMetricCollector>{new EioObjects};
  default:
    return unique_ptr<OSDHealthMetricCollector>{};
  }
}
