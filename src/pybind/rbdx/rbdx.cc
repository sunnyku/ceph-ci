#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "rados/librados.hpp"
#include "rbd/librbdx.hpp"

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>

#include "nlohmann/json.hpp"

namespace {

// it is defined as uint64_t in ceph C++ code
constexpr int64_t CEPH_NOSNAP = ((int64_t)(-2));

}

namespace py = pybind11;

using Map_string_2_pair_image_info_t_int = std::map<std::string, std::pair<librbdx::image_info_t, int>>;
PYBIND11_MAKE_OPAQUE(Map_string_2_pair_image_info_t_int);

namespace {

// is_string
template<typename T>
struct is_string :
    std::integral_constant<bool,
      std::is_same<char*, typename std::decay<T>::type>::value ||
      std::is_same<const char*, typename std::decay<T>::type>::value
    > {};

template<>
struct is_string<std::string> : std::true_type {};

// is_pair
template <typename T>
struct is_pair : std::false_type {};

template <typename T1, typename T2>
struct is_pair<std::pair<T1, T2>> : std::true_type {};

// is_sequence
template <typename T>
struct is_sequence : std::false_type {};

template <typename... Ts> struct is_sequence<std::list<Ts...>> : std::true_type {};
template <typename... Ts> struct is_sequence<std::set<Ts...>> : std::true_type {};
template <typename... Ts> struct is_sequence<std::vector<Ts...>> : std::true_type {};

}

namespace {

using namespace librbdx;
using json = nlohmann::json;

// forward declaration, otherwise will have errors like the following
// error: no matching function for call to ‘json_fmt(const librbdx::snap_info_t&)’
template <typename T,
  typename std::enable_if<std::is_arithmetic<T>::value ||
      is_string<T>::value, std::nullptr_t>::type=nullptr
>
auto json_fmt(const T& o) -> decltype(o);

template <typename T,
  typename std::enable_if<std::is_enum<T>::value, std::nullptr_t>::type=nullptr
>
std::string json_fmt(const T& o);

template <typename T,
  typename std::enable_if<is_pair<T>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const T& o);

template <typename T,
  typename std::enable_if<is_sequence<T>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const T& o);

template <typename K, typename V, typename... Ts,
  typename std::enable_if<std::is_arithmetic<K>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o);

template <typename K, typename V, typename... Ts,
  typename std::enable_if<is_string<K>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o);

json json_fmt(const parent_t& o);
json json_fmt(const child_t& o);
json json_fmt(const snap_info_t& o);
json json_fmt(const image_info_t& o);

template <typename T,
  typename std::enable_if<std::is_arithmetic<T>::value ||
      is_string<T>::value, std::nullptr_t>::type=nullptr
>
auto json_fmt(const T& o) -> decltype(o) {
  return o;
}

template <typename T,
  typename std::enable_if<std::is_enum<T>::value, std::nullptr_t>::type=nullptr
>
std::string json_fmt(const T& o) {
  return stringify(o);
}

template <typename T,
  typename std::enable_if<is_pair<T>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const T& o) {
  json j = json::array({});
  j.push_back(json_fmt(o.first));
  j.push_back(json_fmt(o.second));
  return std::move(j);
}

template <typename T,
  typename std::enable_if<is_sequence<T>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const T& o) {
  json j = json::array({});
  for (auto& i : o) {
    j.push_back(json_fmt(i));
  }
  return std::move(j);
}

template <typename K, typename V, typename... Ts,
  typename std::enable_if<std::is_arithmetic<K>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o) {
  json j = json::object({});
  for (auto& it : o) {
    auto k = std::to_string(it.first);
    j[k] = json_fmt(it.second);
  }
  return std::move(j);
}

template <typename K, typename V, typename... Ts,
  typename std::enable_if<is_string<K>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o) {
  json j = json::object({});
  for (auto& it : o) {
    auto k = it.first;
    j[k] = json_fmt(it.second);
  }
  return std::move(j);
}

json json_fmt(const parent_t& o) {
  json j = json::object({});
  j["pool_id"] = json_fmt(o.pool_id);
  j["pool_namespace"] = json_fmt(o.pool_namespace);
  j["image_id"] = json_fmt(o.image_id);
  j["snap_id"] = json_fmt((int64_t)o.snap_id);
  return std::move(j);
}

json json_fmt(const child_t& o) {
  json j = json::object({});
  j["pool_id"] = json_fmt(o.pool_id);
  j["pool_namespace"] = json_fmt(o.pool_namespace);
  j["image_id"] = json_fmt(o.image_id);
  return std::move(j);
}

json json_fmt(const snap_info_t& o) {
  json j = json::object({});
  j["name"] = json_fmt(o.name);
  j["id"] = json_fmt(o.id);
  j["snap_type"] = json_fmt(o.snap_type);
  j["size"] = json_fmt(o.size);
  j["flags"] = json_fmt(o.flags);
  j["timestamp"] = json_fmt(o.timestamp);
  j["children"] = json_fmt(o.children);
  j["du"] = json_fmt(o.du);
  j["dirty"] = json_fmt(o.dirty);
  return std::move(j);
}

json json_fmt(const image_info_t& o) {
  json j = json::object({});
  j["name"] = json_fmt(o.name);
  j["id"] = json_fmt(o.id);
  j["order"] = json_fmt(o.order);
  j["size"] = json_fmt(o.size);
  j["features"] = json_fmt(o.features);
  j["op_features"] = json_fmt(o.op_features);
  j["flags"] = json_fmt(o.flags);
  j["snaps"] = json_fmt(o.snaps);
  j["parent"] = json_fmt(o.parent);
  j["create_timestamp"] = json_fmt(o.create_timestamp);
  j["access_timestamp"] = json_fmt(o.access_timestamp);
  j["modify_timestamp"] = json_fmt(o.modify_timestamp);
  j["data_pool_id"] = json_fmt(o.data_pool_id);
  j["watchers"] = json_fmt(o.watchers);
  j["metas"] = json_fmt(o.metas);
  j["du"] = json_fmt(o.du);
  j["dirty"] = json_fmt(o.dirty);
  return std::move(j);
}

}

namespace rbdx {

using namespace librados;
using namespace librbdx;
using json = nlohmann::json;

constexpr int json_indent = 4;

PYBIND11_MODULE(rbdx, m) {

  m.attr("CEPH_NOSNAP") = py::int_(CEPH_NOSNAP);

  m.attr("INFO_F_CHILDREN_V1") = py::int_(INFO_F_CHILDREN_V1);
  m.attr("INFO_F_IMAGE_DU") = py::int_(INFO_F_IMAGE_DU);
  m.attr("INFO_F_SNAP_DU") = py::int_(INFO_F_SNAP_DU);
  m.attr("INFO_F_ALL") = py::int_(INFO_F_ALL);

  {
    auto b = py::bind_map<Map_string_2_pair_image_info_t_int>(m, "Map_string_2_pair_image_info_t_int");
    b.def("__repr__", [](const Map_string_2_pair_image_info_t_int& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::enum_<snap_type_t> e(m, "snap_type_t", py::arithmetic());
    e.value("SNAPSHOT_NAMESPACE_TYPE_USER", snap_type_t::SNAPSHOT_NAMESPACE_TYPE_USER);
    e.value("SNAPSHOT_NAMESPACE_TYPE_GROUP", snap_type_t::SNAPSHOT_NAMESPACE_TYPE_GROUP);
    e.value("SNAPSHOT_NAMESPACE_TYPE_TRASH", snap_type_t::SNAPSHOT_NAMESPACE_TYPE_TRASH);
    e.export_values();
  }

  {
    py::class_<parent_t> cls(m, "parent_t");
    cls.def(py::init<>());
    cls.def_readonly("pool_id", &parent_t::pool_id);
    cls.def_readonly("pool_namespace", &parent_t::pool_namespace);
    cls.def_readonly("image_id", &parent_t::image_id);
    cls.def_property_readonly("snap_id", [](const parent_t& self) -> int64_t {
      return (int64_t)self.snap_id;
    }, py::return_value_policy::copy);
    cls.def("__repr__", [](const parent_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<child_t> cls(m, "child_t");
    cls.def(py::init<>());
    cls.def_readonly("pool_id", &child_t::pool_id);
    cls.def_readonly("pool_namespace", &child_t::pool_namespace);
    cls.def_readonly("image_id", &child_t::image_id);
    cls.def("__repr__", [](const child_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<snap_info_t> cls(m, "snap_info_t");
    cls.def(py::init<>());
    cls.def_readonly("name", &snap_info_t::name);
    cls.def_readonly("id", &snap_info_t::id);
    cls.def_readonly("snap_type", &snap_info_t::snap_type);
    cls.def_readonly("size", &snap_info_t::size);
    cls.def_readonly("flags", &snap_info_t::flags);
    cls.def_readonly("timestamp", &snap_info_t::timestamp);
    cls.def_readonly("children", &snap_info_t::children);
    cls.def_readonly("du", &snap_info_t::du);
    cls.def_readonly("dirty", &snap_info_t::dirty);
    cls.def("__repr__", [](const snap_info_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<image_info_t> cls(m, "image_info_t");
    cls.def(py::init<>());
    cls.def_readonly("name", &image_info_t::name);
    cls.def_readonly("id", &image_info_t::id);
    cls.def_readonly("order", &image_info_t::order);
    cls.def_readonly("size", &image_info_t::size);
    cls.def_readonly("features", &image_info_t::features);
    cls.def_readonly("op_features", &image_info_t::op_features);
    cls.def_readonly("flags", &image_info_t::flags);
    cls.def_readonly("snaps", &image_info_t::snaps);
    cls.def_readonly("parent", &image_info_t::parent);
    cls.def_readonly("create_timestamp", &image_info_t::create_timestamp);
    cls.def_readonly("access_timestamp", &image_info_t::access_timestamp);
    cls.def_readonly("modify_timestamp", &image_info_t::modify_timestamp);
    cls.def_readonly("data_pool_id", &image_info_t::data_pool_id);
    cls.def_readonly("watchers", &image_info_t::watchers);
    cls.def_readonly("metas", &image_info_t::metas);
    cls.def_readonly("du", &image_info_t::du);
    cls.def_readonly("dirty", &image_info_t::dirty);
    cls.def("__repr__", [](const image_info_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  //
  // IoCtx
  //
  {
    py::class_<IoCtx> cls(m, "IoCtx");
    cls.def(py::init([](py::handle h_rados_ioctx) {
      auto* ptr = h_rados_ioctx.ptr();
      auto* c_ioctx = reinterpret_cast<rados_ioctx_t*>(PyCapsule_GetPointer(ptr, "ioctx"));
      if (PyErr_Occurred()) {
        throw py::error_already_set();
      }
      auto ioctx = new IoCtx{};
      IoCtx::from_rados_ioctx_t(c_ioctx, *ioctx);
      return std::unique_ptr<IoCtx>{ioctx};
    }));
    // use context manager to ensure release ioctx in time
    cls.def("__enter__", [](IoCtx& self) {
      return self;
    });
    cls.def("__exit__", [](IoCtx& self, py::args) {
      self.close();
    });
  }

  {
    m.def("get_info",
        [](librados::IoCtx& ioctx,
            const std::string& image_name,
            const std::string& image_id,
            uint64_t flags) {
          image_info_t info;
          int r = get_info(ioctx, image_name, image_id, &info, flags);
          return std::make_pair(info, r);
        },
        py::call_guard<py::gil_scoped_release>(),
        py::arg("ioctx"),
        py::arg("image_name"),
        py::arg("image_id"),
        py::arg("flags") = 0);

    m.def("list",
        [](librados::IoCtx& ioctx) {
          std::map<std::string, std::string> images;
          int r = list(ioctx, &images);
          return std::make_pair(images, r);
        },
        py::call_guard<py::gil_scoped_release>());

    m.def("list_info",
        [](librados::IoCtx& ioctx, uint64_t flags) {
          using T = Map_string_2_pair_image_info_t_int;
          auto infos = std::unique_ptr<T>(new Map_string_2_pair_image_info_t_int{});
          int r = list_info(ioctx, infos.get(), flags);
          return std::make_pair(std::move(infos), r);
        },
        py::call_guard<py::gil_scoped_release>(),
        py::arg("ioctx"),
        py::arg("flags") = 0);

    m.def("list_info",
        [](librados::IoCtx& ioctx, const std::map<std::string, std::string>& images, // <id, name>
            uint64_t flags) {
          using T = Map_string_2_pair_image_info_t_int;
          auto infos = std::unique_ptr<T>(new Map_string_2_pair_image_info_t_int{});
          int r = list_info(ioctx, images, infos.get(), flags);
          return std::make_pair(std::move(infos), r);
        },
        py::call_guard<py::gil_scoped_release>(),
        py::arg("ioctx"),
        py::arg("images"),
        py::arg("flags") = 0);
  }

} // PYBIND11_MODULE(rbdx, m)

} // namespace rbdx
