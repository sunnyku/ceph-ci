add_executable(unittest_seastar_denc
  test_denc.cc)
add_ceph_unittest(unittest_seastar_denc)
target_link_libraries(unittest_seastar_denc crimson GTest::Main)

add_executable(perf_crimson_msgr perf_crimson_msgr.cc)
target_link_libraries(perf_crimson_msgr ceph-common crimson)

add_executable(unittest_seastar_echo
  test_alien_echo.cc)
target_link_libraries(unittest_seastar_echo crimson)

add_executable(unittest_async_echo
  test_async_echo.cc)
target_link_libraries(unittest_async_echo ceph-common global)

add_executable(unittest_seastar_config
  test_config.cc)
target_link_libraries(unittest_seastar_config crimson)

add_executable(unittest_seastar_monc
  test_monc.cc)
target_link_libraries(unittest_seastar_monc crimson)

add_executable(unittest_seastar_lru
  test_lru.cc)
add_ceph_unittest(unittest_seastar_lru)
target_link_libraries(unittest_seastar_lru crimson GTest::Main)

