// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <librdkafka/rdkafka.h>

#if RD_KAFKA_VERSION < 0x00092ff
rd_kafka_resp_err_t rd_kafka_flush (rd_kafka_t *rk, int timeout_ms);
#endif

#if RD_KAFKA_VERSION < 0x00091ff
rd_kafka_resp_err_t rd_kafka_last_error (void);
#endif

