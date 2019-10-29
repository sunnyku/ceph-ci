// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "kafka_compat.h"
#include <errno.h>

#if RD_KAFKA_VERSION < 0x00092ff
rd_kafka_resp_err_t rd_kafka_flush (rd_kafka_t *rk, int timeout_ms) {
        unsigned int msg_cnt = 0;
	int qlen;
	rd_ts_t ts_end = rd_timeout_init(timeout_ms);
        int tmout;

	if (rk->rk_type != RD_KAFKA_PRODUCER)
		return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;

        rd_kafka_yield_thread = 0;

        /* First poll call is non-blocking for the case
         * where timeout_ms==RD_POLL_NOWAIT to make sure poll is
         * called at least once. */
        tmout = RD_POLL_NOWAIT;
        do {
                rd_kafka_poll(rk, tmout);
        } while (((qlen = rd_kafka_q_len(rk->rk_rep)) > 0 ||
                  (msg_cnt = rd_kafka_curr_msgs_cnt(rk)) > 0) &&
                 !rd_kafka_yield_thread &&
                 (tmout = rd_timeout_remains_limit(ts_end, 10)) !=
                 RD_POLL_NOWAIT);

	return qlen + msg_cnt > 0 ? RD_KAFKA_RESP_ERR__TIMED_OUT :
		RD_KAFKA_RESP_ERR_NO_ERROR;
}
#endif

#if RD_KAFKA_VERSION < 0x00091ff
rd_kafka_resp_err_t rd_kafka_last_error (void) {
	return rd_kafka_errno2err(errno);
}
#endif

