// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/MetadataRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::MetadataRemoveRequest: "

namespace librbd {
namespace operation {

template <typename I>
MetadataRemoveRequest<I>::MetadataRemoveRequest(I &image_ctx,
                                                Context *on_finish,
                                                const std::string &key)
  : Request<I>(image_ctx, on_finish), m_key(key) {
}

template <typename I>
void MetadataRemoveRequest<I>::send_op() {
  send_status_update();
}

template <typename I>
bool MetadataRemoveRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }

  RWLock::RLocker owner_locker(image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_STATUS_UPDATE:
    ldout(cct, 5) << "STATUS_UPDATE" << dendl;
    send_metadata_remove();
    break;
  case STATE_REMOVE_METADATA:
    ldout(cct, 5) << "REMOVE_METADATA" << dendl;
    finished = true;
    break;
  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    assert(false);
    break;
  }
  return finished;
}

template <typename I>
void MetadataRemoveRequest<I>::send_status_update() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  m_state = STATE_STATUS_UPDATE;

  if (m_key != QOS_MLMT && m_key != QOS_MBDW
      && m_key != QOS_MRSV && m_key != QOS_MWGT) {
    send_metadata_remove();
    return;
  }

  librados::ObjectWriteOperation op;
  if (m_key == QOS_MLMT) {
    cls_client::status_update_qos(&op, image_ctx.id, -1, -2, -2, -2);
  }
  if (m_key == QOS_MBDW) {
    cls_client::status_update_qos(&op, image_ctx.id, -2, -1, -2, -2);
  }
  if (m_key == QOS_MRSV) {
    cls_client::status_update_qos(&op, image_ctx.id, -2, -2, -1, -2);
  }
  if (m_key == QOS_MWGT) {
    cls_client::status_update_qos(&op, image_ctx.id, -2, -2, -2, -1);
  }

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(RBD_STATUS, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
void MetadataRemoveRequest<I>::send_metadata_remove() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  m_state = STATE_REMOVE_METADATA;

  librados::ObjectWriteOperation op;
  cls_client::metadata_remove(&op, m_key);

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  assert(r == 0);
  comp->release();
}

} // namespace operation
} // namespace librbd

template class librbd::operation::MetadataRemoveRequest<librbd::ImageCtx>;
