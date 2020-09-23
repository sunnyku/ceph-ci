// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ReadResult.h"
#include "include/buffer.h"
#include "common/dout.h"
#include "librbd/io/AioCompletion.h"
#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ReadResult: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

struct ReadResult::SetClipLengthVisitor : public boost::static_visitor<void> {
  size_t length;

  explicit SetClipLengthVisitor(size_t length) : length(length) {
  }

  void operator()(Linear &linear) const {
    ceph_assert(length <= linear.buf_len);
    linear.buf_len = length;
  }

  template <typename T>
  void operator()(T &t) const {
  }
};

struct ReadResult::SetImageExtentsVisitor : public boost::static_visitor<void> {
  Extents image_extents;

  explicit SetImageExtentsVisitor(const Extents& image_extents)
    : image_extents(image_extents) {
  }

  void operator()(SparseBufferlist &sbl) const {
    sbl.image_extents = image_extents;
  }

  template <typename T>
  void operator()(T &t) const {
  }
};

struct ReadResult::AssembleResultVisitor : public boost::static_visitor<void> {
  CephContext *cct;
  Striper::StripedReadResult &destriper;

  AssembleResultVisitor(CephContext *cct, Striper::StripedReadResult &destriper)
    : cct(cct), destriper(destriper) {
  }

  void operator()(Empty &empty) const {
    ldout(cct, 20) << "dropping read result" << dendl;
  }

  void operator()(Linear &linear) const {
    ldout(cct, 20) << "copying resulting bytes to "
                   << reinterpret_cast<void*>(linear.buf) << dendl;
    destriper.assemble_result(cct, linear.buf, linear.buf_len);
  }

  void operator()(Vector &vector) const {
    bufferlist bl;
    destriper.assemble_result(cct, bl, true);

    ldout(cct, 20) << "copying resulting " << bl.length() << " bytes to iovec "
                   << reinterpret_cast<const void*>(vector.iov) << dendl;

    bufferlist::iterator it = bl.begin();
    size_t length = bl.length();
    size_t offset = 0;
    int idx = 0;
    for (; offset < length && idx < vector.iov_count; idx++) {
      size_t len = std::min(vector.iov[idx].iov_len, length - offset);
      it.copy(len, static_cast<char *>(vector.iov[idx].iov_base));
      offset += len;
    }
    ceph_assert(offset == bl.length());
  }

  void operator()(Bufferlist &bufferlist) const {
    bufferlist.bl->clear();
    destriper.assemble_result(cct, *bufferlist.bl, true);

    ldout(cct, 20) << "moved resulting " << bufferlist.bl->length() << " "
                   << "bytes to bl " << reinterpret_cast<void*>(bufferlist.bl)
                   << dendl;
  }

  void operator()(SparseBufferlist &sparse_bufferlist) const {
    sparse_bufferlist.extent_map->clear();
    sparse_bufferlist.bl->clear();
    auto buffer_extents_length = destriper.assemble_result(
      cct, sparse_bufferlist.extent_map, sparse_bufferlist.bl);

    ExtentMap buffer_extent_map;
    buffer_extent_map.swap(*sparse_bufferlist.extent_map);
    ldout(cct, 20) << "image_extents="
                   << sparse_bufferlist.image_extents << ", "
                   << "buffer_extent_map=" << buffer_extent_map << dendl;

    // The extent-map is logically addressed by buffer-extents not image- or
    // object-extents. Translate this address mapping to image-extent
    // logical addressing since it's tied to an image-extent read
    uint64_t buffer_offset = 0;
    auto bem_it = buffer_extent_map.begin();
    for (auto [image_offset, image_length] : sparse_bufferlist.image_extents) {
      while (bem_it != buffer_extent_map.end()) {
        auto [buffer_extent_offset, buffer_extent_length] = *bem_it;

        if (buffer_offset + image_length <= buffer_extent_offset) {
          // skip any image extent that is not included in the results
          break;
        }

        // current buffer-extent should be within the current image-extent
        ceph_assert(buffer_offset <= buffer_extent_offset &&
                    buffer_offset + image_length >=
                      buffer_extent_offset + buffer_extent_length);
        auto image_extent_offset =
          image_offset + (buffer_extent_offset - buffer_offset);
        ldout(cct, 20) << "mapping buffer extent " << buffer_extent_offset
                       << "~" << buffer_extent_length << " to image extent "
                       << image_extent_offset << "~" << buffer_extent_length
                       << dendl;
        (*sparse_bufferlist.extent_map)[image_extent_offset] =
          buffer_extent_length;
        ++bem_it;
      }

      buffer_offset += image_length;
    }
    ceph_assert(buffer_offset == buffer_extents_length);
    ceph_assert(bem_it == buffer_extent_map.end());

    ldout(cct, 20) << "moved resulting " << *sparse_bufferlist.extent_map
                   << " extents of total " << sparse_bufferlist.bl->length()
                   << " bytes to bl "
                   << reinterpret_cast<void*>(sparse_bufferlist.bl) << dendl;
  }
};

ReadResult::C_ImageReadRequest::C_ImageReadRequest(
    AioCompletion *aio_completion, const Extents image_extents)
  : aio_completion(aio_completion), image_extents(image_extents) {
  aio_completion->add_request();
}

void ReadResult::C_ImageReadRequest::finish(int r) {
  CephContext *cct = aio_completion->ictx->cct;
  ldout(cct, 10) << "C_ImageReadRequest: r=" << r
                 << dendl;
  if (r >= 0) {
    size_t length = 0;
    for (auto &image_extent : image_extents) {
      length += image_extent.second;
    }
    ceph_assert(length == bl.length());

    aio_completion->lock.lock();
    aio_completion->read_result.m_destriper.add_partial_result(
      cct, bl, image_extents);
    aio_completion->lock.unlock();
    r = length;
  }

  aio_completion->complete_request(r);
}

ReadResult::C_ObjectReadRequest::C_ObjectReadRequest(
    AioCompletion *aio_completion, uint64_t object_off, uint64_t object_len,
    LightweightBufferExtents&& buffer_extents)
  : aio_completion(aio_completion), object_off(object_off),
    object_len(object_len), buffer_extents(std::move(buffer_extents)) {
  aio_completion->add_request();
}

void ReadResult::C_ObjectReadRequest::finish(int r) {
  CephContext *cct = aio_completion->ictx->cct;
  ldout(cct, 10) << "C_ObjectReadRequest: r=" << r
                 << dendl;

  if (r == -ENOENT) {
    r = 0;
  }
  if (r >= 0) {
    ldout(cct, 10) << " got " << extent_map
                   << " for " << buffer_extents
                   << " bl " << bl.length() << dendl;
    aio_completion->lock.lock();
    if (!extent_map.empty()) {
      aio_completion->read_result.m_destriper.add_partial_sparse_result(
        cct, bl, extent_map, object_off, buffer_extents);
    } else {
      // handle the case where a sparse-read wasn't issued
      aio_completion->read_result.m_destriper.add_partial_result(
        cct, std::move(bl), buffer_extents);
    }
    aio_completion->lock.unlock();

    r = object_len;
  }

  aio_completion->complete_request(r);
}

ReadResult::ReadResult() : m_buffer(Empty()) {
}

ReadResult::ReadResult(char *buf, size_t buf_len)
  : m_buffer(Linear(buf, buf_len)) {
}

ReadResult::ReadResult(const struct iovec *iov, int iov_count)
  : m_buffer(Vector(iov, iov_count)) {
}

ReadResult::ReadResult(ceph::bufferlist *bl)
  : m_buffer(Bufferlist(bl)) {
}

ReadResult::ReadResult(std::map<uint64_t, uint64_t> *extent_map,
                       ceph::bufferlist *bl)
  : m_buffer(SparseBufferlist(extent_map, bl)) {
}

void ReadResult::set_clip_length(size_t length) {
  boost::apply_visitor(SetClipLengthVisitor(length), m_buffer);
}

void ReadResult::set_image_extents(const Extents& image_extents) {
  boost::apply_visitor(SetImageExtentsVisitor(image_extents), m_buffer);
}

void ReadResult::assemble_result(CephContext *cct) {
  boost::apply_visitor(AssembleResultVisitor(cct, m_destriper), m_buffer);
}

} // namespace io
} // namespace librbd

