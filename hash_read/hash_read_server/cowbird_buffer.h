#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>

static inline uint64_t rdtsc() {
  unsigned int lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)hi << 32) | lo;
}

class CowbirdBuffer {

public:
  uint64_t capacity_;
  uint64_t head_;
  uint64_t tail_;
  uint64_t real_boundary_;
  char *buffer_;
  int buffer_metadata_size_ = 3 * sizeof(uint64_t);
  uint64_t process_tail_;

  CowbirdBuffer() {}
  CowbirdBuffer(uint64_t capacity) {
    capacity_ = capacity;
    real_boundary_ = 0;
    head_ = 0;
    tail_ = 0;
    process_tail_ = 0;
  }

  ~CowbirdBuffer() {}

  /*------consumer: process requests------*/
  bool process_poll(void *data, int size) {
    // init status, nothing to poll
    if (process_tail_ == 0 && real_boundary_ == 0 && tail_ == 0) {
      return false;
    }
    if (process_tail_ == real_boundary_ && real_boundary_ != 0) {
      printf("process_tail set to 0\n");
      process_tail_ = 0;
    }

    if (process_tail_ >= tail_) {
      return false;
    }

    else if (process_tail_ < tail_) {
      if (process_tail_ + size <= tail_) {
        memcpy(data, buffer_ + buffer_metadata_size_ + process_tail_, size);
        process_tail_ += size;
        return true;
      } else {
        return false;
      }
    }
    return true;
  }
  /*------consumer: read requests------*/
  // return -1 for fail, 0 for success, a positive number for poll size since it
  // reaches the real boundary
  int poll_check(int size) {
    // init status, nothing to poll
    if (head_ == 0 && real_boundary_ == 0 && tail_ == 0) {
      return -1;
    }
    if (tail_ == real_boundary_ && real_boundary_ != 0) {
      tail_ = 0;
      head_ = 0;
      real_boundary_ = 0;
    }

    if (tail_ >= head_) {
      if (real_boundary_ == 0) {
        return -1;
      }
      if (tail_ + size <= real_boundary_) {
        return 0;
      } else {
        return real_boundary_ - tail_;
      }
    } else if (tail_ < head_) {
      if (tail_ + size <= head_) {
        return 0;
      } else {
        return -1;
      }
    }
    return 0;
  }
  void poll_after_check(int size) { tail_ += size; }

  char *get_addr() { return buffer_; }

  void print_buffer() {
    printf("RequestBuffer head: %lu, tail: %lu, process_tail: %lu, "
           "real_boundary: %lu\n",
           head_, tail_, process_tail_, real_boundary_);
  }
};
enum insert_check_status { INSERT_SUCCESS, INSERT_FAIL, INSERT_NEED_FLUSH };
// LightBuffer does not have a real char* buffer.
// It only stores and manages the metadata of the buffer.
class LightCowbirdBuffer {
public:
  uint64_t capacity_;
  uint64_t head_;
  uint64_t tail_;
  uint64_t real_boundary_;
  uint64_t flush_head_;
  int current_write_tail_;

  LightCowbirdBuffer(uint64_t capacity) {
    capacity_ = capacity;
    real_boundary_ = 0;
    head_ = 0;
    tail_ = 0;
    flush_head_ = 0;
    current_write_tail_ = 0;
  }
  /*------producer: write responses------*/
  int insert_check(int size) {
    if (head_ + size > capacity_) {
      real_boundary_ += head_;
      head_ = 0;
      return INSERT_NEED_FLUSH;
    }

    if (head_ >= tail_) {
      return INSERT_SUCCESS;
    }

    else if (head_ < tail_) {
      if (head_ + size <= tail_) {
        return INSERT_SUCCESS;
      } else {
        return INSERT_FAIL;
      }
    }
    return INSERT_FAIL;
  }
  void insert_after_check(int size) { head_ += size; }
  void update_flush_head(int size) {
    if (size == 0) {
      flush_head_ = 0;
      return;
    }
    flush_head_ += size;
  }

  uint64_t get_flush_head() { return flush_head_; }

  void print_buffer() {
    printf("ResponseBuffer head: %lu, tail: %lu, flush_head: %lu, "
           "real_boundary: %lu\n",
           head_, tail_, flush_head_, real_boundary_);
  }
};
