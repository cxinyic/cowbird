#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
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
  int buffer_metadata_size_ = 3 * sizeof(uint64_t);
  uint64_t last_boundary;

  char *buffer_;
  char *buffer_fake_;

  CowbirdBuffer() {}
  CowbirdBuffer(uint64_t capacity) {
    capacity_ = capacity;
    real_boundary_ = 0;
    head_ = 0;
    tail_ = 0;
    last_boundary = 0;
  }

  ~CowbirdBuffer() { delete[] buffer_; }

  /*------producer ------*/
  bool insert_check(int size) {
    if (head_ + size > capacity_) {
      real_boundary_ = head_;
      write_real_boundary();
      head_ = 0;
    }

    if (head_ >= tail_) {
      return true;
    }

    else if (head_ < tail_) {
      if (head_ + size <= tail_) {
        return true;
      } else {
        return false;
      }
    }
    return true;
  }
  bool insert(void *data, int size) {
    uint64_t start, end;
    if (head_ + size > capacity_) {
      if (head_ + size - capacity_ > tail_) {
        return false;
      }
      real_boundary_ = head_;
      write_real_boundary();
      head_ = 0;
    }

    if (head_ >= tail_) {
      memcpy(buffer_ + buffer_metadata_size_ + head_, data, size);
      head_ += size;
      return true;
    }

    else if (head_ < tail_) {
      if (head_ + size <= tail_) {
        memcpy(buffer_ + buffer_metadata_size_ + head_, data, size);
        head_ += size;
        return true;
      } else {
        return false;
      }
    }

    return true;
  }
  void insert_after_check(void *data, int size) {
    memcpy(buffer_ + buffer_metadata_size_ + head_, data, size);
    head_ += size;
  }
  /*------consumer ------*/
  bool poll(void *data, int size) {
    uint64_t start, end;
    // init status, nothing to poll
    if (head_ == 0 && real_boundary_ == 0 && tail_ == 0) {
      return false;
    }
    if (real_boundary_ != 0 && tail_ == real_boundary_ - last_boundary &&
        (real_boundary_ - last_boundary) != 0) {
      // printf("hit the real boundary\n");
      // printf("real boundary is %lu, last boundary is %lu\n", real_boundary_,
      //        last_boundary);
      tail_ = 0;
      last_boundary = real_boundary_;
    }

    if (tail_ < head_) {
      if (tail_ + size <= head_) {
        memcpy(data, buffer_ + buffer_metadata_size_ + tail_, size);
        tail_ += size;
        return true;
      } else {
        return false;
      }
    } else if (tail_ >= head_ && real_boundary_ != 0) {
      if (tail_ + size + last_boundary <= real_boundary_) {
        memcpy(data, buffer_ + buffer_metadata_size_ + tail_, size);
        tail_ += size;
        return true;
      }
      return false;
    }

    return false;
  }
  bool poll_check(int size) {
    if (head_ == 0 && real_boundary_ == 0 && tail_ == 0) {
      return false;
    }
    if (real_boundary_ != 0 && tail_ == real_boundary_ - last_boundary &&
        (real_boundary_ - last_boundary) != 0) {
      tail_ = 0;
      last_boundary = real_boundary_;
    }

    if (tail_ < head_) {
      if (tail_ + size <= head_) {
        tail_ += size;
        return true;
      } else {
        return false;
      }
    } else if (tail_ >= head_ && real_boundary_ != 0) {
      if (tail_ + size + last_boundary <= real_boundary_) {
        tail_ += size;
        return true;
      }
      return false;
    }
    return false;
  }
  // update the request buffer head; real boundary and the response buffer tail
  void write_buffer_metadata(uint64_t response_tail) {
    memcpy(buffer_, &head_, sizeof(uint64_t));
    memcpy(buffer_ + sizeof(uint64_t), &response_tail, sizeof(uint64_t));
    memcpy(buffer_ + 2 * sizeof(uint64_t), &real_boundary_, sizeof(uint64_t));
  }

  void write_request_head() { memcpy(buffer_, &head_, sizeof(uint64_t)); }

  void write_real_boundary() {
    memcpy(buffer_ + 2 * sizeof(uint64_t), &real_boundary_, sizeof(uint64_t));
  }

  void read_request_tail(uint64_t *request_tail) {
    memcpy(request_tail, buffer_ + sizeof(uint64_t), sizeof(uint64_t));
  }

  void read_response_head() {
    memcpy(&head_, buffer_, sizeof(uint64_t));
    memcpy(&real_boundary_, buffer_ + 2 * sizeof(uint64_t), sizeof(uint64_t));
  }

  void write_response_tail(uint64_t response_tail) {
    memcpy(buffer_ + sizeof(uint64_t), &response_tail, sizeof(uint64_t));
  }

  uint64_t get_head() { return head_; }

  uint64_t get_tail() { return tail_; }
  void set_tail(uint64_t new_tail) { tail_ = new_tail; }

  uint64_t get_real_boundary() { return real_boundary_; }
  char *get_addr() { return buffer_; }

  void print_buffer() {
    printf("head: %lu, tail: %lu, real_boundary: %lu, last_boundary is %lu\n",
           head_, tail_, real_boundary_, last_boundary);
  }
};
