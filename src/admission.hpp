#pragma once
// Pure admission-control / queueing logic. No threads, no clocks, no gRPC:
// everything here is unit-tested directly in tests/unit_tests.cpp.
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>

namespace lrit {

enum class Priority : int { Interactive = 0, Batch = 1 };

// Two FIFO lanes. With priority on, interactive work is always dequeued first;
// with it off, everything shares one lane (plain FIFO).
template <class T>
class TwoClassQueue {
 public:
  explicit TwoClassQueue(bool priority = true) : priority_(priority) {}
  void push(T item, Priority p) { lane(p).push_back(std::move(item)); }
  bool empty() const { return hi_.empty() && lo_.empty(); }
  size_t size() const { return hi_.size() + lo_.size(); }
  // Requests that would be served before a new arrival of class p.
  size_t ahead_of(Priority p) const {
    return (priority_ && p == Priority::Interactive) ? hi_.size() : size();
  }
  T pop() {
    auto& q = hi_.empty() ? lo_ : hi_;
    T item = std::move(q.front());
    q.pop_front();
    return item;
  }
  bool priority_enabled() const { return priority_; }

 private:
  std::deque<T>& lane(Priority p) { return (priority_ && p == Priority::Interactive) ? hi_ : lo_; }
  bool priority_;
  std::deque<T> hi_, lo_;
};

// Synthetic cost of one scheduler step (microseconds).
struct CostModel {
  double fixed_us = 2000;    // per step, independent of batch size (think: weights read once)
  double per_seq_us = 125;   // per sequence in the batch
  double prefill_us = 500;   // extra, once per sequence on its first step
  double step_us(size_t batch, size_t new_seqs = 0) const {
    return fixed_us + per_seq_us * static_cast<double>(batch) + prefill_us * static_cast<double>(new_seqs);
  }
};

// Deadline-aware admission: predict when a new request would finish if admitted now.
struct LoadSnapshot {
  size_t ahead = 0;          // waiting requests that will be served before this one
  size_t active = 0;         // sequences currently in some batch
  size_t total_slots = 1;    // workers * max_batch
  size_t workers = 1;
  size_t max_batch = 1;
  double avg_tokens = 32;    // running average of admitted max_tokens
  double slowdown = 1.0;     // measured step time / modelled step time (sleep overshoot etc.)
};

inline double predict_finish_us(const LoadSnapshot& s, const CostModel& c, uint32_t my_tokens) {
  const size_t free_slots = s.total_slots > s.active ? s.total_slots - s.active : 0;
  const double full_step = c.step_us(s.max_batch) * s.slowdown;
  double wait = 0;
  if (s.ahead >= free_slots) {
    // When saturated, slots free up at about total_slots / (avg_tokens * full_step) per us.
    const double slots_per_us = static_cast<double>(s.total_slots) / (std::max(1.0, s.avg_tokens) * full_step);
    wait = static_cast<double>(s.ahead - free_slots + 1) / slots_per_us;
  }
  // Batch size we expect to share a worker with once we are running.
  const size_t per_worker = std::min(s.max_batch, (s.active + s.ahead + 1 + s.workers - 1) / s.workers);
  const double my_step = c.step_us(std::max<size_t>(1, per_worker)) * s.slowdown;
  return wait + c.prefill_us * s.slowdown + my_step * my_tokens;
}

// AIMD concurrency limit (in the spirit of Netflix's concurrency-limits library).
// The limit caps in-flight requests (waiting + running). Fast successes grow it
// additively (~+1 per "window" of limit completions); a deadline miss or a slow
// success shrinks it multiplicatively.
class AimdLimit {
 public:
  AimdLimit(double initial, double min_limit, double max_limit, double backoff = 0.9)
      : limit_(initial), min_(min_limit), max_(max_limit), backoff_(backoff) {}
  bool allow(size_t inflight) const { return static_cast<double>(inflight) < limit_; }
  // ok=false: request missed its deadline / was dropped. slow=true: finished but
  // used most of its latency budget. inflight: current in-flight count.
  void on_complete(bool ok, bool slow, size_t inflight) {
    if (!ok || slow) {
      limit_ = std::max(min_, limit_ * backoff_);
    } else if (static_cast<double>(inflight) * 2 >= limit_) {  // only grow when the limit is actually in use
      limit_ = std::min(max_, limit_ + 1.0 / limit_);
    }
  }
  double limit() const { return limit_; }

 private:
  double limit_, min_, max_, backoff_;
};

}  // namespace lrit
