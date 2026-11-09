#pragma once
// The "serving engine": a shared waiting queue, N replica workers, a batching
// scheduler and admission control. It knows nothing about gRPC; the server
// adapts a gRPC stream onto a Sequence, and the unit tests drive it directly.
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "admission.hpp"
#include "common.hpp"

namespace lrit {

enum class BatchMode { None, Static, Continuous };
enum class Policy { Unbounded, TailDrop, DeadlineAware, Adaptive };
enum class Admit { Accepted, QueueFull, WouldMissDeadline, OverLimit };
enum class Outcome { Pending, Done, Expired, Cancelled };

BatchMode parse_mode(const std::string& s);
Policy parse_policy(const std::string& s);
const char* to_string(Admit a);

struct EngineConfig {
  unsigned workers = 2;
  BatchMode mode = BatchMode::Continuous;
  size_t max_batch = 16;
  int64_t max_wait_us = 5000;        // static batching: how long to wait for a fuller batch
  CostModel cost;
  Policy policy = Policy::TailDrop;
  size_t queue_capacity = 64;        // hard cap on waiting requests (not used by Unbounded)
  bool priority = true;
  double aimd_initial = 32, aimd_min = 4, aimd_max = 1024, aimd_backoff = 0.9;
  double slow_fraction = 0.8;        // AIMD: success using > this fraction of its budget counts as "slow"
  double deadline_slack = 1.0;       // deadline-aware: admit if predicted <= slack * remaining budget
};

struct Token {
  uint32_t index;
  uint64_t value;
  uint32_t batch_size;
  int64_t queue_us;
};

// One request/sequence. The engine produces tokens into `out`; a consumer
// (gRPC handler or test) drains them. Setting `cancelled` makes the engine drop
// it at the next step boundary, even if it is already running.
struct Sequence {
  uint64_t id = 0;
  uint64_t seed = 0;
  uint32_t max_tokens = 1;
  Priority priority = Priority::Batch;
  Clock::time_point arrival = Clock::now();
  Clock::time_point deadline = Clock::time_point::max();
  std::atomic<bool> cancelled{false};

  // Engine-owned progress (touched only by the worker running it).
  uint32_t produced = 0;
  double predicted_us = 0;   // deadline-aware admission's prediction (0 = none)
  Clock::time_point started{};

  // Output channel.
  std::mutex m;
  std::condition_variable cv;
  std::deque<Token> out;
  Outcome outcome = Outcome::Pending;
};

struct EngineStats {
  uint64_t admitted = 0, rejected_queue_full = 0, rejected_deadline = 0, rejected_limit = 0;
  uint64_t completed = 0, expired_in_queue = 0, dropped_active = 0;
  uint64_t tokens_generated = 0, tokens_wasted = 0, steps = 0;
  uint64_t max_batch_seen = 0, peak_waiting = 0;
  double sum_step_us = 0, adaptive_limit = 0;
  // Deadline-aware predictor accuracy on requests that completed: actual / predicted.
  uint64_t pred_n = 0, pred_under = 0;
  double pred_sum_ratio = 0;
  double mean_batch() const { return steps ? static_cast<double>(tokens_generated) / static_cast<double>(steps) : 0; }
  double mean_step_us() const { return steps ? sum_step_us / static_cast<double>(steps) : 0; }
};

class Engine {
 public:
  explicit Engine(EngineConfig cfg);
  ~Engine();
  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;

  Admit submit(const std::shared_ptr<Sequence>& seq);
  EngineStats stats() const;
  const EngineConfig& config() const { return cfg_; }
  // Batch sizes of every step, in order (for tests; bounded to the first 100k steps).
  std::vector<uint32_t> step_log() const;

 private:
  void worker_loop();
  // Pops one runnable sequence, finishing expired/cancelled ones on the way. Requires mu_.
  std::shared_ptr<Sequence> pop_runnable_locked(Clock::time_point now);
  void finish(const std::shared_ptr<Sequence>& seq, Outcome outcome, bool in_queue);

  EngineConfig cfg_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  TwoClassQueue<std::shared_ptr<Sequence>> queue_;
  size_t active_ = 0;           // sequences in some worker's batch
  size_t inflight_ = 0;         // waiting + active
  double avg_tokens_ = 32;      // EWMA of admitted max_tokens
  double slowdown_ = 1.0;       // EWMA of measured/modelled step time
  AimdLimit aimd_;
  EngineStats stats_;
  std::vector<uint32_t> step_log_;
  bool stopping_ = false;
  std::vector<std::thread> workers_;
};

}  // namespace lrit
