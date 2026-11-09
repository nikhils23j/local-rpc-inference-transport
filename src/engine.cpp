#include "engine.hpp"

#include <stdexcept>

namespace lrit {
namespace {
double us_between(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double, std::micro>(b - a).count();
}
}  // namespace

BatchMode parse_mode(const std::string& s) {
  if (s == "none") return BatchMode::None;
  if (s == "static") return BatchMode::Static;
  if (s == "continuous") return BatchMode::Continuous;
  throw std::invalid_argument("batch mode must be none|static|continuous");
}

Policy parse_policy(const std::string& s) {
  if (s == "unbounded") return Policy::Unbounded;
  if (s == "taildrop") return Policy::TailDrop;
  if (s == "deadline") return Policy::DeadlineAware;
  if (s == "adaptive") return Policy::Adaptive;
  throw std::invalid_argument("policy must be unbounded|taildrop|deadline|adaptive");
}

const char* to_string(Admit a) {
  switch (a) {
    case Admit::Accepted: return "accepted";
    case Admit::QueueFull: return "queue full";
    case Admit::WouldMissDeadline: return "would miss deadline";
    case Admit::OverLimit: return "over adaptive concurrency limit";
  }
  return "?";
}

Engine::Engine(EngineConfig cfg)
    : cfg_(cfg),
      queue_(cfg.priority),
      aimd_(cfg.aimd_initial, cfg.aimd_min, cfg.aimd_max, cfg.aimd_backoff) {
  if (cfg_.mode == BatchMode::None) cfg_.max_batch = 1;
  if (cfg_.max_batch < 1) cfg_.max_batch = 1;
  for (unsigned i = 0; i < cfg_.workers; ++i) workers_.emplace_back([this] { worker_loop(); });
}

Engine::~Engine() {
  {
    std::lock_guard lk(mu_);
    stopping_ = true;
  }
  cv_.notify_all();
  for (auto& w : workers_) w.join();
  std::lock_guard lk(mu_);
  while (!queue_.empty()) {
    auto s = queue_.pop();
    std::lock_guard sl(s->m);
    s->outcome = Outcome::Cancelled;
    s->cv.notify_all();
  }
}

Admit Engine::submit(const std::shared_ptr<Sequence>& seq) {
  std::unique_lock lk(mu_);
  const bool bounded = cfg_.policy != Policy::Unbounded;
  if (bounded && queue_.size() >= cfg_.queue_capacity) {
    ++stats_.rejected_queue_full;
    return Admit::QueueFull;
  }
  if (cfg_.policy == Policy::DeadlineAware && seq->deadline != Clock::time_point::max()) {
    LoadSnapshot snap;
    snap.ahead = queue_.ahead_of(seq->priority);
    snap.active = active_;
    snap.workers = cfg_.workers ? cfg_.workers : 1;
    snap.max_batch = cfg_.max_batch;
    snap.total_slots = snap.workers * cfg_.max_batch;
    snap.avg_tokens = avg_tokens_;
    snap.slowdown = slowdown_;
    const double predicted = predict_finish_us(snap, cfg_.cost, seq->max_tokens);
    const double budget = us_between(Clock::now(), seq->deadline);
    if (predicted > cfg_.deadline_slack * budget) {
      ++stats_.rejected_deadline;
      return Admit::WouldMissDeadline;
    }
    seq->predicted_us = predicted;
  }
  if (cfg_.policy == Policy::Adaptive && !aimd_.allow(inflight_)) {
    ++stats_.rejected_limit;
    return Admit::OverLimit;
  }
  queue_.push(seq, seq->priority);
  ++inflight_;
  ++stats_.admitted;
  avg_tokens_ = 0.95 * avg_tokens_ + 0.05 * seq->max_tokens;
  stats_.peak_waiting = std::max<uint64_t>(stats_.peak_waiting, queue_.size());
  lk.unlock();
  if (cfg_.mode == BatchMode::Static) cv_.notify_all(); else cv_.notify_one();
  return Admit::Accepted;
}

EngineStats Engine::stats() const {
  std::lock_guard lk(mu_);
  EngineStats s = stats_;
  s.adaptive_limit = aimd_.limit();
  return s;
}

std::vector<uint32_t> Engine::step_log() const {
  std::lock_guard lk(mu_);
  return step_log_;
}

std::shared_ptr<Sequence> Engine::pop_runnable_locked(Clock::time_point now) {
  while (!queue_.empty()) {
    auto s = queue_.pop();
    if (s->cancelled || now >= s->deadline) {
      finish(s, s->cancelled ? Outcome::Cancelled : Outcome::Expired, /*in_queue=*/true);
      continue;
    }
    return s;
  }
  return nullptr;
}

// Requires mu_.
void Engine::finish(const std::shared_ptr<Sequence>& s, Outcome outcome, bool in_queue) {
  --inflight_;
  if (!in_queue) --active_;
  if (outcome == Outcome::Done) {
    ++stats_.completed;
    if (s->predicted_us > 0) {
      const double ratio = us_between(s->arrival, Clock::now()) / s->predicted_us;
      ++stats_.pred_n;
      stats_.pred_sum_ratio += ratio;
      if (ratio > 1) ++stats_.pred_under;
    }
  } else if (in_queue) {
    ++stats_.expired_in_queue;
  } else {
    ++stats_.dropped_active;
    stats_.tokens_wasted += s->produced;
  }
  if (cfg_.policy == Policy::Adaptive) {
    const bool ok = outcome == Outcome::Done;
    bool slow = false;
    if (ok && s->deadline != Clock::time_point::max()) {
      const double budget = us_between(s->arrival, s->deadline);
      slow = us_between(s->arrival, Clock::now()) > cfg_.slow_fraction * budget;
    }
    aimd_.on_complete(ok, slow, inflight_);
  }
  std::lock_guard sl(s->m);
  s->outcome = outcome;
  s->cv.notify_all();
}

void Engine::worker_loop() {
  prefer_low_latency_thread();
  std::vector<std::shared_ptr<Sequence>> batch;
  for (;;) {
    size_t new_seqs = 0;
    {
      std::unique_lock lk(mu_);
      // Step boundary: drop running sequences that were cancelled or ran out of time.
      const auto now = Clock::now();
      for (auto it = batch.begin(); it != batch.end();) {
        if ((*it)->cancelled || now >= (*it)->deadline) {
          finish(*it, (*it)->cancelled ? Outcome::Cancelled : Outcome::Expired, false);
          it = batch.erase(it);
        } else {
          ++it;
        }
      }
      if (batch.empty()) {
        cv_.wait(lk, [&] { return stopping_ || !queue_.empty(); });
        if (stopping_) break;
        if (cfg_.mode == BatchMode::Static) {
          // Static batching: give the batch a chance to fill before starting it.
          const auto until = Clock::now() + std::chrono::microseconds(cfg_.max_wait_us);
          cv_.wait_until(lk, until, [&] { return stopping_ || queue_.size() >= cfg_.max_batch; });
          if (stopping_) break;
        }
      }
      // Continuous batching admits new sequences at every step boundary;
      // static batching only when the previous batch has fully drained.
      if (cfg_.mode != BatchMode::Static || batch.empty()) {
        const auto t = Clock::now();
        while (batch.size() < cfg_.max_batch) {
          auto s = pop_runnable_locked(t);
          if (!s) break;
          s->started = t;
          batch.push_back(std::move(s));
          ++active_;
          ++new_seqs;
        }
      }
    }
    if (batch.empty()) continue;

    // "Compute": sleep for the modelled step time. No real model runs here.
    const double model_us = cfg_.cost.step_us(batch.size(), new_seqs);
    const auto t0 = Clock::now();
    // Sleep most of the step, then spin the last ~700 us: plain sleeps on macOS
    // overshoot by ~500 us (measured), which would quietly inflate the cost model.
    const auto t_end = t0 + std::chrono::microseconds(static_cast<int64_t>(model_us));
    std::this_thread::sleep_until(t_end - std::chrono::microseconds(700));
    while (Clock::now() < t_end) std::this_thread::yield();
    const double measured_us = us_between(t0, Clock::now());

    const auto bs = static_cast<uint32_t>(batch.size());
    for (auto& s : batch) {
      Token tok{s->produced, token_value(s->seed, s->produced), bs,
                s->produced == 0 ? static_cast<int64_t>(us_between(s->arrival, s->started)) : 0};
      ++s->produced;
      {
        std::lock_guard sl(s->m);
        s->out.push_back(tok);
      }
      s->cv.notify_all();
    }

    std::lock_guard lk(mu_);
    ++stats_.steps;
    stats_.tokens_generated += bs;
    stats_.sum_step_us += measured_us;
    stats_.max_batch_seen = std::max<uint64_t>(stats_.max_batch_seen, bs);
    slowdown_ = 0.95 * slowdown_ + 0.05 * (measured_us / model_us);
    if (step_log_.size() < 100000) step_log_.push_back(bs);
    for (auto it = batch.begin(); it != batch.end();) {
      if ((*it)->produced >= (*it)->max_tokens) {
        finish(*it, Outcome::Done, false);
        it = batch.erase(it);
      } else {
        ++it;
      }
    }
  }
  // Shutting down: release anything still running.
  std::lock_guard lk(mu_);
  for (auto& s : batch) finish(s, Outcome::Cancelled, false);
}

}  // namespace lrit
