// Unit tests for the batching scheduler and admission logic (no gRPC involved).
// Tiny self-contained harness so the project needs no extra test framework.
#include <cmath>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#include "admission.hpp"
#include "engine.hpp"

using namespace lrit;
using namespace std::chrono_literals;

static int failures = 0;
#define CHECK(cond)                                                          \
  do {                                                                       \
    if (!(cond)) {                                                           \
      std::fprintf(stderr, "  CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #cond); \
      ++failures;                                                            \
    }                                                                        \
  } while (0)

static std::shared_ptr<Sequence> make_seq(uint64_t id, uint32_t tokens, Priority p = Priority::Batch,
                                          std::chrono::milliseconds deadline = 10s) {
  auto s = std::make_shared<Sequence>();
  s->id = id;
  s->seed = mix(id);
  s->max_tokens = tokens;
  s->priority = p;
  s->arrival = Clock::now();
  s->deadline = s->arrival + deadline;
  return s;
}

// Waits for a final outcome and returns all tokens produced.
static std::vector<Token> drain(const std::shared_ptr<Sequence>& s, Outcome* outcome) {
  std::unique_lock l(s->m);
  s->cv.wait_for(l, 5s, [&] { return s->outcome != Outcome::Pending; });
  *outcome = s->outcome;
  return {s->out.begin(), s->out.end()};
}

static EngineConfig fast_config(unsigned workers, BatchMode mode, size_t max_batch) {
  EngineConfig c;
  c.workers = workers;
  c.mode = mode;
  c.max_batch = max_batch;
  c.max_wait_us = 100;
  c.cost.fixed_us = 300;
  c.cost.per_seq_us = 20;
  c.cost.prefill_us = 0;
  c.policy = Policy::Unbounded;
  return c;
}

static void test_aimd() {
  AimdLimit l(10, 2, 20, 0.5);
  CHECK(l.allow(9) && !l.allow(10));
  l.on_complete(true, false, 10);
  CHECK(std::abs(l.limit() - 10.1) < 1e-9);  // additive: +1/limit
  l.on_complete(true, false, 1);             // limit barely used -> no growth
  CHECK(std::abs(l.limit() - 10.1) < 1e-9);
  l.on_complete(false, false, 10);           // deadline miss -> multiplicative backoff
  CHECK(std::abs(l.limit() - 5.05) < 1e-9);
  l.on_complete(true, true, 5);              // slow success also backs off
  for (int i = 0; i < 20; ++i) l.on_complete(false, false, 5);
  CHECK(l.limit() == 2);                     // floor
  for (int i = 0; i < 10000; ++i) l.on_complete(true, false, 100);
  CHECK(l.limit() == 20);                    // ceiling
}

static void test_two_class_queue() {
  TwoClassQueue<int> q(true);
  q.push(1, Priority::Batch);
  q.push(2, Priority::Batch);
  q.push(3, Priority::Interactive);
  q.push(4, Priority::Interactive);
  CHECK(q.ahead_of(Priority::Interactive) == 2);
  CHECK(q.ahead_of(Priority::Batch) == 4);
  CHECK(q.pop() == 3 && q.pop() == 4 && q.pop() == 1 && q.pop() == 2 && q.empty());

  TwoClassQueue<int> fifo(false);
  fifo.push(1, Priority::Batch);
  fifo.push(2, Priority::Interactive);
  CHECK(fifo.ahead_of(Priority::Interactive) == 2);
  CHECK(fifo.pop() == 1 && fifo.pop() == 2);
}

static void test_predictor() {
  CostModel c{1000, 100, 500};
  LoadSnapshot s;
  s.workers = 2; s.max_batch = 4; s.total_slots = 8; s.avg_tokens = 10;
  // Idle server: prefill + tokens * step(batch of 1).
  CHECK(std::abs(predict_finish_us(s, c, 10) - (500 + 10 * 1100)) < 1e-6);
  // Saturated: waiting must be added and must grow with queue depth.
  s.active = 8; s.ahead = 0;
  const double one = predict_finish_us(s, c, 10);
  s.ahead = 16;
  const double many = predict_finish_us(s, c, 10);
  CHECK(one > 500 + 10 * 1100);
  CHECK(many > one);
  // Slowdown scales everything.
  s.slowdown = 2.0;
  CHECK(std::abs(predict_finish_us(s, c, 10) - 2 * many) < 1e-6);
}

static void test_continuous_respects_max_batch() {
  Engine e(fast_config(1, BatchMode::Continuous, 4));
  std::vector<std::shared_ptr<Sequence>> seqs;
  for (int i = 0; i < 10; ++i) {
    seqs.push_back(make_seq(i, 5));
    CHECK(e.submit(seqs.back()) == Admit::Accepted);
  }
  for (auto& s : seqs) {
    Outcome o;
    auto toks = drain(s, &o);
    CHECK(o == Outcome::Done);
    CHECK(toks.size() == 5);
    for (uint32_t k = 0; k < toks.size(); ++k) CHECK(toks[k].index == k && toks[k].value == token_value(s->seed, k));
  }
  auto st = e.stats();
  CHECK(st.completed == 10 && st.tokens_generated == 50);
  CHECK(st.max_batch_seen == 4);
  for (auto b : e.step_log()) CHECK(b >= 1 && b <= 4);
}

// A short request arriving while a long one runs: continuous batching lets it
// join at the next step and finish first; static batching makes it wait.
static void join_case(BatchMode mode, bool* short_finished_first) {
  auto cfg = fast_config(1, mode, 4);
  cfg.cost.fixed_us = 1000;
  Engine e(cfg);
  auto longer = make_seq(1, 30);
  CHECK(e.submit(longer) == Admit::Accepted);
  std::this_thread::sleep_for(5ms);
  auto shorter = make_seq(2, 2);
  CHECK(e.submit(shorter) == Admit::Accepted);
  Outcome o;
  drain(shorter, &o);
  CHECK(o == Outcome::Done);
  {
    std::lock_guard l(longer->m);
    *short_finished_first = longer->outcome == Outcome::Pending;
  }
  drain(longer, &o);
  CHECK(o == Outcome::Done);
}

static void test_continuous_vs_static_join() {
  bool first = false;
  join_case(BatchMode::Continuous, &first);
  CHECK(first);
  join_case(BatchMode::Static, &first);
  CHECK(!first);
}

static void test_cancel_running_sequence() {
  auto cfg = fast_config(1, BatchMode::Continuous, 4);
  cfg.cost.fixed_us = 500;
  Engine e(cfg);
  auto s = make_seq(7, 1000);
  CHECK(e.submit(s) == Admit::Accepted);
  std::this_thread::sleep_for(10ms);
  s->cancelled = true;
  Outcome o;
  auto toks = drain(s, &o);
  CHECK(o == Outcome::Cancelled);
  CHECK(!toks.empty() && toks.size() < 1000);
  auto st = e.stats();
  CHECK(st.dropped_active == 1 && st.tokens_wasted == toks.size());
}

static void test_taildrop_and_unbounded() {
  auto cfg = fast_config(0, BatchMode::Continuous, 4);  // no workers: nothing drains
  cfg.policy = Policy::TailDrop;
  cfg.queue_capacity = 3;
  {
    Engine e(cfg);
    for (int i = 0; i < 3; ++i) CHECK(e.submit(make_seq(i, 1)) == Admit::Accepted);
    CHECK(e.submit(make_seq(9, 1)) == Admit::QueueFull);
    CHECK(e.stats().rejected_queue_full == 1);
  }
  cfg.policy = Policy::Unbounded;
  Engine u(cfg);
  for (int i = 0; i < 100; ++i) CHECK(u.submit(make_seq(i, 1)) == Admit::Accepted);
}

static void test_expired_in_queue() {
  auto cfg = fast_config(1, BatchMode::None, 1);
  cfg.cost.fixed_us = 5000;
  Engine e(cfg);
  auto a = make_seq(1, 4);
  auto b = make_seq(2, 4, Priority::Batch, 2ms);
  CHECK(e.submit(a) == Admit::Accepted);
  CHECK(e.submit(b) == Admit::Accepted);
  Outcome oa, ob;
  drain(a, &oa);
  auto tb = drain(b, &ob);
  CHECK(oa == Outcome::Done && ob == Outcome::Expired && tb.empty());
  auto st = e.stats();
  CHECK(st.expired_in_queue == 1 && st.tokens_wasted == 0);
}

static void test_deadline_aware_admission() {
  auto cfg = fast_config(0, BatchMode::Continuous, 4);
  cfg.policy = Policy::DeadlineAware;
  cfg.cost.fixed_us = 10000;  // 10 tokens -> >= 100 ms
  Engine e(cfg);
  CHECK(e.submit(make_seq(1, 10, Priority::Batch, 20ms)) == Admit::WouldMissDeadline);
  CHECK(e.submit(make_seq(2, 10, Priority::Batch, 10s)) == Admit::Accepted);
  CHECK(e.stats().rejected_deadline == 1);
}

static void test_adaptive_limit_admission() {
  auto cfg = fast_config(0, BatchMode::Continuous, 4);
  cfg.policy = Policy::Adaptive;
  cfg.aimd_initial = 4;
  cfg.queue_capacity = 100;
  Engine e(cfg);
  for (int i = 0; i < 4; ++i) CHECK(e.submit(make_seq(i, 1)) == Admit::Accepted);
  CHECK(e.submit(make_seq(5, 1)) == Admit::OverLimit);
}

static void test_priority_served_first() {
  auto cfg = fast_config(1, BatchMode::None, 1);
  cfg.cost.fixed_us = 2000;
  Engine e(cfg);
  auto blocker = make_seq(0, 3);
  CHECK(e.submit(blocker) == Admit::Accepted);
  std::this_thread::sleep_for(1ms);
  auto b = make_seq(1, 1, Priority::Batch);
  auto i = make_seq(2, 1, Priority::Interactive);
  CHECK(e.submit(b) == Admit::Accepted);
  CHECK(e.submit(i) == Admit::Accepted);
  Outcome o;
  drain(i, &o);
  bool batch_still_pending;
  {
    std::lock_guard l(b->m);
    batch_still_pending = b->outcome == Outcome::Pending;
  }
  CHECK(o == Outcome::Done && batch_still_pending);
  drain(b, &o);
  drain(blocker, &o);
}

int main() {
  const std::vector<std::pair<const char*, std::function<void()>>> tests = {
      {"aimd", test_aimd},
      {"two_class_queue", test_two_class_queue},
      {"predictor", test_predictor},
      {"continuous_respects_max_batch", test_continuous_respects_max_batch},
      {"continuous_vs_static_join", test_continuous_vs_static_join},
      {"cancel_running_sequence", test_cancel_running_sequence},
      {"taildrop_and_unbounded", test_taildrop_and_unbounded},
      {"expired_in_queue", test_expired_in_queue},
      {"deadline_aware_admission", test_deadline_aware_admission},
      {"adaptive_limit_admission", test_adaptive_limit_admission},
      {"priority_served_first", test_priority_served_first},
  };
  for (const auto& [name, fn] : tests) {
    const int before = failures;
    fn();
    std::printf("[%s] %s\n", failures == before ? "PASS" : "FAIL", name);
  }
  std::printf("%zu tests, %d failed checks\n", tests.size(), failures);
  return failures ? 1 : 0;
}
