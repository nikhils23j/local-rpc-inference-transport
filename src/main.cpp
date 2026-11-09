// transport: a mini LLM-serving-shaped gRPC front end (server) and an
// open-loop load generator (client). Compute is synthetic (sleep per step).
#include <grpcpp/grpcpp.h>

#include <csignal>
#include <cstdio>
#include <iostream>
#include <map>
#include <random>
#include <sstream>

#include "common.hpp"
#include "engine.hpp"
#include "inference.grpc.pb.h"

using namespace lrit;
using namespace std::chrono_literals;

namespace {

// ---------- tiny flag parser ----------
struct Args {
  std::map<std::string, std::string> kv;
  Args(int argc, char** argv) {
    for (int i = 2; i < argc; ++i) {
      std::string k = argv[i];
      if (k.rfind("--", 0) != 0 || i + 1 >= argc) throw std::invalid_argument("bad flag: " + k);
      kv[k] = argv[++i];
    }
  }
  double num(const std::string& k, double fallback) const {
    auto it = kv.find(k);
    return it == kv.end() ? fallback : std::stod(it->second);
  }
  std::string str(const std::string& k, const std::string& fallback) const {
    auto it = kv.find(k);
    return it == kv.end() ? fallback : it->second;
  }
};

void require(bool ok, const char* what) {
  if (!ok) throw std::invalid_argument(what);
}

// ---------- server ----------
class Service final : public transport::InferenceTransport::Service {
 public:
  explicit Service(EngineConfig cfg) : engine_(cfg) {}

  grpc::Status Generate(grpc::ServerContext* ctx, const transport::GenerateRequest* req,
                        grpc::ServerWriter<transport::TokenChunk>* writer) override {
    if (req->prompt().empty() || req->prompt().size() > 65536)
      return {grpc::StatusCode::INVALID_ARGUMENT, "prompt must be 1..65536 bytes"};
    if (req->max_tokens() < 1 || req->max_tokens() > 1024)
      return {grpc::StatusCode::INVALID_ARGUMENT, "max_tokens must be 1..1024"};
    const uint64_t sum = checksum(req->prompt());
    if (sum != req->expected_checksum()) return {grpc::StatusCode::DATA_LOSS, "prompt checksum mismatch"};

    auto seq = std::make_shared<Sequence>();
    seq->id = req->id();
    seq->seed = sum;
    seq->max_tokens = req->max_tokens();
    seq->priority = req->priority() == transport::INTERACTIVE ? Priority::Interactive : Priority::Batch;
    seq->arrival = Clock::now();
    const auto sys_deadline = ctx->deadline();
    if (sys_deadline != std::chrono::system_clock::time_point::max()) {
      const auto left = sys_deadline - std::chrono::system_clock::now();
      seq->deadline = seq->arrival + std::chrono::duration_cast<Clock::duration>(left);
    }

    const Admit a = engine_.submit(seq);
    if (a != Admit::Accepted) return {grpc::StatusCode::RESOURCE_EXHAUSTED, to_string(a)};

    for (;;) {
      std::deque<Token> chunk;
      Outcome outcome;
      {
        std::unique_lock l(seq->m);
        seq->cv.wait_for(l, 5ms, [&] { return !seq->out.empty() || seq->outcome != Outcome::Pending; });
        chunk.swap(seq->out);
        outcome = seq->outcome;  // tokens are always pushed before the outcome is set
      }
      for (const auto& t : chunk) {
        transport::TokenChunk c;
        c.set_index(t.index);
        c.set_token(t.value);
        c.set_batch_size(t.batch_size);
        c.set_queue_us(static_cast<uint64_t>(std::max<int64_t>(0, t.queue_us)));
        if (!writer->Write(c)) {
          seq->cancelled = true;
          return {grpc::StatusCode::CANCELLED, "client went away"};
        }
      }
      switch (outcome) {
        case Outcome::Done: return grpc::Status::OK;
        case Outcome::Expired: return {grpc::StatusCode::DEADLINE_EXCEEDED, "deadline passed in server"};
        case Outcome::Cancelled: return {grpc::StatusCode::CANCELLED, "cancelled"};
        case Outcome::Pending: break;
      }
      if (ctx->IsCancelled()) {
        seq->cancelled = true;  // engine drops it at the next step boundary / dequeue
        return {grpc::StatusCode::CANCELLED, "client cancelled or deadline exceeded"};
      }
    }
  }

  grpc::Status Stats(grpc::ServerContext*, const transport::StatsRequest*, transport::ServerStats* out) override {
    const auto s = engine_.stats();
    out->set_admitted(s.admitted);
    out->set_rejected_queue_full(s.rejected_queue_full);
    out->set_rejected_deadline(s.rejected_deadline);
    out->set_rejected_limit(s.rejected_limit);
    out->set_completed(s.completed);
    out->set_expired_in_queue(s.expired_in_queue);
    out->set_dropped_active(s.dropped_active);
    out->set_tokens_generated(s.tokens_generated);
    out->set_tokens_wasted(s.tokens_wasted);
    out->set_steps(s.steps);
    out->set_mean_batch(s.mean_batch());
    out->set_mean_step_us(s.mean_step_us());
    out->set_peak_waiting(s.peak_waiting);
    out->set_adaptive_limit(s.adaptive_limit);
    out->set_predictor_mean_ratio(s.pred_n ? s.pred_sum_ratio / static_cast<double>(s.pred_n) : 0);
    out->set_predictor_underestimate_frac(s.pred_n ? static_cast<double>(s.pred_under) / static_cast<double>(s.pred_n) : 0);
    return grpc::Status::OK;
  }

 private:
  Engine engine_;
};

int run_server(const Args& a) {
  EngineConfig cfg;
  const int port = static_cast<int>(a.num("--port", 50051));
  cfg.workers = static_cast<unsigned>(a.num("--workers", 2));
  cfg.mode = parse_mode(a.str("--mode", "continuous"));
  cfg.max_batch = static_cast<size_t>(a.num("--max-batch", 16));
  cfg.max_wait_us = static_cast<int64_t>(a.num("--max-wait-us", 5000));
  cfg.cost.fixed_us = a.num("--step-fixed-us", 2000);
  cfg.cost.per_seq_us = a.num("--step-per-seq-us", 125);
  cfg.cost.prefill_us = a.num("--prefill-us", 500);
  cfg.policy = parse_policy(a.str("--policy", "taildrop"));
  cfg.queue_capacity = static_cast<size_t>(a.num("--capacity", 64));
  cfg.priority = a.num("--priority", 1) != 0;
  cfg.aimd_initial = a.num("--aimd-initial", 32);
  cfg.deadline_slack = a.num("--deadline-slack", 1.0);
  require(cfg.workers >= 1 && cfg.workers <= 64, "--workers must be 1..64");
  require(cfg.max_batch >= 1 && cfg.max_batch <= 256, "--max-batch must be 1..256");
  require(cfg.queue_capacity >= 1, "--capacity must be >= 1");
  require(cfg.cost.fixed_us >= 0 && cfg.cost.per_seq_us >= 0 && cfg.cost.prefill_us >= 0, "costs must be >= 0");

  // Handle SIGINT/SIGTERM on a dedicated thread so the server shuts down cleanly.
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);

  Service service(cfg);
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:" + std::to_string(port), grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  if (!server) {
    std::cerr << "failed to bind 127.0.0.1:" << port << "\n";
    return 1;
  }
  std::thread sig([&] {
    int s = 0;
    sigwait(&set, &s);
    server->Shutdown(std::chrono::system_clock::now() + 2s);
  });
  std::cerr << "ready port=" << port << " workers=" << cfg.workers << " mode=" << a.str("--mode", "continuous")
            << " max_batch=" << cfg.max_batch << " policy=" << a.str("--policy", "taildrop")
            << " capacity=" << cfg.queue_capacity << " priority=" << cfg.priority << std::endl;
  server->Wait();
  sig.join();
  return 0;
}

// ---------- client: open-loop Poisson load generator ----------
struct Planned {
  double at_us;
  Priority cls;
  uint32_t tokens;
};

enum class Result { Ok, Rejected, Deadline, DataLoss, BadStream, Error };

struct Record {
  Priority cls = Priority::Batch;
  Result result = Result::Error;
  double latency_ms = 0, ttft_ms = -1, lag_ms = 0;
  uint32_t tokens = 0;
};

std::string summary_json(const std::vector<Record>& recs, bool want_class, Priority cls, double span_s) {
  long sent = 0, ok = 0, rej = 0, dl = 0, dl_after_first = 0, dloss = 0, bad = 0, err = 0, tokens_ok = 0;
  std::vector<double> lat, ttft;
  for (const auto& r : recs) {
    if (want_class && r.cls != cls) continue;
    ++sent;
    switch (r.result) {
      case Result::Ok: ++ok; lat.push_back(r.latency_ms); ttft.push_back(r.ttft_ms); tokens_ok += r.tokens; break;
      case Result::Rejected: ++rej; break;
      case Result::Deadline: ++dl; if (r.ttft_ms >= 0) ++dl_after_first; break;
      case Result::DataLoss: ++dloss; break;
      case Result::BadStream: ++bad; break;
      case Result::Error: ++err; break;
    }
  }
  std::ostringstream o;
  o.setf(std::ios::fixed);
  o.precision(3);
  o << "{\"sent\":" << sent << ",\"ok\":" << ok << ",\"rejected\":" << rej << ",\"deadline\":" << dl
    << ",\"deadline_after_first_token\":" << dl_after_first << ",\"data_loss\":" << dloss
    << ",\"bad_stream\":" << bad << ",\"errors\":" << err << ",\"tokens_ok\":" << tokens_ok
    << ",\"goodput_rps\":" << (span_s > 0 ? ok / span_s : 0)
    << ",\"latency_ms\":{\"p50\":" << percentile(lat, .5) << ",\"p95\":" << percentile(lat, .95)
    << ",\"p99\":" << percentile(lat, .99) << "}"
    << ",\"ttft_ms\":{\"p50\":" << percentile(ttft, .5) << ",\"p95\":" << percentile(ttft, .95)
    << ",\"p99\":" << percentile(ttft, .99) << "}}";
  return o.str();
}

int run_client(const Args& a) {
  const int port = static_cast<int>(a.num("--port", 50051));
  const double rate = a.num("--rate", 100);                // requests/s (Poisson)
  const double duration_s = a.num("--duration-s", 5);
  const long count_flag = static_cast<long>(a.num("--count", 0));  // if > 0: exactly this many arrivals
  const uint64_t seed = static_cast<uint64_t>(a.num("--seed", 1));
  const double ifrac = a.num("--interactive-frac", 0);
  const auto i_min = static_cast<uint32_t>(a.num("--i-tokens-min", 8)), i_max = static_cast<uint32_t>(a.num("--i-tokens-max", 24));
  const auto b_min = static_cast<uint32_t>(a.num("--b-tokens-min", 16)), b_max = static_cast<uint32_t>(a.num("--b-tokens-max", 48));
  const double i_deadline_ms = a.num("--i-deadline-ms", 500), b_deadline_ms = a.num("--b-deadline-ms", 1000);
  const auto prompt_bytes = static_cast<size_t>(a.num("--prompt-bytes", 1024));
  const int threads_n = static_cast<int>(a.num("--threads", 512));
  const int channels_n = static_cast<int>(a.num("--channels", 4));
  const bool corrupt = a.num("--corrupt-checksum", 0) != 0;
  require(rate > 0 && duration_s > 0, "--rate and --duration-s must be > 0");
  require(ifrac >= 0 && ifrac <= 1, "--interactive-frac must be 0..1");
  require(i_min >= 1 && i_min <= i_max && i_max <= 1024 && b_min >= 1 && b_min <= b_max && b_max <= 1024, "bad token range");
  require(prompt_bytes >= 1 && prompt_bytes <= 65536, "--prompt-bytes must be 1..65536");
  require(threads_n >= 1 && threads_n <= 2048 && channels_n >= 1 && channels_n <= 64, "bad --threads/--channels");

  // Arrival schedule is fixed up front (seeded), so a slow server cannot slow the arrivals down.
  std::mt19937_64 rng(seed);
  std::exponential_distribution<double> gap(rate / 1e6);
  std::uniform_real_distribution<double> u(0, 1);
  std::vector<Planned> plan;
  double t = 0;
  while (count_flag > 0 ? static_cast<long>(plan.size()) < count_flag : t < duration_s * 1e6) {
    const bool inter = u(rng) < ifrac;
    const uint32_t lo = inter ? i_min : b_min, hi = inter ? i_max : b_max;
    std::uniform_int_distribution<uint32_t> tok(lo, hi);
    plan.push_back({t, inter ? Priority::Interactive : Priority::Batch, tok(rng)});
    t += gap(rng);
  }
  const double span_s = count_flag > 0 ? std::max(t / 1e6, 1e-3) : duration_s;

  std::vector<std::shared_ptr<grpc::Channel>> channels;
  for (int c = 0; c < channels_n; ++c) {
    grpc::ChannelArguments args;
    args.SetInt("lrit.channel_index", c);  // distinct args -> distinct TCP connections
    args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    channels.push_back(grpc::CreateCustomChannel("127.0.0.1:" + std::to_string(port),
                                                 grpc::InsecureChannelCredentials(), args));
  }
  for (auto& ch : channels) ch->WaitForConnected(std::chrono::system_clock::now() + 5s);

  std::vector<Record> recs(plan.size());
  std::atomic<size_t> next{0};
  const auto start = Clock::now() + 50ms;
  std::vector<std::thread> pool;
  for (int w = 0; w < threads_n; ++w) {
    pool.emplace_back([&, w] {
      prefer_low_latency_thread();
      auto stub = transport::InferenceTransport::NewStub(channels[w % channels.size()]);
      for (;;) {
        const size_t i = next.fetch_add(1);
        if (i >= plan.size()) return;
        const auto& p = plan[i];
        const auto sched = start + std::chrono::microseconds(static_cast<int64_t>(p.at_us));
        std::this_thread::sleep_until(sched);
        Record& r = recs[i];
        r.cls = p.cls;
        r.lag_ms = std::chrono::duration<double, std::milli>(Clock::now() - sched).count();

        transport::GenerateRequest req;
        req.set_id(i);
        std::string prompt = make_prompt(i, prompt_bytes);
        const uint64_t sum = checksum(prompt);
        req.set_prompt(std::move(prompt));
        req.set_expected_checksum(sum ^ (corrupt ? 1ULL : 0ULL));
        req.set_max_tokens(p.tokens);
        req.set_priority(p.cls == Priority::Interactive ? transport::INTERACTIVE : transport::BATCH);

        // Deadline counts from the *scheduled* arrival: client lag eats into it.
        const double dl_ms = p.cls == Priority::Interactive ? i_deadline_ms : b_deadline_ms;
        const auto steady_deadline = sched + std::chrono::microseconds(static_cast<int64_t>(dl_ms * 1000));
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::duration_cast<std::chrono::system_clock::duration>(steady_deadline - Clock::now()));
        auto reader = stub->Generate(&ctx, req);
        transport::TokenChunk c;
        uint32_t n = 0;
        bool stream_ok = true;
        while (reader->Read(&c)) {
          if (n == 0) r.ttft_ms = std::chrono::duration<double, std::milli>(Clock::now() - sched).count();
          if (c.index() != n || c.token() != token_value(sum, n)) stream_ok = false;
          ++n;
        }
        const auto status = reader->Finish();
        // Latency is measured from the scheduled send time (corrects for coordinated omission).
        r.latency_ms = std::chrono::duration<double, std::milli>(Clock::now() - sched).count();
        r.tokens = n;
        switch (status.error_code()) {
          case grpc::StatusCode::OK: r.result = (stream_ok && n == p.tokens) ? Result::Ok : Result::BadStream; break;
          case grpc::StatusCode::RESOURCE_EXHAUSTED: r.result = Result::Rejected; break;
          case grpc::StatusCode::DEADLINE_EXCEEDED:
          case grpc::StatusCode::CANCELLED: r.result = Result::Deadline; break;
          case grpc::StatusCode::DATA_LOSS: r.result = Result::DataLoss; break;
          default: r.result = Result::Error; break;
        }
        if (!stream_ok && r.result != Result::Ok) r.result = Result::BadStream;
      }
    });
  }
  for (auto& th : pool) th.join();
  const double wall_s = std::chrono::duration<double>(Clock::now() - start).count();

  std::vector<double> lags;
  for (const auto& r : recs) lags.push_back(r.lag_ms);
  long unexpected = 0;
  for (const auto& r : recs) unexpected += (r.result == Result::Error || r.result == Result::BadStream);

  transport::ServerStats ss;
  {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);
    auto stub = transport::InferenceTransport::NewStub(channels[0]);
    stub->Stats(&ctx, transport::StatsRequest{}, &ss);
  }

  std::ostringstream o;
  o.setf(std::ios::fixed);
  o.precision(3);
  o << "{\"offered_rps\":" << plan.size() / span_s << ",\"span_s\":" << span_s << ",\"wall_s\":" << wall_s
    << ",\"client_lag_ms\":{\"p99\":" << percentile(lags, .99) << ",\"max\":" << percentile(lags, 1.0) << "}"
    << ",\"all\":" << summary_json(recs, false, Priority::Batch, span_s)
    << ",\"interactive\":" << summary_json(recs, true, Priority::Interactive, span_s)
    << ",\"batch\":" << summary_json(recs, true, Priority::Batch, span_s)
    << ",\"server\":{\"admitted\":" << ss.admitted() << ",\"rejected_queue_full\":" << ss.rejected_queue_full()
    << ",\"rejected_deadline\":" << ss.rejected_deadline() << ",\"rejected_limit\":" << ss.rejected_limit()
    << ",\"completed\":" << ss.completed() << ",\"expired_in_queue\":" << ss.expired_in_queue()
    << ",\"dropped_active\":" << ss.dropped_active() << ",\"tokens_generated\":" << ss.tokens_generated()
    << ",\"tokens_wasted\":" << ss.tokens_wasted() << ",\"steps\":" << ss.steps()
    << ",\"mean_batch\":" << ss.mean_batch() << ",\"mean_step_us\":" << ss.mean_step_us()
    << ",\"peak_waiting\":" << ss.peak_waiting() << ",\"adaptive_limit\":" << ss.adaptive_limit()
    << ",\"predictor_mean_ratio\":" << ss.predictor_mean_ratio()
    << ",\"predictor_underestimate_frac\":" << ss.predictor_underestimate_frac() << "}}\n";
  std::cout << o.str();
  return unexpected ? 1 : 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage: transport server|client [--flag value ...]  (see README)\n";
    return 2;
  }
  try {
    const std::string mode = argv[1];
    Args a(argc, argv);
    if (mode == "server") return run_server(a);
    if (mode == "client") return run_client(a);
    std::cerr << "unknown mode " << mode << "\n";
    return 2;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 2;
  }
}
