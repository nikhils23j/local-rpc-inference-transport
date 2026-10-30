#include <grpcpp/grpcpp.h>
#include "inference.grpc.pb.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <queue>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

static uint64_t checksum(const std::string& data) {
  uint64_t h = 14695981039346656037ULL;
  for (unsigned char c : data) { h ^= c; h *= 1099511628211ULL; }
  return h;
}
static long arg(int argc, char** argv, const std::string& key, long fallback) {
  for (int i = 2; i + 1 < argc; ++i) if (argv[i] == key) return std::stol(argv[i + 1]);
  return fallback;
}
static std::string strarg(int argc, char** argv, const std::string& key, std::string fallback) {
  for (int i = 2; i + 1 < argc; ++i) if (argv[i] == key) return argv[i + 1];
  return fallback;
}
struct Job {
  transport::InferenceRequest request;
  transport::InferenceReply reply;
  Clock::time_point enqueued;
  std::atomic<bool> cancelled{false};
  bool done = false;
  std::mutex mutex;
  std::condition_variable cv;
};

class Service final : public transport::InferenceTransport::Service {
 public:
  Service(size_t capacity, unsigned workers, int work_us) : capacity_(capacity), work_us_(work_us) {
    for (unsigned i = 0; i < workers; ++i) workers_.emplace_back([this] { work(); });
  }
  ~Service() override {
    { std::lock_guard lock(mutex_); stopping_ = true; }
    cv_.notify_all();
    for (auto& worker : workers_) worker.join();
  }
  grpc::Status Send(grpc::ServerContext* context, const transport::InferenceRequest* request,
                    transport::InferenceReply* reply) override {
    if (request->payload().empty() || request->payload().size() > 65536)
      return {grpc::StatusCode::INVALID_ARGUMENT, "payload must be 1..65536 bytes"};
    auto job = std::make_shared<Job>();
    job->request = *request;
    job->enqueued = Clock::now();
    {
      std::lock_guard lock(mutex_);
      if (queue_.size() >= capacity_) return {grpc::StatusCode::RESOURCE_EXHAUSTED, "queue full"};
      queue_.push(job);
    }
    cv_.notify_one();
    std::unique_lock lock(job->mutex);
    while (!job->done) {
      job->cv.wait_for(lock, 1ms);
      if (context->IsCancelled()) {
        job->cancelled = true;
        return {grpc::StatusCode::CANCELLED, "client cancelled or deadline exceeded"};
      }
    }
    *reply = job->reply;
    if (reply->checksum() != request->expected_checksum())
      return {grpc::StatusCode::DATA_LOSS, "checksum mismatch"};
    return grpc::Status::OK;
  }
 private:
  void work() {
    for (;;) {
      std::shared_ptr<Job> job;
      {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
        if (stopping_ && queue_.empty()) return;
        job = queue_.front(); queue_.pop();
      }
      if (job->cancelled) continue;
      const auto start = Clock::now();
      auto sum = checksum(job->request.payload());
      if (work_us_ > 0) std::this_thread::sleep_for(std::chrono::microseconds(work_us_));
      const auto finish = Clock::now();
      {
        std::lock_guard lock(job->mutex);
        job->reply.set_id(job->request.id());
        job->reply.set_checksum(sum);
        job->reply.set_queue_us(std::chrono::duration_cast<std::chrono::microseconds>(start - job->enqueued).count());
        job->reply.set_service_us(std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count());
        job->done = true;
      }
      job->cv.notify_one();
    }
  }
  size_t capacity_;
  int work_us_;
  bool stopping_ = false;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::queue<std::shared_ptr<Job>> queue_;
  std::vector<std::thread> workers_;
};

static std::string payload(uint64_t id, size_t size) {
  std::string data(size, '\0');
  uint64_t x = id + 0x9e3779b97f4a7c15ULL;
  for (size_t i = 0; i < size; ++i) { x ^= x >> 12; x ^= x << 25; x ^= x >> 27; data[i] = static_cast<char>(x & 255); }
  return data;
}
static double pct(std::vector<long>& values, double p) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  return values[std::min(values.size()-1, static_cast<size_t>((values.size()-1)*p))];
}
int main(int argc, char** argv) {
  if (argc < 2) { std::cerr << "usage: transport server|client [options]\n"; return 2; }
  std::string mode = argv[1];
  auto port = arg(argc, argv, "--port", 50051);
  if (mode == "server") {
    auto capacity = arg(argc, argv, "--capacity", 64);
    auto workers = arg(argc, argv, "--workers", 4);
    auto work_us = arg(argc, argv, "--work-us", 0);
    if (capacity < 1 || workers < 1 || workers > 256 || work_us < 0) return 2;
    Service service(capacity, workers, work_us);
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:" + std::to_string(port), grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server) return 1;
    std::cerr << "ready port=" << port << " workers=" << workers << " capacity=" << capacity << "\n";
    server->Wait();
    return 0;
  }
  if (mode != "client") return 2;
  auto count = arg(argc, argv, "--count", 1000);
  auto concurrency = arg(argc, argv, "--concurrency", 8);
  auto size = arg(argc, argv, "--payload-bytes", 1024);
  auto deadline_ms = arg(argc, argv, "--deadline-ms", 5000);
  if (count < 1 || concurrency < 1 || concurrency > 256 || size < 1 || size > 65536 || deadline_ms < 1) return 2;
  auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials());
  const bool corrupt_checksum = arg(argc, argv, "--corrupt-checksum", 0) != 0;
  std::atomic<long> next{0}, ok{0}, rejected{0}, deadline{0}, data_loss{0}, errors{0};
  std::mutex times_mutex;
  std::vector<long> latencies;
  auto begin = Clock::now();
  std::vector<std::thread> threads;
  for (int t = 0; t < concurrency; ++t) threads.emplace_back([&] {
    auto stub = transport::InferenceTransport::NewStub(channel);
    for (;;) {
      auto id = next.fetch_add(1);
      if (id >= count) break;
      transport::InferenceRequest request;
      request.set_id(id);
      auto data = payload(id, size);
      request.set_payload(data);
      request.set_expected_checksum(checksum(data) ^ (corrupt_checksum ? 1ULL : 0ULL));
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_ms));
      transport::InferenceReply reply;
      auto start = Clock::now();
      auto status = stub->Send(&context, request, &reply);
      auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
      if (status.ok() && reply.id() == static_cast<uint64_t>(id) && reply.checksum() == request.expected_checksum()) {
        ok++;
        std::lock_guard lock(times_mutex); latencies.push_back(us);
      } else if (status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED) rejected++;
      else if (status.error_code() == grpc::StatusCode::DATA_LOSS) data_loss++;
      else if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED || status.error_code() == grpc::StatusCode::CANCELLED) deadline++;
      else errors++;
    }
  });
  for (auto& thread : threads) thread.join();
  auto seconds = std::chrono::duration<double>(Clock::now()-begin).count();
  auto p50 = pct(latencies, .50), p95 = pct(latencies, .95);
  std::cout << "{\"count\":" << count << ",\"payload_bytes\":" << size
            << ",\"concurrency\":" << concurrency << ",\"ok\":" << ok
            << ",\"rejected\":" << rejected << ",\"deadline\":" << deadline
            << ",\"data_loss\":" << data_loss << ",\"errors\":" << errors << ",\"seconds\":" << seconds
            << ",\"requests_per_second\":" << ok/seconds
            << ",\"p50_us\":" << p50 << ",\"p95_us\":" << p95 << "}\n";
  return errors ? 1 : 0;
}
