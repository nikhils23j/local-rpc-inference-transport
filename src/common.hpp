#pragma once
// Small helpers shared by the engine, the gRPC server/client and the tests.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>
#ifdef __APPLE__
#include <pthread/qos.h>
#endif

namespace lrit {

using Clock = std::chrono::steady_clock;

// FNV-1a: integrity check for the synthetic prompt, NOT cryptographic.
inline uint64_t checksum(const std::string& data) {
  uint64_t h = 14695981039346656037ULL;
  for (unsigned char c : data) { h ^= c; h *= 1099511628211ULL; }
  return h;
}

// splitmix64 finaliser; used to derive "tokens" from (prompt checksum, index).
inline uint64_t mix(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}
inline uint64_t token_value(uint64_t seed, uint32_t index) { return mix(seed ^ mix(index)); }

// Deterministic pseudo-random prompt bytes from a request id.
inline std::string make_prompt(uint64_t id, size_t size) {
  std::string data(size, '\0');
  uint64_t x = mix(id) | 1;
  for (size_t i = 0; i < size; ++i) {
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    data[i] = static_cast<char>(x & 255);
  }
  return data;
}

// Nearest-rank percentile on a copy (p in [0,1]).
inline double percentile(std::vector<double> v, double p) {
  if (v.empty()) return 0;
  std::sort(v.begin(), v.end());
  size_t rank = static_cast<size_t>(p * static_cast<double>(v.size()) + 0.999999);
  rank = std::clamp<size_t>(rank, 1, v.size());
  return v[rank - 1];
}

// Ask the OS to schedule the calling thread promptly. On a shared/busy Mac the
// default QoS lets sleeping threads wake late, which distorts the synthetic step
// time and the client's arrival schedule. No-op elsewhere.
inline void prefer_low_latency_thread() {
#ifdef __APPLE__
  pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#endif
}

}  // namespace lrit
