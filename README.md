# Local RPC Inference Transport

An AI-assisted C++20 portfolio project for studying the **host-side communication path** of an inference service. It uses real gRPC and Protocol Buffers RPCs on localhost. The server validates a deterministic payload checksum, queues work in a configurable bounded FIFO, processes it with worker threads, returns queue and service times, and rejects excess queued requests with `RESOURCE_EXHAUSTED`. The client supports concurrency, per-RPC deadlines, payload-size selection, and a deliberate checksum-corruption check.

The processing stage computes a checksum and optionally sleeps for a configured duration. It **does not run an LLM, use an accelerator, move data over PCIe, or measure production inference**. The queue limit applies to waiting jobs; gRPC's own connection buffers still consume memory. A client cancellation marks queued work so workers can skip it when dequeued, but an active job is not preempted.

## Build and run

Tested on an Apple M2 Mac with AppleClang 21, CMake 4.4.5, protobuf 36.2, and gRPC 1.85.0. On macOS, install build dependencies with `brew install cmake protobuf grpc`. On other systems, use CMake 3.20+, a C++20 compiler, protobuf and gRPC packages with CMake config files and the `grpc_cpp_plugin` executable.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

In one terminal:

```sh
./build/transport server --port 50051 --workers 4 --capacity 64 --work-us 500
```

In another:

```sh
./build/transport client --port 50051 --count 10000 --concurrency 32 --payload-bytes 1024 --deadline-ms 20000
```

The `--work-us` delay makes queue behavior reproducible enough to inspect; it is not a model-execution measurement. The service binds only to `127.0.0.1` and uses insecure local credentials. Do not expose it on a network without authentication and TLS.

## Reproduce evaluation

```sh
python3 evaluate.py build/transport
```

The script starts fresh servers for a 1-worker/unbounded-queue baseline, a 4-worker/64-waiting-job configuration, and an overload case. It writes `results.json`. Inputs are generated in the client from request IDs; no private data or employer systems are used. Each run reports successful, rejected, deadline and checksum-failure counts; accepted-request p50/p95 latency; accepted throughput; and sampled peak server RSS. RSS sampling is approximate and platform dependent.

| Case | Requests | Payload | Workers / queue | Accepted | Rejected | p95 latency | Accepted throughput | Peak server RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Single-worker baseline | 10,000 | 1 KiB | 1 / 10,000 | 10,000 | 0 | 21.055 ms | 1,551/s | 19,856 KiB |
| Bounded four-worker | 10,000 | 1 KiB | 4 / 64 | 10,000 | 0 | 5.179 ms | 6,321/s | 20,096 KiB |
| Overload | 1,000 | 64 KiB | 2 / 8 | 40 | 960 | 48.865 ms for accepted calls | 371/s accepted | 50,608 KiB |

These are **one local run** on the above machine, not a generalized performance guarantee. The approximately 4.1× throughput change compares different worker counts; it must not be attributed to queue bounding alone. In the overload case, rejecting work is expected behavior; it trades acceptance for a bounded application queue. See `results.json` for unrounded counts and timings.

## Design and checks

- `proto/inference.proto` defines one unary RPC with request ID, payload and expected checksum.
- `src/main.cpp` has the server, bounded queue, worker pool and load client. The FNV-1a checksum is for data-integrity testing, **not cryptographic security**.
- `tests/smoke.sh` verifies 100 successful RPCs and that three deliberately wrong checksums return `DATA_LOSS`.
- `evaluate.py` checks count accounting and absence of unexpected errors under baseline, four-worker and overload configurations.

The project was implemented with Codex assistance for Nikhil Jain. The code and measurements are published so a reviewer can reproduce and discuss them; this is not a claim of independent authorship or production deployment.
