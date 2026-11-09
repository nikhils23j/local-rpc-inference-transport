# Interview notes: local-rpc-inference-transport

## 1. What it is

A small C++20 gRPC server that behaves like the front end of an LLM serving
system. It streams "tokens", batches requests (none / static / continuous), keeps
two priority lanes, and decides at the door whether to admit a request
(unbounded, tail-drop, deadline-aware, or an AIMD concurrency limit). There's no
model: each step is a sleep sized by a made-up cost model. So the project is about
the **queueing and scheduling**, not ML. I measured it with an open-loop load
generator on my laptop.

## 2. How it works, and why I made these choices

**Request path.** The client calls `Generate` (a server-streaming RPC) with a
prompt, its FNV-1a checksum, `max_tokens`, a priority, and a gRPC deadline. The
server then:
1. Checks the checksum (`DATA_LOSS` if it's wrong).
2. Runs the admission policy (`RESOURCE_EXHAUSTED` if rejected).
3. Puts the request in a two-lane queue.
4. Streams tokens back as a worker produces them.

**Engine (`src/engine.cpp`, no gRPC inside).** N "replica" workers share one
waiting queue. Each worker loops:
- At each step boundary, drop sequences that were cancelled or are past their
  deadline.
- Pull new sequences from the queue:
  - continuous mode: up to the max batch, every step
  - static mode: only when the batch is empty, after waiting up to `max_wait` for
    it to fill
- "Compute" for `fixed + per_seq*batch + prefill*new` µs.
- Emit one token per sequence.

Why I made these choices:
- **Engine separate from gRPC.** The scheduling logic can then be unit-tested
  directly (11 tests) without sockets.
- **Fixed-heavy cost model (2000 µs fixed, 125 µs per sequence).** LLM decode is
  roughly memory-bound: you read the weights once per step no matter how many
  sequences are in the batch. That's why batching works. The numbers themselves
  are invented.
- **Sleep plus a short spin.** Plain `sleep_until` on macOS overshot 2 ms sleeps by
  about 500 µs at the median in my test. So I sleep until 700 µs before the target
  and spin-yield the rest. The engine also records the real step time, so I can
  see when the model and reality drift apart.
- **Deadline-aware admission.** When the batch slots are full, the predicted wait
  is `(queue_ahead − free_slots + 1) × avg_tokens × step_time / total_slots`, plus
  this request's own `tokens × step_time`. Step time is scaled by a running average
  of measured/modelled step time. If the prediction is above `slack × remaining
  deadline`, the request is rejected now instead of timing out later.
- **AIMD limit.** This caps in-flight requests. On a fast success the limit grows
  by +1/limit, but only when the limit is actually being used. On a deadline miss,
  or a success that used more than 80% of its budget, the limit is multiplied by
  0.9. It's the Netflix concurrency-limits idea, simplified.
- **Cancellation at step boundaries.** If the client goes away or the deadline
  passes, the sequence leaves the batch at the next step. The old version could
  only skip work that was still queued.
- **Open-loop client.** Arrivals are a pre-computed, seeded Poisson schedule.
  Latency is measured from the *scheduled* time, so a slow server can't hide its
  slowness by slowing the client down (coordinated omission).
- **Token integrity.** Each token is `mix(prompt_checksum, index)`. The client
  re-derives every one, so reordering, loss or corruption would show up.

## 3. Key results (all from `results.json`; median of 3 runs [min–max])

Setup: M2 laptop, 2 workers, max batch 16, 16–48 tokens per request, 1 s deadline.

- **Batching.**
  - At 100/s offered, continuous batching had TTFT p99 of 12 ms [11–22], static
    had 115 ms [112–129], and no batching collapsed to 12/s goodput.
  - At 200/s, continuous TTFT p99 was 28 ms vs 265 ms for static, with the same
    197/s goodput.
- **Congestion collapse.** With no batching and no admission control, goodput fell
  from 25/s (at 50/s offered) to 7/s (at 300/s offered).
- **Admission at 300/s offered (~1,200 requests).**
  - Unbounded: 218/s goodput, 317 timeouts, 6,958 wasted tokens.
  - Tail-drop 64: 239/s, p99 510 ms, 0 timeouts.
  - Deadline-aware (slack 0.8): 267/s, 0 timeouts.
  - AIMD: 228/s, p99 291 ms (the lowest).
- **Priority (30% interactive at 250/s).** Interactive requests served went from
  36/299 to 299/299, and their TTFT p99 was 64 ms. Batch timeouts went from 0 to
  40 [7–141].
- **Workers with the queue policy fixed (400/s, tail-drop 64).** 1 → 130/s,
  2 → 240/s, 4 → 394/s goodput.

What these results **do not** show:
- Anything about real model inference, GPU utilisation, KV-cache memory, or
  vLLM/TGI performance.
- Results that hold beyond this cost model, this workload mix, and one shared
  laptop with client and server on the same machine.
- Tight statistics. Three runs per cell shows the spread, nothing more.
- A tuned deadline slack. 0.8 came from looking at one exploratory run where the
  predictor was ~7% optimistic.

## 4. Two-minute answer (STAR)

**Situation.**
"I got curious about why chat assistants stay responsive even when they're
obviously overloaded. I'd assumed it was mostly about faster GPUs. Reading about
vLLM and Orca, it seemed like a lot of it was actually scheduling. I'd already
written a simple gRPC request-queue benchmark in C++, but it was basically a FIFO
with worker threads. Its headline comparison also changed two things at once,
worker count and queue bounding, so I couldn't tell which one mattered."

**Task.**
"I wanted to turn it into a mini LLM-serving front end, still with fake compute,
and actually measure what each scheduling decision buys you. That meant
time-to-first-token, tail latency, and especially goodput, meaning requests that
finish before their deadline. And I wanted experiments that change only one thing
at a time."

**Action.**
"It was a personal project, and I built it with an AI pair-programmer that wrote a
lot of the code. My part was the design, the experiment plan, and checking every
result by running it.
- I added a server-streaming RPC and an engine with three batching modes: none,
  static, and continuous, where new requests join the running batch every step.
- I added four admission policies: unbounded, tail-drop, a deadline-aware one that
  predicts your finish time from queue depth and the measured step time, and an
  AIMD concurrency limit.
- I added two priority lanes, and cancellation that removes a request from the
  batch at the next step.
- I kept the engine separate from gRPC so I could unit-test it, and added an
  end-to-end integration test.
- I wrote an open-loop load generator that measures latency from the scheduled
  send time, so the client can't hide slowness.
- One messy thing: the laptop was shared with other heavy jobs, and my first full
  runs had steps taking 1.8× longer than modelled. So I threw those results out,
  added a load gate, and recorded the measured step inflation for every run."

**Result.**
"The clearest result was congestion collapse. Without admission control, a
non-batching server's goodput didn't level off when overloaded, it fell from about
25 to 7 requests per second, because it spent its time on requests that then timed
out. Continuous batching kept TTFT p99 at 28 ms at 200 requests per second, versus
265 ms for static batching at the same throughput. Under 1.5× overload, a
deadline-aware admission check got 267 goodput per second with zero timeouts,
versus 218 with an unbounded queue. And priority lanes took interactive requests
from 36 out of 299 served to all 299. The biggest lesson for me was that no policy
wins everywhere. AIMD had by far the best tail latency but rejected the most. It's
all synthetic on one laptop, so I treat it as intuition-building, not a
benchmark."

## 5. Likely follow-up questions

1. **Why is there no real model? Doesn't that make it meaningless?**
   It makes it a scheduling experiment, not an inference benchmark. The cost model
   keeps the one property that matters for batching: a big fixed cost per step and
   a small cost per sequence. Absolute numbers don't carry over. What I'd expect
   to carry over is the qualitative stuff, like collapse without admission control
   and static vs continuous TTFT.

2. **How does your deadline predictor work, and how accurate was it?**
   It estimates the wait from how many requests are ahead minus free batch slots,
   with slots freeing at about `total_slots / (avg_tokens × step_time)`. Then it
   adds this request's own decode time. On completed requests, actual time was
   about 1.07× the prediction on average (1.07–1.10 in the overload runs), so it
   was slightly optimistic. With slack 1.0 that meant 114 borderline timeouts; with
   0.8 there were none. It also gets worse with priorities: batch requests were
   1.32× their prediction, because interactive traffic jumps ahead of them later.

3. **Why did a bigger queue do worse?**
   A queue only helps if the wait still fits inside the deadline. With 256 waiting
   slots, requests at the back waited around 1 s and then died. That's wasted work,
   and the p99 sits right at the deadline. With 64 slots, the wait stays under the
   deadline, so everything admitted finishes.

4. **What is coordinated omission, and how did you avoid it?**
   With a closed-loop client, a slow server slows down the arrivals, so the
   latencies look better than real users would see. My client fixes the arrival
   schedule up front and measures from the scheduled time. Lag shows up as latency,
   and I report the client's own wake-up lag too (about 5 ms at p99).

5. **Why sync gRPC with a thread per stream?**
   It was the simplest thing that's obviously correct, and at a few hundred
   in-flight streams it was fine. For thousands I'd switch to the callback/reactor
   API so streams don't each hold a thread. I didn't measure that.

6. **How do you know the numbers aren't just noise?**
   Three repetitions with different seeds, and I report min–max, not just the
   median. Most gaps I talk about are far outside the spread, like 28 ms vs 265 ms.
   Some are close: unbounded vs tail-drop 256 overlap a bit. I also log load
   average and step inflation per run, and I threw out runs from when the machine
   was busy.

7. **What did the AI help with, and what did you do?**
   Honestly, a large share of the code was written by the AI pair-programmer from
   descriptions, including the CMake/proto boilerplate and the test harness. My job
   was deciding what to build and what to measure, reading and questioning the
   code, running everything, and not trusting numbers I couldn't explain. Examples:
   the confound in the old version, the macOS sleep overshoot, and the runs I threw
   out because the machine was busy. If I can't explain a line, I don't claim it.

8. **What would you do next?**
   Plug in a real small model, for example llama.cpp on CPU, to calibrate the cost
   model. Add preemption so interactive requests can bump running batch requests.
   Make the predictor priority-aware. Run client and server on separate machines.

## 6. Things I should be able to explain cold

- [ ] The worker loop step by step: drop expired, admit new (continuous vs static), compute, emit, finish.
- [ ] Why batching helps when the fixed cost dominates. Per-token cost at batch 1 vs 16:
      2125 µs vs (2000 + 16·125)/16 = 250 µs.
- [ ] Static vs continuous batching, and why static hurts TTFT but not throughput.
- [ ] Goodput vs throughput, and why unbounded queues collapse (wasted tokens).
- [ ] The deadline predictor formula, its 1.07× optimism, and what slack does.
- [ ] AIMD: when it grows (+1/limit, only if the limit is in use) and when it shrinks (×0.9).
- [ ] Why tail-drop 64 beat 256: queue wait vs deadline (Little's law intuition).
- [ ] Priority lanes and the starvation tradeoff (40 batch timeouts on median).
- [ ] Open-loop load and coordinated omission; why latency is measured from the scheduled time.
- [ ] gRPC deadline propagation: client `set_deadline` → server `ctx->deadline()` → engine; cancellation path.
- [ ] Token integrity check: `mix(prompt_checksum, index)`; FNV-1a is not security.
- [ ] Why the old 4-worker vs 1-worker number was confounded, and how E4 fixes it.
- [ ] Measurement hygiene: the sleep-overshoot fix, the load gate, step inflation per run, 3 reps and min–max.
- [ ] What it is NOT: no model, no GPU, no KV cache, one shared laptop, localhost.
