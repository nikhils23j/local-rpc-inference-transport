#!/usr/bin/env python3
"""Experiment matrix for the synthetic LLM-serving front end.

Every run starts a fresh server, drives it with an open-loop Poisson client,
records the client's JSON (which includes server counters), then kills the
server. Each experiment changes ONE factor; everything else stays at BASE.

usage: python3 evaluate.py [build/transport] [--quick] [--reps N] [--out results.json]
"""
import argparse
import json
import os
import platform
import statistics
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent

BASE_SERVER = {
    'workers': 2, 'mode': 'continuous', 'max-batch': 16, 'max-wait-us': 5000,
    'step-fixed-us': 2000, 'step-per-seq-us': 125, 'prefill-us': 500,
    'policy': 'unbounded', 'capacity': 64, 'priority': 1,
}
BASE_CLIENT = {
    'rate': 100, 'duration-s': 4, 'interactive-frac': 0,
    'b-tokens-min': 16, 'b-tokens-max': 48, 'b-deadline-ms': 1000,
    'i-tokens-min': 8, 'i-tokens-max': 24, 'i-deadline-ms': 300,
    'prompt-bytes': 1024, 'threads': 512, 'channels': 4,
}
MIXED = {'interactive-frac': 0.3, 'b-tokens-min': 32, 'b-tokens-max': 96, 'b-deadline-ms': 3000}


def experiments(quick):
    rates = [50, 200, 300] if quick else [25, 50, 100, 200, 300]
    ex = []
    # E1: batching mode. Same workers (2), same admission (unbounded + 1 s deadline).
    for mode in ['none', 'static', 'continuous']:
        for rate in rates:
            ex.append(('E1_batching', f'{mode}@{rate}', {'mode': mode}, {'rate': rate}))
    # E2: admission policy. Same workers (2), continuous batching, same offered load.
    policies = [('unbounded', {}), ('taildrop64', {'policy': 'taildrop', 'capacity': 64}),
                ('taildrop256', {'policy': 'taildrop', 'capacity': 256}),
                ('deadline', {'policy': 'deadline', 'capacity': 1024}),
                ('deadline_slack0.8', {'policy': 'deadline', 'capacity': 1024, 'deadline-slack': 0.8}),
                ('adaptive', {'policy': 'adaptive', 'capacity': 1024})]
    for rate in ([300] if quick else [150, 300]):
        for name, srv in policies:
            ex.append(('E2_admission', f'{name}@{rate}', srv, {'rate': rate}))
    # E3: priority classes. Mixed traffic at overload, deadline-aware admission; priority off vs on.
    for prio in [0, 1]:
        ex.append(('E3_priority', f'priority={prio}', {'priority': prio, 'policy': 'deadline', 'capacity': 1024},
                   dict(MIXED, rate=250)))
    # E4: worker count with the SAME queue policy (fixes the old worker-vs-queue confound).
    for workers in [1, 2, 4]:
        ex.append(('E4_workers', f'workers={workers}', {'workers': workers, 'policy': 'taildrop', 'capacity': 64},
                   {'rate': 400}))
    return ex


def flags(d):
    out = []
    for k, v in d.items():
        out += [f'--{k}', str(v)]
    return out


def wait_for_quiet_machine(gate, max_wait_s):
    """This laptop is shared with other jobs. Wait (bounded) until the 1-minute load
    average is below `gate` so runs are less distorted; the load is recorded either way."""
    t0 = time.time()
    while os.getloadavg()[0] >= gate and time.time() - t0 < max_wait_s:
        time.sleep(5)
    return round(time.time() - t0, 1)


def run_once(binary, port, srv, cli, seed):
    server_cmd = [str(binary), 'server', '--port', str(port)] + flags(srv)
    client_cmd = [str(binary), 'client', '--port', str(port), '--seed', str(seed)] + flags(cli)
    server = subprocess.Popen(server_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        ready = server.stderr.readline().strip()
        if not ready.startswith('ready '):
            raise RuntimeError(f'server failed to start: {ready}')
        load_before = os.getloadavg()[0]
        res = subprocess.run(client_cmd, capture_output=True, text=True, timeout=120)
        if res.returncode != 0:
            raise RuntimeError(f'client failed ({res.returncode}): {res.stderr}{res.stdout}')
        data = json.loads(res.stdout)
        a = data['all']
        total = a['ok'] + a['rejected'] + a['deadline'] + a['data_loss'] + a['bad_stream'] + a['errors']
        assert total == a['sent'], data
        assert a['bad_stream'] == 0 and a['errors'] == 0 and a['data_loss'] == 0, data
        data['loadavg_1m_before'] = load_before
        data['loadavg_1m_after'] = os.getloadavg()[0]
        # How much slower real steps were than the cost model (ignores prefill, so slightly > 1 is normal).
        modelled = srv['step-fixed-us'] + srv['step-per-seq-us'] * data['server']['mean_batch']
        data['step_inflation'] = data['server']['mean_step_us'] / modelled if data['server']['steps'] else 0
        return data, server_cmd, client_cmd
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


def pick(run, path):
    x = run
    for p in path.split('.'):
        x = x[p]
    return x


METRICS = {
    'goodput_rps': 'all.goodput_rps', 'p50_ms': 'all.latency_ms.p50', 'p99_ms': 'all.latency_ms.p99',
    'ttft_p50_ms': 'all.ttft_ms.p50', 'ttft_p99_ms': 'all.ttft_ms.p99',
    'sent': 'all.sent', 'ok': 'all.ok', 'rejected': 'all.rejected', 'timed_out': 'all.deadline',
    'timed_out_after_first_token': 'all.deadline_after_first_token',
    'tokens_wasted': 'server.tokens_wasted', 'tokens_generated': 'server.tokens_generated',
    'mean_batch': 'server.mean_batch', 'mean_step_us': 'server.mean_step_us',
    'adaptive_limit_end': 'server.adaptive_limit', 'client_lag_p99_ms': 'client_lag_ms.p99',
    'i_goodput_rps': 'interactive.goodput_rps', 'i_ok': 'interactive.ok', 'i_sent': 'interactive.sent',
    'i_ttft_p99_ms': 'interactive.ttft_ms.p99', 'i_p99_ms': 'interactive.latency_ms.p99',
    'b_goodput_rps': 'batch.goodput_rps', 'b_ok': 'batch.ok', 'b_sent': 'batch.sent',
    'b_timed_out': 'batch.deadline', 'i_rejected': 'interactive.rejected',
    'predictor_mean_ratio': 'server.predictor_mean_ratio', 'step_inflation': 'step_inflation',
    'loadavg_1m_before': 'loadavg_1m_before',
    'predictor_underestimate_frac': 'server.predictor_underestimate_frac',
}


def aggregate(runs):
    out = {}
    for name, path in METRICS.items():
        vals = [pick(r, path) for r in runs]
        out[name] = {'median': statistics.median(vals), 'min': min(vals), 'max': max(vals)}
    return out


def environment():
    def sh(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
        except Exception as e:  # noqa: BLE001
            return f'unavailable ({e})'
    return {
        'cpu': sh(['sysctl', '-n', 'machdep.cpu.brand_string']),
        'logical_cpus': os.cpu_count(),
        'mem_bytes': sh(['sysctl', '-n', 'hw.memsize']),
        'os': f"{platform.system()} {platform.mac_ver()[0] or platform.release()}",
        'compiler': sh(['c++', '--version']).splitlines()[0] if sh(['c++', '--version']) else '',
        'cmake': sh(['cmake', '--version']).splitlines()[0],
        'grpc': ', '.join(sorted(p.name for p in Path('/opt/homebrew/Cellar/grpc').glob('*'))) or 'unknown',
        'protobuf': ', '.join(sorted(p.name for p in Path('/opt/homebrew/Cellar/protobuf').glob('*'))) or 'unknown',
        'python': platform.python_version(),
        'git_commit': sh(['git', '-C', str(ROOT), 'rev-parse', '--short', 'HEAD']),
        'git_dirty': bool(sh(['git', '-C', str(ROOT), 'status', '--porcelain', '.'])),
        'loadavg_at_start': os.getloadavg(),
        'note': 'localhost only; synthetic compute (sleep per scheduler step); client and server share the machine',
    }


def fmt(m, key, nd=0):
    v = m[key]
    f = f'{{:.{nd}f}}'
    return f"{f.format(v['median'])} [{f.format(v['min'])}-{f.format(v['max'])}]"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('binary', nargs='?', default='build/transport')
    ap.add_argument('--quick', action='store_true')
    ap.add_argument('--reps', type=int, default=3)
    ap.add_argument('--out', default=str(ROOT / 'results.json'))
    ap.add_argument('--load-gate', type=float, default=12.0, help='wait for 1-min loadavg below this before each run')
    ap.add_argument('--gate-max-wait-s', type=float, default=60.0)
    ap.add_argument('--gate-total-budget-s', type=float, default=240.0, help='stop gating after this much total waiting')
    args = ap.parse_args()
    binary = Path(args.binary).resolve()
    reps = 1 if args.quick else args.reps

    env = environment()
    t_start = time.time()
    cases = []
    port = 50300
    gate_waited_total = 0.0
    for exp, label, srv_over, cli_over in experiments(args.quick):
        srv = dict(BASE_SERVER, **srv_over)
        cli = dict(BASE_CLIENT, **cli_over)
        runs = []
        for rep in range(reps):
            port += 1
            budget_left = max(0.0, args.gate_total_budget_s - gate_waited_total)
            waited = wait_for_quiet_machine(args.load_gate, min(args.gate_max_wait_s, budget_left))
            gate_waited_total += waited
            data, server_cmd, client_cmd = run_once(binary, port, srv, cli, seed=rep + 1)
            data['load_gate_wait_s'] = waited
            runs.append(data)
        agg = aggregate(runs)
        rel = lambda cmd: ' '.join(['./build/transport'] + cmd[1:])  # noqa: E731  (no home-dir paths in results)
        cases.append({'experiment': exp, 'label': label, 'server': srv, 'client': cli,
                      'server_cmd': rel(server_cmd), 'client_cmd': rel(client_cmd), 'aggregate': agg, 'runs': runs})
        print(f'{exp:13s} {label:20s} goodput {fmt(agg, "goodput_rps")}/s  p99 {fmt(agg, "p99_ms")} ms  '
              f'ttft99 {fmt(agg, "ttft_p99_ms")} ms  rej {fmt(agg, "rejected")}  timeout {fmt(agg, "timed_out")}  '
              f'wasted_tok {fmt(agg, "tokens_wasted")}  batch {fmt(agg, "mean_batch", 1)}  '
              f'step_us {fmt(agg, "mean_step_us")} (x{fmt(agg, "step_inflation", 2)} of model)  '
              f'load {fmt(agg, "loadavg_1m_before")}', flush=True)
        if exp == 'E3_priority':
            print(f'{"":34s} interactive ok {fmt(agg, "i_ok")}/{fmt(agg, "i_sent")}  ttft99 {fmt(agg, "i_ttft_p99_ms")} ms  '
                  f'| batch ok {fmt(agg, "b_ok")}/{fmt(agg, "b_sent")} timeout {fmt(agg, "b_timed_out")}', flush=True)
        if srv.get('policy') == 'deadline':
            print(f'{"":34s} predictor actual/predicted {fmt(agg, "predictor_mean_ratio", 2)}  '
                  f'underestimated {fmt(agg, "predictor_underestimate_frac", 2)}', flush=True)

    env['loadavg_at_end'] = os.getloadavg()
    out = {
        'description': 'Synthetic LLM-serving front end over gRPC on localhost. No model, no GPU: '
                       'each scheduler step sleeps fixed + per_seq*batch + prefill*new_seqs microseconds.',
        'environment': env,
        'repetitions': reps,
        'aggregation': 'median [min-max] across repetitions; each repetition uses a different arrival seed',
        'definitions': {
            'goodput_rps': 'requests that received all tokens before their deadline, divided by the arrival window',
            'latency/ttft': 'measured by the client from the *scheduled* arrival time (open loop, corrects coordinated omission)',
            'rejected': 'RESOURCE_EXHAUSTED at admission (queue full, would-miss-deadline, or over adaptive limit)',
            'timed_out': 'DEADLINE_EXCEEDED/CANCELLED after being admitted',
            'tokens_wasted': 'tokens the server generated for requests that then did not complete',
        },
        'base_server': BASE_SERVER, 'base_client': BASE_CLIENT,
        'reproduce': f'python3 evaluate.py build/transport --reps {reps}' + (' --quick' if args.quick else ''),
        'load_gate': {'loadavg_1m_below': args.load_gate, 'max_wait_s_per_run': args.gate_max_wait_s,
                      'total_budget_s': args.gate_total_budget_s, 'total_waited_s': round(gate_waited_total, 1)},
        'eval_wall_seconds': round(time.time() - t_start, 1),
        'cases': cases,
    }
    Path(args.out).write_text(json.dumps(out, indent=1) + '\n')
    print(f'wrote {args.out} in {out["eval_wall_seconds"]} s')


if __name__ == '__main__':
    main()
