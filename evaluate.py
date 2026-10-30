#!/usr/bin/env python3
"""Reproducible localhost comparisons; numbers describe synthetic request handling only."""
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

BIN = Path(sys.argv[1] if len(sys.argv) > 1 else 'build/transport').resolve()
CASES = [
    ('single_worker', 1, 10000, 10000, 32, 1024, 500),
    ('bounded_four_workers', 4, 64, 10000, 32, 1024, 500),
    ('overload', 2, 8, 1000, 128, 65536, 3000),
]


def run_case(case, port):
    name, workers, capacity, count, concurrency, payload_bytes, work_us = case
    server = subprocess.Popen(
        [str(BIN), 'server', '--port', str(port), '--workers', str(workers),
         '--capacity', str(capacity), '--work-us', str(work_us)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        ready = server.stderr.readline().strip()
        if not ready.startswith('ready '):
            raise RuntimeError(f'server failed: {ready}')
        peak_rss_kib = [0]
        stop = threading.Event()

        def sample_memory():
            while not stop.is_set():
                result = subprocess.run(['ps', '-o', 'rss=', '-p', str(server.pid)],
                                        capture_output=True, text=True)
                if result.returncode == 0 and result.stdout.strip():
                    peak_rss_kib[0] = max(peak_rss_kib[0], int(result.stdout.strip()))
                stop.wait(.02)

        sampler = threading.Thread(target=sample_memory, daemon=True)
        sampler.start()
        result = subprocess.run(
            [str(BIN), 'client', '--port', str(port), '--count', str(count),
             '--concurrency', str(concurrency), '--payload-bytes', str(payload_bytes),
             '--deadline-ms', '20000'], capture_output=True, text=True, timeout=90,
        )
        stop.set()
        sampler.join()
        if result.returncode:
            raise RuntimeError(result.stderr + result.stdout)
        data = json.loads(result.stdout)
        assert data['ok'] + data['rejected'] + data['deadline'] + data['data_loss'] + data['errors'] == count, data
        assert data['errors'] == 0 and data['deadline'] == 0 and data['data_loss'] == 0, data
        data.update(name=name, workers=workers, capacity=capacity, work_us=work_us,
                    peak_server_rss_kib=peak_rss_kib[0])
        return data
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


def main():
    results = [run_case(case, 50110 + i) for i, case in enumerate(CASES)]
    output = {'environment': 'macOS localhost; synthetic checksum service, not real inference',
              'cases': results}
    Path('results.json').write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
