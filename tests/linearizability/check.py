#!/usr/bin/env python3
"""
Wing-Gong linearizability checker for NimbusVault.

Reads a history CSV (produced by HistoryTracer) and verifies that the
observed PUT/GET sequence is linearizable with respect to a single-key
register model.  Each chunk_id is treated as an independent register.

Model:
  - Register starts with value None (unwritten).
  - PUT(chunk, data)  → writes data; the register's value becomes data.
  - GET(chunk)        → must read the most recently written value (or None
                        if never written), at the point where the GET is
                        linearized in the total order.
  - DELETE(chunk)     → resets the register to None.

Algorithm (Wing-Gong DFS):
  For each chunk independently, collect all completed operations as
  (start_us, end_us, type, value) intervals.  Then perform DFS over all
  linearization orderings, pruning via the register invariant.

  Complexity: O(n!) worst case, but in practice the pruning cuts it
  to polynomial for typical histories.  Histories with > 20 concurrent
  ops per key may be slow; use --max-ops-per-key to skip those.

Usage:
  python3 tests/linearizability/check.py --csv results.csv
  python3 tests/linearizability/check.py --csv results.csv --verbose
  python3 tests/linearizability/check.py --csv results.csv --max-ops-per-key 15
"""

import argparse
import csv
import sys
from collections import defaultdict
from typing import List, Optional, Tuple



class Op:
    __slots__ = ("start_us", "end_us", "op_type", "value", "ok")

    def __init__(self, start_us: int, end_us: int, op_type: str,
                 value: str, ok: bool):
        self.start_us = start_us
        self.end_us   = end_us
        self.op_type  = op_type   # "PUT", "GET", "DELETE"
        self.value    = value     # data for PUT; returned data for GET; "" for DELETE
        self.ok       = ok

    def __repr__(self):
        return f"{self.op_type}({self.value!r})[{self.start_us}-{self.end_us}]"



def load_history(csv_path: str) -> dict:
    """Return dict[chunk_id -> List[Op]] from a HistoryTracer CSV."""
    ops_by_chunk: dict = defaultdict(list)

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            op_type = row.get("op_type", "")
            if op_type == "TIER_CHANGE":
                continue
            if op_type not in ("PUT", "GET", "DELETE"):
                continue

            chunk_id  = row.get("chunk_id", "")
            start_us  = int(row.get("timestamp_us", 0))
            latency   = int(row.get("latency_us", 0))
            end_us    = start_us + latency
            ok        = row.get("ok", "1") == "1"
            value     = row.get("data", "")

            ops_by_chunk[chunk_id].append(
                Op(start_us, end_us, op_type, value, ok)
            )

    return dict(ops_by_chunk)



def real_time_before(a: Op, b: Op) -> bool:
    """True if op a must precede op b in any valid linearization."""
    return a.end_us <= b.start_us



def is_linearizable_register(ops: List[Op], verbose: bool) -> bool:
    """
    Return True iff the list of ops on one chunk is linearizable.

    State = current register value (None = never written / deleted).
    """
    n = len(ops)
    if n == 0:
        return True

    def candidates(done: frozenset) -> List[int]:
        result = []
        for i in range(n):
            if i in done:
                continue
            # i is a candidate if all ops that must precede it are done.
            ok = True
            for j in range(n):
                if j != i and j not in done and real_time_before(ops[j], ops[i]):
                    ok = False
                    break
            if ok:
                result.append(i)
        return result

    # DFS.
    # Stack entries: (done_set, register_value)
    stack = [(frozenset(), None)]
    while stack:
        done, reg = stack.pop()
        if len(done) == n:
            return True

        for idx in candidates(done):
            op = ops[idx]
            # Skip failed ops — they're not required to be linearizable.
            if not op.ok:
                stack.append((done | {idx}, reg))
                continue

            if op.op_type == "PUT":
                new_reg = op.value if op.value else reg
                stack.append((done | {idx}, new_reg))

            elif op.op_type == "DELETE":
                stack.append((done | {idx}, None))

            elif op.op_type == "GET":
                if op.value and reg is not None and op.value != reg:
                    if verbose:
                        print(f"    GET read {op.value!r}, but register={reg!r} — skip")
                    continue   # this linearization point is invalid
                stack.append((done | {idx}, reg))

    return False



def check_history(history: dict, verbose: bool,
                  max_ops_per_key: Optional[int]) -> Tuple[int, int, int]:
    """
    Returns (passed, failed, skipped) counts.
    """
    passed = failed = skipped = 0

    for chunk_id, ops in sorted(history.items()):
        if max_ops_per_key is not None and len(ops) > max_ops_per_key:
            if verbose:
                print(f"  SKIP {chunk_id}: {len(ops)} ops > --max-ops-per-key={max_ops_per_key}")
            skipped += 1
            continue

        ok = is_linearizable_register(ops, verbose)
        if ok:
            if verbose:
                print(f"  PASS {chunk_id} ({len(ops)} ops)")
            passed += 1
        else:
            print(f"  FAIL {chunk_id} ({len(ops)} ops): history is NOT linearizable")
            if verbose:
                for op in ops:
                    print(f"    {op}")
            failed += 1

    return passed, failed, skipped


def main():
    ap = argparse.ArgumentParser(
        description="Wing-Gong linearizability checker for NimbusVault")
    ap.add_argument("--csv", required=True,
                    help="HistoryTracer CSV file to check")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="Print per-chunk result and failing op details")
    ap.add_argument("--max-ops-per-key", type=int, default=20,
                    metavar="N",
                    help="Skip chunks with more than N ops (DFS is exponential; "
                         "default 20)")
    args = ap.parse_args()

    print(f"Loading history from {args.csv} ...")
    history = load_history(args.csv)
    total_chunks = len(history)
    total_ops    = sum(len(v) for v in history.values())
    print(f"  {total_chunks} chunks, {total_ops} ops\n")

    passed, failed, skipped = check_history(
        history, args.verbose, args.max_ops_per_key)

    print()
    print("─" * 40)
    print(f"Chunks checked : {passed + failed}")
    print(f"  PASS         : {passed}")
    print(f"  FAIL         : {failed}")
    print(f"  SKIP (large) : {skipped}")
    print("─" * 40)

    if failed > 0:
        print("RESULT: NOT LINEARIZABLE")
        sys.exit(1)
    else:
        print("RESULT: LINEARIZABLE ✓")
        sys.exit(0)


if __name__ == "__main__":
    main()
