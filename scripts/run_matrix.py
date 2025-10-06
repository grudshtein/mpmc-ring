import argparse
import subprocess
import itertools
import yaml

def expand_suite(suite):
    """Expand each suite into all parameter combinations."""
    repeats = suite.get("repeats", 1)
    note = suite.get("notes", "")
    params = {
        k: v if isinstance(v, list) else [v]
        for k, v in suite.items()
        if k not in ("repeats", "notes")
    }
    combos = [dict(zip(params, values)) for values in itertools.product(*params.values())]
    return repeats, note, combos

def run_suites(bench_path, suites, csv_path):
    total_runs = sum(expand_suite(s)[0] * len(expand_suite(s)[2]) for s in suites)
    completed = 0

    for suite_idx, suite in enumerate(suites, 1):
        repeats, note, combos = expand_suite(suite)

        for combo_idx, params in enumerate(combos, 1):
            cmd = [bench_path]
            for k, v in params.items():
                cmd += [f"--{k}", str(v)]
            if note:
                cmd += ["--notes", note]
            cmd += ["--csv", csv_path]

            for r in range(repeats):
                completed += 1
                print(f"\n[{completed}/{total_runs}] Suite {suite_idx}/{len(suites)}, "
                      f"Combo {combo_idx}/{len(combos)}, Repeat {r+1}/{repeats}")
                subprocess.run(cmd)

    print(f"\nCompleted {completed} run(s).")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix")
    args = parser.parse_args()
    matrix = args.matrix
    assert "_" in matrix, "matrix name invalid"

    with open(matrix, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    csv_path = "results/raw/" + matrix.split('/')[-1].split('_')[0] + "_results.csv"
    run_suites(cfg["bench"], cfg["suites"], csv_path)

if __name__ == "__main__":
    main()
