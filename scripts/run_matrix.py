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

def run_suites(bench_path, suites):
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

            for r in range(repeats):
                completed += 1
                print(f"\n[{completed}/{total_runs}] Suite {suite_idx}/{len(suites)}, "
                      f"Combo {combo_idx}/{len(combos)}, Repeat {r+1}/{repeats}")
                subprocess.run(cmd)

    print(f"\nCompleted {completed} run(s).")

def main():
    with open("scripts/matrix.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    run_suites(cfg["bench"], cfg["suites"])

if __name__ == "__main__":
    main()
