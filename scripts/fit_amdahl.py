#!/usr/bin/env python3
"""Fit Amdahl's Law serial fraction s to benchmark data and plot speedup curves."""
import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

ROOT = Path(os.path.dirname(os.path.abspath(__file__))).parent

# NOTE: Change the CSV_PATH accordingly
CSV_PATH = ROOT / "plots" / "benchmark_scaling_pruning.csv"
OUT_PATH = ROOT / "plots" / "amdahl_fit.png"


def amdahl(N: np.ndarray, s: float) -> np.ndarray:
    return 1.0 / (s + (1.0 - s) / N)


def main() -> None:
    df = pd.read_csv(CSV_PATH)
    cores = df["cores"].values.astype(float)
    speedup = df["speedup"].values.astype(float)

    popt, pcov = curve_fit(amdahl, cores, speedup, p0=[0.1], bounds=(0.0, 1.0))
    s_fit = float(popt[0])
    s_std = float(np.sqrt(pcov[0, 0]))
    max_speedup = 1.0 / s_fit

    print(f"Fitted serial fraction  s = {s_fit:.4f} ± {s_std:.4f}")
    print(f"Theoretical max speedup   = {max_speedup:.2f}×")
    print(f"Parallel fraction    1-s  = {1-s_fit:.4f}")

    # Plot
    N_range = np.linspace(1, max(cores) * 1.5, 500)

    fig, ax = plt.subplots(figsize=(8, 5))

    # Reference curves
    reference_serials = [0.25, 0.50, 0.75, 0.90]
    ref_styles = ["--", "-.", ":", (0, (3, 1, 1, 1))]
    for s_ref, ls in zip(reference_serials, ref_styles):
        ax.plot(
            N_range,
            amdahl(N_range, s_ref),
            linestyle=ls,
            color="grey",
            linewidth=1.2,
            label=f"Amdahl s={int(s_ref*100)}%",
        )

    # Ideal linear speedup
    ax.plot(N_range, N_range, linestyle="--", color="green", linewidth=1.0, label="Ideal linear")

    # Fitted curve
    ax.plot(
        N_range,
        amdahl(N_range, s_fit),
        color="steelblue",
        linewidth=2.2,
        label=f"Fitted s={s_fit:.3f} (max {max_speedup:.1f}×)",
    )

    # Observed data points
    ax.scatter(cores, speedup, color="red", zorder=5, s=60, label="Observed speedup")

    ax.set_xlabel("Number of cores (N)")
    ax.set_ylabel("Speedup")
    ax.set_title("Amdahl's Law Fit — Borůvka MST Scaling")
    ax.legend(loc="upper left", fontsize=9)
    ax.set_xlim(1, max(cores) * 1.5)
    ax.set_ylim(0.5, max(cores) * 1.5)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(OUT_PATH, dpi=150)
    print(f"Plot saved to {OUT_PATH}")


if __name__ == "__main__":
    main()
