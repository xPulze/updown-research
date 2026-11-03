#!/usr/bin/env python3
"""Fit the market's own z-curve: p_market = Phi(zm(bps) * sqrt(120/tau)),
zm piecewise-linear through knots at bps [0,2,4,6,8,10], zm(0)=0 fixed,
zm clamped flat above 10 (no favored data beyond 9.2bps).

Fit on favored-side rows (n=81). Validate on: losing-side rows (p_fav ~=
1 - p_obs, bid-biased so expect obs slightly BELOW model), near-even rows,
and report old-model comparison. Coordinate-descent on knot values.
"""
import csv, math, os

HERE = os.path.dirname(os.path.abspath(__file__))
CSVS = [os.path.join(os.path.dirname(HERE), "reports", f)
        for f in ("binance_screenshot_points_20260704.csv",
                  "binance_screenshot_points_20260705.csv")]

Z_KNOTS_120S = [(2.0, 0.4987), (4.0, 0.9154), (6.0, 1.1601), (8.0, 1.1901),
                (10.0, 1.2591), (20.0, 1.6646), (30.0, 1.8522)]

def z_old(abs_bps):
    k = Z_KNOTS_120S
    if abs_bps <= k[0][0]: return k[0][1]
    if abs_bps >= k[-1][0]: return k[-1][1]
    for (b0, z0), (b1, z1) in zip(k, k[1:]):
        if b0 <= abs_bps <= b1:
            return z0 + (abs_bps - b0) / (b1 - b0) * (z1 - z0)
    return k[-1][1]

def phi(z): return 0.5 * (1 + math.erf(z / math.sqrt(2)))

rows = []
for path in CSVS:
    if not os.path.exists(path):
        continue
    for r in csv.DictReader(open(path)):
        try: strike = float(r["strike"])
        except (ValueError, KeyError): continue
        note = r["notes"].lower()
        if "anomalous" in note or "duplicate" in note: continue
        delta = float(r["delta_usd"].replace("+", ""))
        tau = float(r["tau_s"])
        if tau <= 0: continue
        tw = float(r["to_win_for_10"])
        rows.append(dict(delta=delta, tau=tau, bps=abs(delta)/strike*1e4,
                         p_obs_up=10.0/tw))

fav  = [r for r in rows if r["delta"] > 0.5]
lose = [r for r in rows if r["delta"] < -0.5]
even = [r for r in rows if abs(r["delta"]) <= 0.5]

BPS_KNOTS = [0.0, 2.0, 4.0, 6.0, 8.0, 10.0]

def zm(bps, vals):
    ks = BPS_KNOTS
    if bps >= ks[-1]: return vals[-1]
    for i in range(len(ks) - 1):
        if ks[i] <= bps <= ks[i+1]:
            f = (bps - ks[i]) / (ks[i+1] - ks[i])
            return vals[i] + f * (vals[i+1] - vals[i])
    return vals[-1]

def model_p(bps, tau, vals):
    return min(phi(zm(bps, vals) * math.sqrt(120.0 / tau)), 0.9999)

def sse(vals, data):
    return sum((model_p(r["bps"], r["tau"], vals) - r["p_obs_up"])**2 for r in data)

# init from old curve * 0.68; vals[0]=0 fixed
vals = [0.0, 0.68*0.4987, 0.68*0.9154, 0.68*1.1601, 0.68*1.1901, 0.68*1.2591]
step = 0.4
best = sse(vals, fav)
while step > 0.002:
    improved = False
    for i in range(1, len(vals)):
        for d in (+step, -step):
            trial = vals[:]
            trial[i] = max(0.0, vals[i] + d)
            # enforce monotone non-decreasing (market confidence rises with bps)
            if any(trial[j] > trial[j+1] for j in range(len(trial)-1)):
                continue
            s = sse(trial, fav)
            if s < best - 1e-12:
                best, vals, improved = s, trial, True
    if not improved:
        step /= 2

print("fitted zm knots (bps -> z):")
for b, v in zip(BPS_KNOTS, vals):
    print(f"  ({b:4.1f}, {v:.4f})   old 0.68*z = {0.68*z_old(b) if b>0 else 0:.4f}")

def report(data, label, use_new=True):
    errs = []
    for r in data:
        pm = model_p(r["bps"], r["tau"], vals) if use_new else \
             min(phi(0.68*z_old(r["bps"])*math.sqrt(120/r["tau"])), 0.9999)
        errs.append(pm - r["p_obs_up"])
    n = len(errs)
    mae = sum(abs(e) for e in errs)/n
    bias = sum(errs)/n
    print(f"  {label:34s} n={n:3d} mae={mae*100:5.1f}pp bias={bias*100:+5.1f}pp")

print("\n=== favored rows (fit set) ===")
report(fav, "old model (K=0.68 curve)", use_new=False)
report(fav, "new fitted zm curve")

print("\n=== per-tau buckets, new model ===")
for lo, hi, lbl in [(180,10**5,">=180s"),(120,180,"120-180s"),(90,120,"90-120s"),
                    (45,90,"45-90s"),(0,45,"<45s")]:
    sub = [r for r in fav if lo <= r["tau"] < hi]
    if sub: report(sub, lbl)

print("\n=== per-bps bands, new model ===")
for lo, hi in [(0,2),(2,4),(4,6),(6,8),(8,10)]:
    sub = [r for r in fav if lo <= r["bps"] < hi]
    if sub: report(sub, f"bps {lo}-{hi}")

print("\n=== validation: losing-side rows (expect obs slightly below model due to spread) ===")
errs = []
for r in lose:
    pm = model_p(r["bps"], r["tau"], vals)
    p_fav_obs = 1 - r["p_obs_up"]
    errs.append(pm - p_fav_obs)
n = len(errs)
print(f"  n={n} mae={sum(abs(e) for e in errs)/n*100:.1f}pp bias={sum(errs)/n*100:+.1f}pp (positive=model above bid-implied, expected)")

print("\n=== validation: near-even rows ===")
for r in even:
    pm = model_p(max(r['bps'],0.01), r["tau"], vals)
    print(f"  tau={r['tau']:4.0f}s bps={r['bps']:.2f} p_obs={r['p_obs_up']:.3f} new={pm:.3f}")

print("\n=== payout comparison (favored) ===")
for lo, hi, lbl in [(180,10**5,">=180s"),(120,180,"120-180s"),(90,120,"90-120s"),
                    (45,90,"45-90s"),(0,45,"<45s")]:
    sub = [r for r in fav if lo <= r["tau"] < hi]
    if not sub: continue
    op = sum((1-r["p_obs_up"])/r["p_obs_up"] for r in sub)/len(sub)
    np_ = sum((1-model_p(r["bps"],r["tau"],vals))/model_p(r["bps"],r["tau"],vals) for r in sub)/len(sub)
    print(f"  {lbl:10s} n={len(sub):3d} obs_payout={op*100:6.1f}%  new_model={np_*100:6.1f}%")
