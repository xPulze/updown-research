# Binance 5-Min Stake Monitor — User Manual

## What this is
A live dashboard that watches BTC/USDT price on Binance and tells you, for each 5-minute window, whether the current partial move is likely to continue — and how much of *your* capital to stake if so. You place the actual bet yourself in the Binance app; this tool never touches your money or places trades automatically.

## 1. Log in
- Go to **your-dashboard-host/binance**
- Enter the username and password you were given
- You'll land on the live dashboard
- A **📖 User Manual** button at the top of the dashboard reopens this document any time

## 2. Set your capital (do this first)
- Near the top, find **"Your capital (USDT)"**
- Enter your current balance and click **Save**
- This is what your stake amounts are calculated from. After every win or loss, come back and update it to your new balance so stakes keep scaling with your actual capital (compounding). No one else updates this for you — if you leave it stale, your stake sizes will be wrong.
- **Capital is capped at $200.** This isn't arbitrary — it's liquidity and spread. On a $15,000 orderbook 5-min product, a small stake fills at close to the quoted price, and slippage is trivial. As your stake size grows, you eat a bigger chunk of the available book on your side: the effective spread widens, you get filled at a worse price on entry, and you're forced to exit at a worse price too. That gap between the price the dashboard is calling and the price you actually get compounds directly into your win rate — the same signal that wins at $100 can lose at $1,000 purely on execution cost, not on the call being wrong. Capping capital at $200 keeps your fills close enough to the model's assumptions that the historical win rates still apply to you.

## 3. Reading the dashboard
- **Time zone** — a dropdown near the top lets you pick the time zone the Window column and charts are shown in. It defaults to auto-detecting your device's zone; change it once and it's remembered on that browser.
- The charts (Live Price & Tick, 1-Minute Candles, Master 24h Range, Price Range 24h) are context only — you don't act on them directly. Click a chart's title bar to collapse/expand it.
- **Live — current window's To Win** — three cards that update continuously (every few seconds) while a window is in progress, previewing where each of the three stake opportunities stands right now:
  - 💰 **To Win** — if you've staked, shows the live payout on that stake; if you haven't, shows what the payout would be if you bought this side right now.
  - 💎 **Value** — live progress toward the 30% value re-entry (see below).
  - 🌙 **Late** — live dollar-move-from-open and payout in the final 90 seconds, ticking toward the $15–20 firing band (not a bps rule).
  - These cards are a preview only — the instruction to act still comes from the table below (the Stake / 💎 Value / 🌙 Late columns).
- The **table** below is what matters. Each row is one 5-minute window, shown in the time zone you picked above. Above the table, three filter dropdowns (Tier / Stake / 🌙 Late) let you narrow it down — e.g. only staked windows, or only windows where the late trigger fired — for reviewing history; they don't change any live behavior.
  - **T=0 Lean** — a dim, muted arrow shown the moment a window opens, before any real signal has had a chance to develop. **This is not a stake signal** — it's a coin-flip-adjacent early guess (~52% backtest accuracy) so you have something to look at while T+2/T+3 confirmation is pending. Never place money on this column alone; wait for an actual Tier/Stake, 💎 Value, or 🌙 Late call.
  - **Tier** — how strong the signal is: WEAK → MODERATE → STRONG → VERY STRONG → EXTREME. Higher tier = historically more accurate.
  - **ADX** — a trend-strength check; a ✓ means it confirms the tier.
  - **Stake** — this is the actual instruction:
    - **NO STAKE** → sit this window out. (It may still show which way price leaned — that's for info only, not an instruction to bet.)
    - A side + dollar amount, e.g. `HIGHER $35` → bet that direction, for that amount.
    - An **EARLY** badge next to the tier means the call was made before T+3 (the move already qualified earlier) — same instruction, just sooner. Act on it the same way.
  - **💎 Value** — a second stake chance on windows that already staked STRONG or better: if price pulls back toward the start price (without flipping direction), the same bet suddenly pays much more (~30%+ instead of ~25%). When this column shows `🔥 SIDE $X`, place that side/amount immediately — the value moment can pass quickly.
  - **🌙 Late** — a third, independent stake chance in the **last 90 seconds** of a window. It fires when the move from the window's open price lands in a **$15–20 band and holds there for 20 seconds** (moves smaller than $15 are a coin flip; moves bigger than $20 have little payout left) — even on windows that said NO STAKE at the main decision:
    - `👁 watching` → the trigger is scanning the final 90s right now
    - `🔥 SIDE $X` → act NOW — place that side/amount immediately (you'll have at least ~45s)
    - `✓ / ✗` → how a fired late stake settled; `no fire` → the window ended without one
  - **Result** — once the window finishes: CORRECT or WRONG.
  - There's a **🔔 Signal sound** button above the table — click it once to get an audible alarm whenever *any* of the three stake signals fires (main T+2/T+3 decision, 💎 Value, or 🌙 Late), so you don't need to watch the screen. The main decision plays a distinct lower two-note tone; Value and Late share a higher four-note tone — so you can tell which one fired without looking.
- Scoreboards above the table:
  - **All-time** — every window ever recorded, survives restarts.
  - **Per-tier auto on/off gate** — shows each tier's live rolling accuracy vs. its own backtest bar. A tier that's underperforming pauses itself automatically (shown here) until it recovers — informational, nothing for you to act on.
  - **Recent** — a dropdown lets you switch this card between the **last 12 hours**, **last 24 hours**, or **all-time**; it matches whatever range you pick.

## 4. When to act
- The dashboard normally decides each window at **T+3** (3 minutes in) — but if the move already qualifies earlier, the call comes at **T+2 or mid-window** (marked EARLY). Either way: the moment the Stake column fills in is the moment to act.
- Place the bet as soon as you see it — every second you wait, Binance's own odds move against you on an obvious move.
- A **🌙 late stake** can additionally fire in the last 90 seconds (see above). It always leaves you at least ~45 seconds to act.
- **Binance can stop accepting orders well before the window closes on a decisive move** — confirmed as early as 35 seconds before expiry on an ~8bps move (the order book simply runs out of anyone willing to take the losing side). It is **not** a fixed "~5 seconds" cutoff — the more obvious the move, the earlier it can lock. If the "Buy" button is greyed out or shows "Orders Not Allowed," that window is done; don't wait for it to come back.

## 5. Placing the stake on the Binance app
1. Open the Binance app and find the 5-minute BTC prediction product (look for "Event Contract" / "Higher-Lower" under Trade or a dedicated Prediction section — Binance's exact menu naming shifts occasionally).
2. Make sure you're on the **same 5-minute window** that's currently active on the dashboard (times should line up).
3. Choose the side shown in the Stake column (Higher or Lower).
4. Enter the dollar amount shown and confirm.
5. Done — Binance settles the contract automatically at the 5-minute mark. Nothing else to do until the next alert.

## 6. Rules to not lose money
- Only stake when the dashboard shows an actual side **and** dollar amount. `NO STAKE` means no stake, even on a hunch. The **T=0 Lean** column is never a reason to stake by itself — it's explicitly a low-confidence, informational-only guess.
- Never bet more than the amount shown, and never increase your bet to chase a loss back.
- Some losses are expected — stake sizes are built around each tier's live, measured accuracy versus its own **real-payout breakeven** (a damped Kelly formula, calibrated against real app payout screenshots rather than a flat model, capped at 5% of your capital per window). Tiers automatically pause themselves when their recent accuracy drops below their own bar, and a window is skipped outright — not just sized smaller — whenever the tier's real payout would need better-than-current accuracy to break even. It's a statistical edge, not a guarantee on any single window. If you hit a losing streak, stop for the day instead of raising your stake size.
- Two other safeguards run quietly in the background: an early (T+2) call has to hold its direction for a short dwell before it's allowed to fire, and during the New York equities open (09:30–12:00 America/New York) the minimum move required to stake is raised. Neither needs action from you — they just mean fewer, more selective alerts during those conditions.
- Keep your capital number current (Step 2) — everything downstream depends on it being accurate.

## Questions / issues
Ping the owner if the dashboard looks wrong, the timing feels off, or you're unsure what a row means.
