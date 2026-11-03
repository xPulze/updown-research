"""Explain unavailable executable quotes without inventing liquidity."""
import math
import time


def diagnose(levels, feed=None, token=None, now=None, stale_after=10):
    now = time.time() if now is None else now
    if levels is not None:
        def count(rows):
            return sum(1 for p, s in rows if math.isfinite(float(p))
                       and math.isfinite(float(s)) and 0 < float(p) < 1 and float(s) > 0)
        bids, asks = count(levels[0]), count(levels[1])
        if not bids and not asks:
            return 'book_empty'
        if not asks:
            return 'book_no_asks'
        if not bids:
            return 'book_no_bids'
        return 'book_two_sided'
    book = getattr(feed, '_books', {}).get(str(token)) if feed else None
    task = getattr(feed, '_task', None) if feed else None
    if not book:
        return 'book_snapshot_missing'
    if task is None or task.done():
        return 'book_stream_disconnected'
    if now - book.get('ts', 0) > stale_after:
        return 'book_stale'
    raw = (list(book.get('bids', {}).items()), list(book.get('asks', {}).items()))
    state = diagnose(raw)
    if state != 'book_two_sided':
        return state
    return 'book_touch_mismatch' if book.get('touch_mismatch') else 'book_unavailable'
