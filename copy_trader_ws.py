#!/usr/bin/env python3
"""
Hyperliquid Copy Trading Bot - Ed...
"""

import os
import json
import math
import time
import datetime
import threading
import websocket
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# hyperliquid python sdk
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange


class CopyTradingBot:
    """
    Copy trading bot that listens to a target user's fills and mirrors them
    """

    def __init__(self):
        print("\n🤖 Hyperliquid Copy Trading Bot v2.9")

        load_dotenv()

        # === Configuration ===
        self.endpoint = os.getenv('HYPERLIQUID_ENDPOINT')
        self.api_key = os.getenv('HYPERLIQUID_API_KEY', '')
        self.target_wallet = os.getenv('TARGET_WALLET_ADDRESS', '').strip().lower()
        self.copy_percentage = float(os.getenv('COPY_PERCENTAGE', '5.0'))
        self.dry_run = os.getenv('DRY_RUN', 'true').lower() == 'true'
        self.max_position_usd = float(os.getenv('MAX_POSITION_SIZE_USD', '100'))
        self.min_position_usd = float(os.getenv('MIN_POSITION_SIZE_USD', '10'))
        self.max_open_positions = int(os.getenv('MAX_OPEN_POSITIONS', '4'))
        self.slippage_tolerance_pct = float(os.getenv('SLIPPAGE_TOLERANCE_PCT', '0.5'))
        self.min_notional_usd = 10.0  # Hyperliquid exchange minimum

        # Coin filtering mode
        self.coin_filter_mode = os.getenv('COIN_FILTER_MODE', 'ALL').upper()  # ALL or ENABLED
        enabled_coins_str = os.getenv('ENABLED_COINS', '').strip()
        self.enabled_coins = set([c.strip().upper() for c in enabled_coins_str.split(",") if c.strip()]) if enabled_coins_str else set()

        # Coalesce
        self.coalesce_window_ms = int(os.getenv("COALESCE_WINDOW_MS", "25"))
        self.coalesce_lock = threading.Lock()  # protect _coalesce_buf (NEW)
        self.coalesce_flusher_thread = None     # (NEW)

        # === State Tracking ===
        self.processed_fills = set()      # Avoid duplicate fills (TARGET only; includes aggregated ids)
        self.raw_target_seen = set()     # Raw target fill ids seen (pre-coalesce) to avoid double-counting
        self.open_positions = {}          # {coin: net size} - positive=long, negative=short (OUR ACCOUNT)
        self.virtual_positions = {}       # {coin: net size} - optimistic position used for sizing while our fills lag (NEW)
        self.coin_metadata = {}           # Cached size precision per coin
        self.target_positions = {}        # {coin: net size} - target trader reconstructed from fills

        # ---- Pending closes + sync-on-miss rate limit ----
        self.pending_closes = {}          # {coin: [ {frac, price, dir, ts_ms}, ... ]}
        self.last_sync_ts = {}            # {coin: last_sync_time_sec}
        self.sync_on_miss_cooldown_sec = float(os.getenv("SYNC_ON_MISS_COOLDOWN_SEC", "0.5"))

        #
        self._coalesce_buf = {}  # key -> { "first_ts": int(ms), "last_ts": int(ms), "fills":[...], "coin":..., "side":..., "dir":..., "is_closing":bool }
        self._agg_seq = 0

        self.stop_event = threading.Event()
        self.state_lock = threading.Lock()

        # WS reconnect behavior + safety flatten
        self.reconnect_delay_sec = float(os.getenv("RECONNECT_DELAY_SEC", "2.0"))
        sflat = os.getenv("SAFETY_FLATTEN_AFTER_DISCONNECT_SEC", "").strip()
        self.safety_flatten_after_sec = float(sflat) if sflat else None
        self.disconnect_start_time = None

        # target fill queue
        self.fill_queue = []
        self.fill_queue_lock = threading.Lock()

        # our wallet is derived from api key in exchange (or env)
        self.our_wallet = os.getenv("OUR_WALLET_ADDRESS", "").strip().lower() or None

        # threads
        self.ws_thread = None
        self.worker_thread = None
        self.exec_pool = ThreadPoolExecutor(max_workers=int(os.getenv("EXEC_WORKERS", "16")))

        # lag stats
        self.processed_unique_target_fills = 0

        # Hyperliquid clients
        self.info = Info(self.endpoint)
        self.exchange = Exchange(self.api_key, self.endpoint)

    # ------------------------
    # Utility + metadata
    # ------------------------
    def _coin_enabled(self, coin: str) -> bool:
        if self.coin_filter_mode == "ALL":
            return True
        if self.coin_filter_mode == "ENABLED":
            return coin.upper() in self.enabled_coins
        return True

    def _get_size_precision(self, coin: str) -> int:
        coin = coin.upper()
        if coin in self.coin_metadata:
            return self.coin_metadata[coin]

        # pull meta from Info
        try:
            meta = self.info.meta()
            universe = meta.get("universe", [])
            for a in universe:
                if a.get("name", "").upper() == coin:
                    sz_decimals = int(a.get("szDecimals", 0))
                    self.coin_metadata[coin] = sz_decimals
                    return sz_decimals
        except Exception:
            pass

        self.coin_metadata[coin] = 2
        return 2

    def _round_size(self, coin: str, size: float) -> float:
        dec = self._get_size_precision(coin)
        # round to allowed decimals
        factor = 10 ** dec
        return math.floor(size * factor + 1e-12) / factor

    # ------------------------
    # Coalescing helpers
    # ------------------------
    def _coalesce_key(self, fill: dict):
        coin = fill.get("coin", "")
        side = fill.get("side", "")
        direction = fill.get("dir", "")
        is_closing = bool(fill.get("closedPnl", 0) not in (0, "0", 0.0, None))
        return (coin, side, direction, bool(is_closing))

    def _raw_target_fill_id(self, fill: dict) -> str:
        # Stable id for raw target fills regardless of local aggregation
        return f"{fill.get('hash','')}_{fill.get('tid','')}"

    def _coalesce_add_fill(self, fill: dict):
        now_ms = int(time.time() * 1000)
        key = self._coalesce_key(fill)

        with self.coalesce_lock:
            entry = self._coalesce_buf.get(key)
            if entry is None:
                self._coalesce_buf[key] = {
                    "first_ts": now_ms,
                    "last_ts": now_ms,
                    "fills": [fill],
                    "coin": fill.get("coin", ""),
                    "side": fill.get("side", ""),
                    "dir": fill.get("dir", ""),
                    "is_closing": bool(fill.get("closedPnl", 0) not in (0, "0", 0.0, None)),
                }
                return

            entry["last_ts"] = now_ms
            entry["fills"].append(fill)

    def _coalesce_flush_due(self, now_ms: int):
        due = []
        with self.coalesce_lock:
            for k, entry in list(self._coalesce_buf.items()):
                if now_ms - entry["last_ts"] >= self.coalesce_window_ms:
                    due.append((k, entry))
                    del self._coalesce_buf[k]

        for _, entry in due:
            self._emit_coalesced(entry)

    def _coalesce_flush_all(self):
        with self.coalesce_lock:
            items = list(self._coalesce_buf.items())
            self._coalesce_buf = {}
        for _, entry in items:
            self._emit_coalesced(entry)

    def _emit_coalesced(self, entry: dict):
        fills = entry.get("fills", [])
        if not fills:
            return

        # If only one fill, pass through (but still dedup via processed_fills)
        if len(fills) == 1:
            fill = fills[0]
            self._enqueue_fill(fill)
            return

        # Aggregate
        coin = entry.get("coin", "")
        side = entry.get("side", "")
        direction = entry.get("dir", "")
        is_closing = entry.get("is_closing", False)

        # Combine size + weighted avg price, sum pnl/fees
        total_sz = 0.0
        wpx = 0.0
        closed_pnl = 0.0
        fee = 0.0
        # use latest time fields
        last = fills[-1]

        for f in fills:
            try:
                sz = float(f.get("sz", 0.0))
                px = float(f.get("px", 0.0))
                total_sz += sz
                wpx += sz * px
            except Exception:
                pass
            try:
                closed_pnl += float(f.get("closedPnl", 0.0) or 0.0)
            except Exception:
                pass
            try:
                fee += float(f.get("fee", 0.0) or 0.0)
            except Exception:
                pass

        avg_px = (wpx / total_sz) if total_sz > 0 else float(last.get("px", 0.0) or 0.0)

        self._agg_seq += 1
        agg_id = f"agg_{int(time.time()*1000)}_{self._agg_seq}"

        merged = dict(last)
        merged["_agg_id"] = agg_id
        merged["_agg_count"] = len(fills)
        merged["_agg_first_ms"] = entry.get("first_ts")
        merged["_agg_last_ms"] = entry.get("last_ts")
        merged["coin"] = coin
        merged["side"] = side
        merged["dir"] = direction
        merged["sz"] = str(total_sz)
        merged["px"] = str(avg_px)
        merged["closedPnl"] = str(closed_pnl) if is_closing else merged.get("closedPnl", "0")
        merged["fee"] = str(fee)

        self._enqueue_fill(merged)

    # ------------------------
    # Queue + worker
    # ------------------------
    def _enqueue_fill(self, fill: dict):
        now_ms = int(time.time() * 1000)
        fill["_enq_ms"] = now_ms

        with self.fill_queue_lock:
            self.fill_queue.append(fill)

    def _pop_next_fill(self):
        with self.fill_queue_lock:
            if not self.fill_queue:
                return None
            return self.fill_queue.pop(0)

    def worker_loop(self):
        while not self.stop_event.is_set():
            f = self._pop_next_fill()
            if f is None:
                # also flush coalesce
                try:
                    self._coalesce_flush_due(int(time.time() * 1000))
                except Exception:
                    pass
                time.sleep(0.001)
                continue

            try:
                self.process_fill(self.target_wallet, f)
            except Exception as e:
                print(f"❌ Error processing fill: {e}")

    # ------------------------
    # Pending closes
    # ------------------------
    def _process_pending_closes_for_coin(self, coin: str):
        # Try to execute queued close fractions once we have a position
        with self.state_lock:
            pend = list(self.pending_closes.get(coin, []))
            self.pending_closes[coin] = []

        if not pend:
            return

        # Ensure we have position (sync already attempted when queued)
        with self.state_lock:
            our_pos = float(self.open_positions.get(coin, 0.0))
        if abs(our_pos) < 1e-12:
            # still none, requeue
            with self.state_lock:
                self.pending_closes.setdefault(coin, []).extend(pend)
            return

        # Apply each fraction sequentially using latest our_pos
        for item in pend:
            frac = float(item.get("frac", 0.0))
            price = float(item.get("price", 0.0))
            if frac <= 0:
                continue

            with self.state_lock:
                our_pos = float(self.open_positions.get(coin, 0.0))
            if abs(our_pos) < 1e-12:
                continue

            our_close_sz = self._round_size(coin, abs(our_pos) * frac)
            if our_close_sz <= 0:
                continue

            close_side = 'A' if our_pos > 0 else 'B'
            self.place_order(coin, close_side, our_close_sz, price, is_closing=True)

    def _trigger_pending_close_processing(self, coin: str):
        if self.exec_pool is None:
            return
        try:
            self.exec_pool.submit(self._process_pending_closes_for_coin, coin)
        except Exception:
            pass

    # ------------------------
    # OUR fills -> position update
    # ------------------------
    def _apply_our_fill_to_positions(self, fill: dict):
        coin = fill.get("coin", "")
        side = fill.get("side", "")
        sz = fill.get("sz", None)
        if not coin or side not in ("B", "A") or sz is None:
            return

        try:
            delta = float(sz) if side == "B" else -float(sz)
        except Exception:
            return

        with self.state_lock:
            prev = float(self.open_positions.get(coin, 0.0))
            new = prev + delta
            if abs(new) < 1e-10:
                if coin in self.open_positions:
                    del self.open_positions[coin]
                # keep virtual in sync with actual
                if coin in self.virtual_positions:
                    del self.virtual_positions[coin]
            else:
                self.open_positions[coin] = new
                # keep virtual in sync with actual
                self.virtual_positions[coin] = new

            has_pending = bool(self.pending_closes.get(coin))
            has_pos = (coin in self.open_positions)

        if has_pending and has_pos:
            self._trigger_pending_close_processing(coin)

    # ------------------------
    # Position sync
    # ------------------------
    def _sync_positions_from_exchange(self, verbose=False):
        try:
            addr = self.our_wallet
            if not addr:
                return

            state = self.info.user_state(addr)
            positions = state.get("assetPositions", [])
            new_positions = {}
            synced_count = 0

            for p in positions:
                pos = p.get("position", {})
                coin = pos.get("coin", "")
                szi = pos.get("szi", None)
                if not coin or szi is None:
                    continue
                try:
                    sz = float(szi)
                except Exception:
                    continue
                if abs(sz) < 1e-12:
                    continue
                new_positions[coin] = sz
                synced_count += 1

            with self.state_lock:
                self.open_positions = new_positions
                self.virtual_positions = dict(new_positions)

            if verbose:
                if synced_count == 0:
                    print("   No positions")
                else:
                    print(f"   Synced {synced_count} positions:")
                    for coin, sz in new_positions.items():
                        side = "LONG" if sz > 0 else "SHORT"
                        print(f"   - {coin}: {abs(sz)} ({side})")

        except Exception as e:
            print(f"❌ Error syncing positions: {e}")

    def _sync_on_miss_for_coin(self, coin: str):
        now = time.time()
        with self.state_lock:
            last = self.last_sync_ts.get(coin, 0.0)
            if now - last < self.sync_on_miss_cooldown_sec:
                return
            self.last_sync_ts[coin] = now

        # sync all positions (simple)
        self._sync_positions_from_exchange(verbose=False)

    # ------------------------
    # Order sizing
    # ------------------------
    def calculate_position_size(self, coin, target_size, price):
        """
        Convert target fill size to our size (COPY_PERCENTAGE),
        clamp by max_position_usd and min_notional, round to coin precision.
        """
        try:
            target_size = float(target_size)
            price = float(price)
        except Exception:
            return 0.0

        # our notional = target notional * copy_percentage
        target_notional = abs(target_size) * price
        our_notional = (target_notional * self.copy_percentage) / 100.0

        # clamp notional
        if our_notional > self.max_position_usd:
            our_notional = self.max_position_usd

        # enforce minimum per order (bot-configured)
        if our_notional < self.min_position_usd:
            return 0.0

        # enforce exchange minimum
        if our_notional < self.min_notional_usd:
            return 0.0

        our_size = our_notional / price
        our_size = self._round_size(coin, our_size)
        return our_size

    # ------------------------
    # Target position tracking
    # ------------------------
    def _update_target_position(self, coin, size, direction):
        """
        direction: 'Open Long', 'Open Short', 'Close Long', 'Close Short'
        size is always positive
        """
        coin = coin.upper()
        try:
            size = float(size)
        except Exception:
            return

        with self.state_lock:
            prev = float(self.target_positions.get(coin, 0.0))
            new = prev

            if direction.startswith("Open Long"):
                new = prev + size
            elif direction.startswith("Open Short"):
                new = prev - size
            elif direction.startswith("Close Long"):
                new = prev - size
            elif direction.startswith("Close Short"):
                new = prev + size
            else:
                # unknown - do nothing
                return

            if abs(new) < 1e-10:
                if coin in self.target_positions:
                    del self.target_positions[coin]
            else:
                self.target_positions[coin] = new

    # ------------------------
    # Place order
    # ------------------------
    def place_order(self, coin, side, size, price, is_closing=False):
        try:
            size = float(size)
            price = float(price)
        except Exception:
            return

        if size <= 0 or price <= 0:
            return

        notional = size * price
        action = "CLOSE" if is_closing else "OPEN"
        side_name = 'BUY' if side == 'B' else 'SELL'

        print(f"\n   📝 {action}: {side_name} {size} {coin} @ ${price} (${notional:.2f})")

        # Slippage protection -> limit price
        is_buy = (side == 'B')
        limit_px = price
        try:
            sl = self.slippage_tolerance_pct / 100.0
            if is_buy:
                worst_px = price * (1.0 + sl)
                worst_px = float(f"{worst_px:.6g}")  # compact
                print(f"      💡 Slippage: pay up to ${worst_px} (vs target's ${price})")
                limit_px = worst_px
            else:
                worst_px = price * (1.0 - sl)
                worst_px = float(f"{worst_px:.6g}")
                print(f"      💡 Slippage: accept down to ${worst_px} (vs target's ${price})")
                limit_px = worst_px
        except Exception:
            pass

        if self.dry_run:
            print("      🧪 DRY RUN: order not sent")
            return

        try:
            resp = self.exchange.order(coin, is_buy, size, limit_px, {"limit": {"tif": "Ioc"}})
        except Exception as e:
            print(f"      ❌ Order error: {e}")
            return

        try:
            status = resp.get("status", "")
            if status != "ok":
                print(f"      ❌ Order status: {status} {resp}")
                return

            r = resp.get("response", {}) or {}
            data = r.get("data", {}) or {}
            statuses = data.get("statuses", [])
            if not isinstance(statuses, list):
                statuses = []

            total_filled = 0.0
            # Hyperliquid may return multiple filled statuses if the order matched several resting orders
            for st in statuses:
                if not isinstance(st, dict):
                    continue
                if "filled" in st and isinstance(st["filled"], dict):
                    f = st["filled"]
                    try:
                        filled_sz = float(f.get("totalSz", 0.0))
                    except Exception:
                        filled_sz = 0.0
                    total_filled += filled_sz
                    print(f"      ✅ Order status: filled totalSz={f.get('totalSz')} avgPx={f.get('avgPx')} oid={f.get('oid')}")
                elif "resting" in st:
                    # Not expected for IOC, but handle gracefully
                    rest = st.get("resting", {})
                    oid = rest.get("oid") if isinstance(rest, dict) else None
                    print(f"      ✅ Order status: resting oid={oid}")
                elif "error" in st:
                    err = st.get("error")
                    print(f"      ❌ Order status: error {err}")
                else:
                    # Fallback: print raw status entry
                    print(f"      ✅ Order status: {st}")

            if not statuses:
                print("      ✅ Order status: ok")

            # Optimistic position update for sizing (virtual_positions), so burst CLOSEs don't overshoot/flip
            if total_filled > 0:
                delta = total_filled if side == "B" else -total_filled
                with self.state_lock:
                    base = float(self.virtual_positions.get(coin, self.open_positions.get(coin, 0.0)))
                    newv = base + delta
                    if abs(newv) < 1e-10:
                        self.virtual_positions.pop(coin, None)
                    else:
                        self.virtual_positions[coin] = newv
        except Exception:
            print(f"      ⚠️  Order placed (could not parse status)")

    # ------------------------
    # Process target fill
    # ------------------------
    def process_fill(self, target_user, fill_data):
        if not isinstance(fill_data, dict):
            return

        # Use aggregated id if present; else hash_tid
        agg_id = fill_data.get("_agg_id", None)
        if agg_id:
            fill_id = str(agg_id)
        else:
            fill_id = f"{fill_data.get('hash', '')}_{fill_data.get('tid', '')}"

        with self.state_lock:
            if fill_id in self.processed_fills:
                return
            self.processed_fills.add(fill_id)

        # count unique
        self.processed_unique_target_fills += 1

        # Extract fields
        coin = (fill_data.get("coin") or "").upper()
        if not coin:
            return

        if not self._coin_enabled(coin):
            return

        side = fill_data.get("side", "")
        if side not in ("B", "A"):
            return
        size = float(fill_data.get("sz", 0.0) or 0.0)
        price = float(fill_data.get("px", 0.0) or 0.0)
        direction = fill_data.get("dir", "")
        closed_pnl = fill_data.get("closedPnl", 0)

        is_closing = bool(closed_pnl not in (0, "0", 0.0, None))
        action = "CLOSE" if is_closing else "OPEN"

        # ---- Lag measurements ----
        recv_ms = fill_data.get("_recv_ms", None)
        enq_ms = fill_data.get("_enq_ms", None)
        now_ms = int(time.time() * 1000)

        exchange_lag_ms = None
        ws_recv_lag_ms = None
        queue_lag_ms = None

        try:
            # Hyperliquid fill has "time" in ms
            ex_ms = int(fill_data.get("time", 0))
            if ex_ms > 0:
                exchange_lag_ms = now_ms - ex_ms
                if recv_ms is not None:
                    ws_recv_lag_ms = recv_ms - ex_ms
        except Exception:
            pass

        if enq_ms is not None:
            queue_lag_ms = now_ms - enq_ms

        qsize = 0
        with self.fill_queue_lock:
            qsize = len(self.fill_queue)

        # timestamp
        tstamp_str = datetime.datetime.now().strftime("%H:%M:%S")

        side_name = 'BUY' if side == 'B' else 'SELL'
        notional = float(size) * float(price)
        pnl_str = f" | PnL: ${closed_pnl}" if is_closing else ""

        lag_parts = []
        if exchange_lag_ms is not None:
            lag_parts.append(f"exchange_lag={exchange_lag_ms}ms")
        if ws_recv_lag_ms is not None:
            lag_parts.append(f"ws_recv_lag={ws_recv_lag_ms}ms")
        if queue_lag_ms is not None:
            lag_parts.append(f"queue_lag={queue_lag_ms}ms")
        lag_parts.append(f"queue_size={qsize}")

        print("\n" + "=" * 70)
        print(f"📩 {tstamp_str} Target {action}: {side_name} {size} {coin} @ ${price} (${notional:.2f}){pnl_str} | {', '.join(lag_parts)}")

        # ===== CLOSE logic =====
        if is_closing:
            # Prefer exchange-provided startPosition (position before this fill) for robust close fractions
            start_pos = fill_data.get("startPosition", None)
            prev_target_pos = None
            if start_pos is not None:
                try:
                    prev_target_pos = float(start_pos)
                except Exception:
                    prev_target_pos = None

            if prev_target_pos is None:
                with self.state_lock:
                    prev_target_pos = self.target_positions.get(coin, 0.0)

            target_close_sz = float(size)
            frac = (target_close_sz / abs(prev_target_pos)) if abs(prev_target_pos) > 0 else 1.0

            with self.state_lock:
                has_pos = (coin in self.virtual_positions)
                our_pos = float(self.virtual_positions.get(coin, 0.0))

            if not has_pos:
                self._sync_on_miss_for_coin(coin)

                with self.state_lock:
                    has_pos2 = (coin in self.virtual_positions)
                    our_pos2 = float(self.virtual_positions.get(coin, 0.0))

                if not has_pos2:
                    with self.state_lock:
                        self.pending_closes.setdefault(coin, []).append({
                            "frac": frac,
                            "price": price,
                            "dir": direction,
                            "ts_ms": int(time.time() * 1000),
                        })
                    print(f"   ⏳ PENDING: No {coin} position yet; queued CLOSE (frac={frac:.4f}) to retry after our fills arrive")
                    print("=" * 70)
                    self._update_target_position(coin, size, direction)
                    return
                our_pos = our_pos2

            our_close_sz = self._round_size(coin, abs(our_pos) * frac)
            if our_close_sz <= 0:
                print("   ⏭️  SKIP: Computed close size is 0")
                print("=" * 70)
                self._update_target_position(coin, size, direction)
                return

            close_side = 'A' if our_pos > 0 else 'B'
            print(f"   📉 Target close fraction: {frac:.4f} of their position")
            print(f"   📉 Our close: {our_close_sz} (from our pos {abs(our_pos):.4f})")

            self._update_target_position(coin, size, direction)
            self.place_order(coin, close_side, our_close_sz, price, is_closing=True)
            print("=" * 70)
            return

        # ===== OPEN logic =====
        our_sz = self.calculate_position_size(coin, size, price)
        if our_sz <= 0:
            # show why skipped
            our_notional = (abs(size) * price) * (self.copy_percentage / 100.0)
            print(f"      ⏭️  SKIP: our notional ${our_notional:.2f} < min ${self.min_position_usd:.2f}")
            print("=" * 70)
            self._update_target_position(coin, size, direction)
            return

        # limit number of open positions
        with self.state_lock:
            cur_open_positions = len(self.open_positions)

        if cur_open_positions >= self.max_open_positions:
            print(f"      ⏭️  SKIP: max open positions reached ({cur_open_positions}/{self.max_open_positions})")
            print("=" * 70)
            self._update_target_position(coin, size, direction)
            return

        # show our order
        our_notional = our_sz * price
        pct = (our_notional / (abs(size) * price) * 100.0) if (abs(size) * price) > 0 else 0.0
        print(f"   📊 Our open: {our_sz} (${our_notional:.2f}, {pct:.1f}% of target order)")

        # place in executor (async)
        self._update_target_position(coin, size, direction)
        self.place_order(coin, side, our_sz, price, is_closing=False)
        print("=" * 70)

    # ------------------------
    # Emergency safety: flatten everything (market-ish via IOC at mid)
    # ------------------------
    def emergency_flatten_all_positions(self):
        try:
            self._sync_positions_from_exchange(verbose=True)
            with self.state_lock:
                positions = dict(self.open_positions)

            if not positions:
                print("✅ SAFETY: No positions to flatten.")
                return

            print("\n🚨 SAFETY: Flattening all positions now...\n")
            for coin, sz in positions.items():
                # close full size
                # determine side: if long, sell; if short, buy
                side = 'A' if sz > 0 else 'B'
                # price hint: use mid if possible
                px = None
                try:
                    md = self.info.all_mids()
                    px = float(md.get(coin, 0.0) or 0.0)
                except Exception:
                    px = 0.0
                if px <= 0:
                    # fallback: don't place
                    print(f"❌ SAFETY: could not get price for {coin}, skipping")
                    continue

                self.place_order(coin, side, abs(sz), px, is_closing=True)

            print("\n✅ SAFETY flatten submitted.\n")
        except Exception as e:
            print(f"❌ SAFETY flatten error: {e}")

    # ------------------------
    # WS stream: target fills + our account events
    # ------------------------
    def stream_ws(self):
        ws_url = os.getenv("HYPERLIQUID_WS_URL", "").strip() or "wss://api.hyperliquid.xyz/ws"

        while not self.stop_event.is_set():
            try:
                print("🔌 Connecting to Hyperliquid WebSocket stream...")
                ws = websocket.create_connection(ws_url, timeout=30)

                ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "userFills", "user": self.target_wallet}
                }))

                if self.our_wallet:
                    ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "userFills", "user": self.our_wallet}
                    }))
                    ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "userEvents", "user": self.our_wallet}
                    }))
                    ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "orderUpdates", "user": self.our_wallet}
                    }))

                print("✅ WebSocket stream started. Waiting for events...")

                self.disconnect_start_time = None

                while not self.stop_event.is_set():
                    try:
                        ws.settimeout(30)
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        try:
                            self._coalesce_flush_due(int(time.time() * 1000))
                        except Exception:
                            pass
                        try:
                            ws.send(json.dumps({"method": "ping"}))
                        except Exception:
                            pass
                        continue

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    channel = msg.get("channel", "")
                    data = msg.get("data", None)

                    if channel in ("subscriptionResponse", "pong"):
                        continue

                    if channel == "userFills" and isinstance(data, dict):
                        if data.get("isSnapshot", False):
                            continue
                        user = (data.get("user") or "").lower()
                        fills = data.get("fills", [])
                        if not isinstance(fills, list):
                            continue

                        if user == self.target_wallet:
                            # stamp WS receive time on each fill
                            recv_ms = int(time.time() * 1000)
                            for fill in fills:
                                if isinstance(fill, dict):
                                    fill["_recv_ms"] = recv_ms
                                    # Pre-coalesce dedup (prevents double-counting target_positions on reconnect duplicates)
                                    rid = self._raw_target_fill_id(fill)
                                    with self.state_lock:
                                        if rid in self.raw_target_seen:
                                            continue
                                        self.raw_target_seen.add(rid)

                                    self._coalesce_add_fill(fill)
                            continue

                        if self.our_wallet and user == self.our_wallet:
                            for fill in fills:
                                if isinstance(fill, dict):
                                    self._apply_our_fill_to_positions(fill)
                            continue

                        continue

                    if channel == "userEvents" and isinstance(data, dict):
                        user = (data.get("user") or "").lower()
                        if self.our_wallet and user == self.our_wallet:
                            fills = data.get("fills", [])
                            if isinstance(fills, list):
                                for fill in fills:
                                    if isinstance(fill, dict):
                                        self._apply_our_fill_to_positions(fill)
                        continue

                    if channel == "orderUpdates" and isinstance(data, dict):
                        continue

            except Exception as e:
                print(f"❌ WebSocket error: {e}")

                try:
                    self._coalesce_flush_all()
                except Exception:
                    pass

                if self.disconnect_start_time is None:
                    self.disconnect_start_time = time.time()

                if self.safety_flatten_after_sec is not None:
                    elapsed = time.time() - self.disconnect_start_time
                    if elapsed >= self.safety_flatten_after_sec:
                        print(f"\n🚨 SAFETY: Disconnected for {elapsed:.1f}s (>= {self.safety_flatten_after_sec}s). Flattening positions...\n")
                        self.emergency_flatten_all_positions()
                        self.disconnect_start_time = time.time()

                print(f"🔁 Connection lost. Reconnecting in {self.reconnect_delay_sec} seconds...\n")
                time.sleep(self.reconnect_delay_sec)
                continue

    # ------------------------
    # Start / stop
    # ------------------------
    def start(self):
        # init account value / wallet
        try:
            if not self.our_wallet:
                # best-effort: try to read from exchange
                try:
                    self.our_wallet = (self.exchange.wallet_address or "").lower()
                except Exception:
                    self.our_wallet = None
        except Exception:
            self.our_wallet = None

        # account value
        try:
            if self.our_wallet:
                st = self.info.user_state(self.our_wallet)
                av = st.get("marginSummary", {}).get("accountValue", None)
                if av is not None:
                    print(f"💰 Account value: ${float(av):.2f}")
        except Exception:
            pass

        print(f"\n🎯 Copying trades from: {self.target_wallet}")
        print(f"📊 Copy percentage: {self.copy_percentage:.1f}%")
        print(f"🧪 Dry run mode: {self.dry_run}")
        print(f"🧩 Coin filter: {self.coin_filter_mode if self.coin_filter_mode!='ENABLED' else ('ENABLED ' + str(sorted(self.enabled_coins)))}")
        print(f"📌 Max open positions: {self.max_open_positions}")
        print(f"📉 Slippage tolerance: {self.slippage_tolerance_pct:.3g}%")
        print(f"⛔ Min notional per order: ${self.min_position_usd:.2f}")
        print(f"🧮 Coalesce window: {self.coalesce_window_ms}ms\n")

        # Sync positions
        if self.our_wallet:
            print("🔄 Syncing positions...")
            self._sync_positions_from_exchange(verbose=True)
        else:
            print("⚠️  OUR_WALLET_ADDRESS not set; cannot track our positions reliably.")

        # Start worker
        self.worker_thread = threading.Thread(target=self.worker_loop, daemon=True)
        self.worker_thread.start()

        # Start WS
        self.ws_thread = threading.Thread(target=self.stream_ws, daemon=True)
        self.ws_thread.start()

        # Main loop
        try:
            while True:
                time.sleep(0.2)
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            self.stop_event.set()
            try:
                self._coalesce_flush_all()
            except Exception:
                pass

            print(f"📊 Processed {self.processed_unique_target_fills} unique target fills")
            print("\nGoodbye! 👋")


if __name__ == "__main__":
    bot = CopyTradingBot()
    bot.start()
