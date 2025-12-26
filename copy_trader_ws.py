#!/usr/bin/env python3
"""
Hyperliquid Copy Trading Bot - Educational Example

⚠️  WARNING: This bot trades with REAL money on MAINNET
    - Always test in DRY_RUN mode first
    - Never share your private keys
    - You can lose money - use at your own risk
    - Not financial advice
"""

import os
import sys
import json
import signal
import math
import time
import datetime
import threading
import hashlib
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from dotenv import load_dotenv
import eth_account
import websocket

# Hyperliquid official SDK
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants


class CopyTradingBot:
    """
    Copy trading bot that listens to a target user's fills and mirrors them
    """

    def __init__(self):
        print("\n🤖 Hyperliquid Copy Trading Bot v2.8")

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
        self.enabled_coins = set(c.strip() for c in enabled_coins_str.split(',')) if enabled_coins_str else None

        # Credentials for live trading
        self.private_key = os.getenv('HYPERLIQUID_PRIVATE_KEY', '')
        self.wallet_address = os.getenv('HYPERLIQUID_WALLET_ADDRESS', '').strip()
        self.our_wallet = self.wallet_address.lower() if self.wallet_address else None

        # Safety / reconnect config
        self.reconnect_delay_sec = float(os.getenv("RECONNECT_DELAY_SEC", "2.0"))
        safety_flatten = os.getenv("SAFETY_FLATTEN_AFTER_SEC", "").strip()
        self.safety_flatten_after_sec = float(safety_flatten) if safety_flatten else None
        self.disconnect_start_time = None

        # ---- Coalescing ----
        self.coalesce_window_ms = int(os.getenv("COALESCE_WINDOW_MS", "100"))
        self._coalesce_buf = {}  # key -> {sum_sz, sum_px_sz, max_time, first_ms, last_ms, template_fill}
        self._agg_counter = 0

        # ---- Option B: periodic flusher thread (NEW) ----
        self.coalesce_flush_interval_ms = int(os.getenv("COALESCE_FLUSH_INTERVAL_MS", "25"))
        self.coalesce_lock = threading.Lock()  # protect _coalesce_buf (NEW)
        self.coalesce_flusher_thread = None     # (NEW)

        # === State Tracking ===
        self.processed_fills = set()      # Avoid duplicate fills (TARGET only; includes aggregated ids)
        self.raw_target_seen = set()     # Raw target fill ids seen (pre-coalesce) to avoid double-counting
        self.open_positions = {}          # {coin: net size} - positive=long, negative=short (OUR ACCOUNT)
        self.coin_metadata = {}           # Cached size precision per coin
        self.target_positions = {}        # {coin: net size} - target trader reconstructed from fills

        # ---- Pending closes + sync-on-miss rate limit ----
        self.pending_closes = {}          # {coin: [ {frac, price, dir, ts_ms}, ... ]}
        self.last_sync_ts = {}            # {coin: last_sync_time_sec}
        self.sync_on_miss_cooldown_sec = float(os.getenv("SYNC_ON_MISS_COOLDOWN_SEC", "0.5"))

        # === Async pipeline (avoid blocking WS receiver on order execution) ===
        self.state_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.fill_queue_max = int(os.getenv('FILL_QUEUE_MAX', '5000'))
        self.order_workers = int(os.getenv('ORDER_WORKERS', '4'))
        self.fill_queue = Queue(maxsize=self.fill_queue_max)
        self.exec_pool = None
        self.dispatcher_thread = None
        self.dropped_fills = 0

        signal.signal(signal.SIGINT, self._signal_handler)

        # Basic checks
        if not self.target_wallet:
            print("\n❌ ERROR: TARGET_WALLET_ADDRESS not set in .env\n")
            sys.exit(1)

        if not self.dry_run:
            if not self.private_key or not self.wallet_address:
                print("\n❌ ERROR: Live mode requires HYPERLIQUID_PRIVATE_KEY and HYPERLIQUID_WALLET_ADDRESS in .env\n")
                sys.exit(1)

        # === Initialize Hyperliquid SDK ===
        self.info = None
        self.exchange = None

        if not self.dry_run:
            account = eth_account.Account.from_key(self.private_key)
            self.info = Info(constants.MAINNET_API_URL, skip_ws=True)
            self.exchange = Exchange(account, constants.MAINNET_API_URL, account_address=self.wallet_address)

            # Fetch account value
            try:
                user_state = self.info.user_state(self.wallet_address)
                account_value = float(user_state["marginSummary"]["accountValue"])
                print(f"💰 Account value: ${account_value:.2f}")
            except Exception as e:
                print(f"⚠️  Could not fetch account value: {e}")

        print(f"\n🎯 Copying trades from: {self.target_wallet}")
        print(f"📊 Copy percentage: {self.copy_percentage}%")
        print(f"🧪 Dry run mode: {self.dry_run}")
        if self.coin_filter_mode == 'ENABLED':
            print(f"🧩 Coin filter: ENABLED (allowed: {sorted(self.enabled_coins) if self.enabled_coins else []})")
        else:
            print("🧩 Coin filter: ALL")
        print(f"📌 Max open positions: {self.max_open_positions}")
        print(f"📉 Slippage tolerance: {self.slippage_tolerance_pct}%")
        print(f"⛔ Min notional per order: ${self.min_notional_usd:.2f}")
        print(f"🧮 Coalesce window: {self.coalesce_window_ms}ms\n")

        # Cache metadata
        try:
            self._fetch_coin_metadata()
        except Exception as e:
            print(f"⚠️  Could not fetch coin metadata: {e}")

        # Sync our positions at startup
        if not self.dry_run:
            try:
                self._sync_positions_from_exchange(verbose=True)
            except Exception as e:
                print(f"⚠️  Could not sync positions: {e}")

    # ------------------------
    # Async pipeline (target fills)
    # ------------------------
    def _start_async_pipeline(self):
        if self.exec_pool is not None:
            return
        self.exec_pool = ThreadPoolExecutor(max_workers=max(1, self.order_workers))
        self.dispatcher_thread = threading.Thread(target=self._dispatcher_loop, name="fill-dispatcher", daemon=True)
        self.dispatcher_thread.start()

        # ---- Option B: start periodic flusher (NEW) ----
        if self.coalesce_flusher_thread is None:
            self.coalesce_flusher_thread = threading.Thread(
                target=self._coalesce_flusher_loop,
                name="coalesce-flusher",
                daemon=True
            )
            self.coalesce_flusher_thread.start()

    def _stop_async_pipeline(self):
        self.stop_event.set()
        try:
            if self.exec_pool is not None:
                self.exec_pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

    def _enqueue_fill(self, fill: dict):
        """Enqueue TARGET fill quickly (non-blocking) and tag enqueue time for queue lag."""
        if self.stop_event.is_set():
            return
        f = dict(fill)
        f["_enq_ms"] = int(time.time() * 1000)
        try:
            self.fill_queue.put_nowait(f)
        except Exception:
            self.dropped_fills += 1
            if self.dropped_fills == 1 or (self.dropped_fills % 100) == 0:
                print(f"⚠️  Fill queue full: dropped {self.dropped_fills} fill(s). Consider increasing FILL_QUEUE_MAX or ORDER_WORKERS.")

    def _dispatcher_loop(self):
        while not self.stop_event.is_set():
            try:
                fill = self.fill_queue.get(timeout=0.25)
            except Exception:
                continue
            try:
                self.exec_pool.submit(self.process_fill, self.target_wallet, fill)
            except Exception as e:
                print(f"❌ Dispatcher error: {e}")
            finally:
                try:
                    self.fill_queue.task_done()
                except Exception:
                    pass

    # ------------------------
    # Coalescing
    # ------------------------
    def _raw_target_fill_id(self, fill: dict) -> str:
        """Best-effort stable id for a raw TARGET fill (used for pre-coalesce dedup)."""
        h = fill.get("hash", "")
        tid = fill.get("tid", "")
        if h or tid:
            return f"{h}_{tid}"
        # Fallback (rare): build a deterministic key from fields that usually identify a fill
        coin = fill.get("coin", "")
        side = fill.get("side", "")
        t = fill.get("time", "")
        px = fill.get("px", "")
        sz = fill.get("sz", "")
        dir_ = fill.get("dir", "")
        return f"fb_{coin}_{side}_{dir_}_{t}_{px}_{sz}"

    def _coalesce_key(self, fill: dict):
        coin = fill.get("coin", "")
        side = fill.get("side", "")
        direction = fill.get("dir", "") or ""
        closed_pnl = fill.get("closedPnl", "0")
        is_closing = direction.startswith("Close") if direction else (closed_pnl and float(closed_pnl) != 0)
        return (coin, side, direction, bool(is_closing))

    def _coalesce_add_fill(self, fill: dict):
        """
        Add one TARGET fill to coalescing buffer. Flushes any buckets older than window.
        """
        now_ms = int(time.time() * 1000)
        key = self._coalesce_key(fill)

        try:
            sz = float(fill.get("sz", 0))
        except Exception:
            sz = 0.0
        try:
            px = float(fill.get("px", 0))
        except Exception:
            px = 0.0
        try:
            t_ms = int(float(fill.get("time", now_ms)))
        except Exception:
            t_ms = now_ms

        if sz <= 0 or px <= 0:
            return

        with self.coalesce_lock:  # (NEW)
            b = self._coalesce_buf.get(key)
            if b is None:
                self._coalesce_buf[key] = {
                    "sum_sz": sz,
                    "sum_px_sz": sz * px,
                    "max_time": t_ms,
                    "first_ms": now_ms,
                    "last_ms": now_ms,
                    "template": dict(fill),
                    "count": 1,
                    "ids": [self._raw_target_fill_id(fill)],
                }
            else:
                b["sum_sz"] += sz
                b["sum_px_sz"] += sz * px
                if t_ms > b["max_time"]:
                    b["max_time"] = t_ms
                b["last_ms"] = now_ms
                b["count"] = int(b.get("count", 1)) + 1
                try:
                    b.setdefault("ids", []).append(self._raw_target_fill_id(fill))
                except Exception:
                    pass

        self._coalesce_flush_due(now_ms)

    def _coalesce_flush_due(self, now_ms: int):
        with self.coalesce_lock:  # (NEW)
            if not self._coalesce_buf:
                return
            win = self.coalesce_window_ms
            to_flush = []
            for k, b in self._coalesce_buf.items():
                if (now_ms - b["last_ms"]) >= win:
                    to_flush.append(k)

            for k in to_flush:
                b = self._coalesce_buf.pop(k, None)
                if not b:
                    continue
                self._enqueue_aggregated_bucket(k, b)

    def _coalesce_flush_all(self):
        with self.coalesce_lock:  # (NEW)
            if not self._coalesce_buf:
                return
            for k, b in list(self._coalesce_buf.items()):
                self._coalesce_buf.pop(k, None)
                self._enqueue_aggregated_bucket(k, b)

    def _enqueue_aggregated_bucket(self, key, bucket):
        sum_sz = bucket.get("sum_sz", 0.0)
        sum_px_sz = bucket.get("sum_px_sz", 0.0)
        if sum_sz <= 0 or sum_px_sz <= 0:
            return
        vwap = sum_px_sz / sum_sz

        tmpl = bucket.get("template", {})
        agg = dict(tmpl)
        agg["sz"] = str(sum_sz)
        agg["px"] = str(vwap)
        agg["time"] = bucket.get("max_time", tmpl.get("time"))
        # ---- propagate WS receive timestamp for ws_recv_lag ----
        agg["_recv_ms"] = bucket.get("last_ms")
        # create a deterministic id for bot-side dedupe (stable across reconnect duplicates)
        ids = bucket.get("ids") or []
        try:
            ids_str = "|".join(sorted(str(x) for x in ids))
            digest = hashlib.sha1(ids_str.encode("utf-8")).hexdigest()[:16] if ids_str else ""
        except Exception:
            digest = ""
        self._agg_counter += 1
        agg["_agg_n"] = int(bucket.get("count", 1))
        agg["_agg_id"] = f"agg_{digest}_{self._agg_counter}" if digest else f"agg_{int(time.time()*1000)}_{self._agg_counter}"
        self._enqueue_fill(agg)

    # ---- Option B: periodic flusher loop (NEW) ----
    def _coalesce_flusher_loop(self):
        interval = max(1, int(self.coalesce_flush_interval_ms))
        while not self.stop_event.is_set():
            try:
                now_ms = int(time.time() * 1000)
                self._coalesce_flush_due(now_ms)
            except Exception:
                pass
            time.sleep(interval / 1000.0)

    # ------------------------
    # Metadata / rounding
    # ------------------------
    def _fetch_coin_metadata(self):
        if self.info is None:
            self.info = Info(constants.MAINNET_API_URL, skip_ws=True)

        meta = self.info.meta()
        universe = meta.get("universe", [])
        for asset in universe:
            coin = asset.get("name")
            sz_decimals = asset.get("szDecimals", 0)
            self.coin_metadata[coin] = {"szDecimals": sz_decimals}

    def _round_size(self, coin, size):
        decimals = self.coin_metadata.get(coin, {}).get("szDecimals", 0)
        q = Decimal(str(size)).quantize(Decimal(10) ** -decimals)
        return float(q)

    def _round_price_aggressive(self, coin, price, is_buy):
        if price <= 0:
            return price

        s = f"{price:.16f}".rstrip("0").rstrip(".")
        max_dp = len(s.split(".")[1]) if "." in s else 0

        def sig_figs(x: float) -> int:
            s2 = f"{abs(x):.16f}".rstrip("0").rstrip(".")
            s2 = s2.lstrip("0").replace(".", "")
            return len(s2) if s2 else 1

        for dp in range(max_dp, -1, -1):
            scale = 10 ** dp
            p = math.ceil(price * scale) / scale if is_buy else math.floor(price * scale) / scale
            if dp > 0 and sig_figs(p) > 5:
                continue
            if sig_figs(p) <= 5:
                return p

        mag = int(math.floor(math.log10(price))) if price > 0 else 0
        scale = 10 ** (mag - 4)
        return math.ceil(price / scale) * scale if is_buy else math.floor(price / scale) * scale

    # ------------------------
    # Position tracking (OUR side from WS)
    # ------------------------
    def _trigger_pending_close_processing(self, coin: str):
        if self.exec_pool is None:
            return
        try:
            self.exec_pool.submit(self._process_pending_closes_for_coin, coin)
        except Exception:
            pass

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
            else:
                self.open_positions[coin] = new

            has_pending = bool(self.pending_closes.get(coin))
            has_pos = (coin in self.open_positions)

        if has_pending and has_pos:
            self._trigger_pending_close_processing(coin)

    # ------------------------
    # Exchange sync (fallback / debug)
    # ------------------------
    def _sync_positions_from_exchange(self, verbose=True):
        try:
            if verbose:
                print("🔄 Syncing positions...")

            user_state = self.info.user_state(self.wallet_address)
            asset_positions = user_state.get("assetPositions", [])

            new_positions = {}
            synced_count = 0

            for asset_pos in asset_positions:
                position = asset_pos.get("position", {})
                coin = position.get("coin", "")
                szi = position.get("szi", "0")
                size = float(szi)

                if coin and abs(size) > 1e-10:
                    new_positions[coin] = size
                    synced_count += 1

            with self.state_lock:
                self.open_positions = new_positions

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
            if (now - last) < self.sync_on_miss_cooldown_sec:
                return
            self.last_sync_ts[coin] = now

        try:
            self._sync_positions_from_exchange(verbose=False)
        except Exception:
            pass

    def _process_pending_closes_for_coin(self, coin: str):
        with self.state_lock:
            closes = list(self.pending_closes.get(coin, []))
            if not closes:
                return
            our_pos = float(self.open_positions.get(coin, 0.0))
            has_pos = coin in self.open_positions

        if (not has_pos) or abs(our_pos) <= 1e-10:
            return

        self.pending_closes.pop(coin, None)

        for c in closes:
            frac = float(c.get("frac", 1.0))
            price = c.get("price", "0")
            direction = c.get("dir", "")
            ts_ms = c.get("ts_ms", None)

            our_close_sz = self._round_size(coin, abs(our_pos) * frac)
            if our_close_sz <= 0:
                continue

            close_side = 'A' if our_pos > 0 else 'B'
            print("\n" + "=" * 70)
            tstamp_str = ""
            if ts_ms is not None:
                try:
                    dt = datetime.datetime.fromtimestamp(ts_ms / 1000.0)
                    tstamp_str = dt.strftime("%H:%M:%S")
                except Exception:
                    tstamp_str = ""
            print(f"📌 {tstamp_str} RETRY CLOSE: {coin} frac={frac:.4f} our_close={our_close_sz} (pos={abs(our_pos):.4f})")
            self.place_order(coin, close_side, our_close_sz, price, is_closing=True)
            print("=" * 70)

    # ------------------------
    # Order placement
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

        # ---- slippage guard ----
        is_buy = (side == 'B')
        slippage = self.slippage_tolerance_pct / 100.0
        if is_buy:
            worst_px = price * (1.0 + slippage)
            worst_px = self._round_price_aggressive(coin, worst_px, True)
            print(f"      💡 Slippage: pay up to ${worst_px} (vs target's ${price})")
            limit_px = worst_px
        else:
            worst_px = price * (1.0 - slippage)
            worst_px = self._round_price_aggressive(coin, worst_px, False)
            print(f"      💡 Slippage: accept down to ${worst_px} (vs target's ${price})")
            limit_px = worst_px

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
            if status == "ok":
                r = resp.get("response", {})
                oid = r.get("oid", None)
                if "filled" in r:
                    filled = r["filled"]
                    print(f"      ✅ Order status: filled totalSz={filled.get('totalSz')} avgPx={filled.get('avgPx')} oid={oid}")
                elif "resting" in r:
                    print(f"      ✅ Order status: resting oid={oid}")
                else:
                    print(f"      ✅ Order status: ok oid={oid}")
            else:
                print(f"      ❌ Order status: {status} {resp}")
        except Exception:
            print(f"      ⚠️  Order placed (could not parse status)")

    # ------------------------
    # Compute our position size (OPEN)
    # ------------------------
    def calculate_position_size(self, target_size, target_price, coin):
        try:
            target_size = float(target_size)
            target_price = float(target_price)
        except Exception:
            return None

        if target_size <= 0 or target_price <= 0:
            return None

        target_notional = target_size * target_price
        our_notional = target_notional * (self.copy_percentage / 100.0)

        if our_notional < self.min_position_usd:
            print(f"      ⏭️  SKIP: our notional ${our_notional:.2f} < min ${self.min_position_usd:.2f}")
            return None

        our_notional = min(our_notional, self.max_position_usd)
        our_size = our_notional / target_price
        our_size = self._round_size(coin, our_size)

        if our_size <= 0:
            print("      ⏭️  SKIP: Computed size is 0")
            return None

        if (our_size * target_price) < self.min_notional_usd:
            print(f"      ⏭️  SKIP: our notional ${(our_size * target_price):.2f} < min ${self.min_notional_usd:.2f}")
            return None

        # Print rounding info
        raw_size = target_size * (self.copy_percentage / 100.0)
        if abs(raw_size - our_size) > 1e-12:
            dec = self.coin_metadata.get(coin, {}).get("szDecimals", 0)
            print(f"      🔧 Rounded: {raw_size:.{dec+6}f} → {our_size} ({dec} decimals)")

        return our_size

    # ------------------------
    # Target position reconstruction (for close fractions)
    # ------------------------
    def _update_target_position(self, coin, size, direction):
        try:
            sz = float(size)
        except Exception:
            sz = 0.0

        with self.state_lock:
            prev = float(self.target_positions.get(coin, 0.0))

            if direction.startswith("Open"):
                # Side is encoded via dir: Long/Short...
                if "Short" in direction:
                    new = prev - sz
                else:
                    new = prev + sz
            else:
                # Close decreases magnitude in direction of position
                if prev < 0:
                    new = prev + sz
                else:
                    new = prev - sz

            if abs(new) < 1e-10:
                if coin in self.target_positions:
                    del self.target_positions[coin]
            else:
                self.target_positions[coin] = new

    # ------------------------
    # Signal handler
    # ------------------------
    def _signal_handler(self, sig, frame):
        print("\n\n🛑 Shutting down...")

        try:
            self._coalesce_flush_all()
        except Exception:
            pass

        self._stop_async_pipeline()
        self.stop_event.set()

        with self.state_lock:
            unique = len(self.processed_fills)
        print(f"📊 Processed {unique} unique target fills")

        sys.exit(0)

    # ------------------------
    # Main fill processor
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

        coin = fill_data.get('coin', '')
        side = fill_data.get('side', '')  # 'B'=Buy, 'A'=Sell
        size = fill_data.get('sz', '0')
        price = fill_data.get('px', '0')
        closed_pnl = fill_data.get('closedPnl', '0')
        direction = fill_data.get('dir', '')

        is_closing = direction.startswith('Close') if direction else (closed_pnl and float(closed_pnl) != 0)

        if self.coin_filter_mode == 'ENABLED':
            if self.enabled_coins and coin not in self.enabled_coins:
                return

        if not is_closing:
            with self.state_lock:
                _already_open = coin in self.open_positions
                _open_count = len(self.open_positions)
            if (not _already_open) and (_open_count >= self.max_open_positions):
                print(f"\n⏭️  SKIP: Max positions ({_open_count}/{self.max_open_positions})")
                return

        # ===== Lag metrics =====
        now_ms = int(time.time() * 1000)

        fill_time_ms = None
        ts = fill_data.get("time", None)
        if ts is not None:
            try:
                fill_time_ms = int(float(ts))
            except Exception:
                fill_time_ms = None

        exchange_lag_ms = None
        if fill_time_ms is not None:
            exchange_lag_ms = max(0, now_ms - fill_time_ms)

        # ---- WS receive lag ----
        ws_recv_ms = fill_data.get("_recv_ms", None)
        ws_recv_lag_ms = None
        if fill_time_ms is not None and ws_recv_ms is not None:
            try:
                ws_recv_lag_ms = max(0, int(ws_recv_ms) - fill_time_ms)
            except Exception:
                ws_recv_lag_ms = None

        enq_ms = fill_data.get("_enq_ms", None)
        queue_lag_ms = None
        if enq_ms is not None:
            try:
                queue_lag_ms = max(0, now_ms - int(enq_ms))
            except Exception:
                queue_lag_ms = None

        try:
            qsize = self.fill_queue.qsize()
        except Exception:
            qsize = -1

        tstamp_str = ""
        if fill_time_ms is not None:
            try:
                dt = datetime.datetime.fromtimestamp(fill_time_ms / 1000.0)
                tstamp_str = dt.strftime("%H:%M:%S")
            except Exception:
                tstamp_str = ""

        action = "CLOSE" if is_closing else "OPEN"
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
                has_pos = (coin in self.open_positions)
                our_pos = float(self.open_positions.get(coin, 0.0))

            if not has_pos:
                self._sync_on_miss_for_coin(coin)

                with self.state_lock:
                    has_pos2 = (coin in self.open_positions)
                    our_pos2 = float(self.open_positions.get(coin, 0.0))

                if not has_pos2:
                    with self.state_lock:
                        self.pending_closes.setdefault(coin, []).append({
                            "frac": frac,
                            "price": price,
                            "dir": direction,
                            "ts_ms": fill_time_ms,
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
        our_size = self.calculate_position_size(size, price, coin)
        if our_size is None:
            print("=" * 70)
            self._update_target_position(coin, size, direction)
            return

        notional_value = our_size * float(price)
        print(f"   📊 Our open: {our_size} (${notional_value:.2f}, {self.copy_percentage}% of target order)")

        self._update_target_position(coin, size, direction)
        self.place_order(coin, side, our_size, price, is_closing=False)
        print("=" * 70)

    # ------------------------
    # Safety flatten (unchanged)
    # ------------------------
    def emergency_flatten_all_positions(self):
        if self.dry_run or self.exchange is None or self.info is None:
            print("⚠️  SAFETY flatten skipped (dry-run or missing exchange/info).")
            return

        try:
            self._sync_positions_from_exchange(verbose=False)
            with self.state_lock:
                positions = dict(self.open_positions)

            if not positions:
                print("✅ SAFETY: No positions to flatten.")
                return

            print(f"🚨 SAFETY: Flattening {len(positions)} position(s)...")
            for coin, pos in positions.items():
                if pos == 0:
                    continue
                is_buy = pos < 0
                side = 'B' if is_buy else 'A'
                size = abs(pos)

                px = 0
                try:
                    ctx = self.info.meta_and_asset_ctxs()
                    universe = ctx[0]["universe"]
                    ctxs = ctx[1]
                    idx = None
                    for i, a in enumerate(universe):
                        if a.get("name") == coin:
                            idx = i
                            break
                    if idx is not None:
                        mark = float(ctxs[idx].get("markPx", 0))
                        px = mark if mark > 0 else 0
                except Exception:
                    px = 0

                if px <= 0:
                    px = 1.0

                try:
                    self.place_order(coin, side, size, px, is_closing=True)
                except Exception as e:
                    print(f"❌ SAFETY: Failed to flatten {coin}: {e}")

            print("✅ SAFETY: Flatten attempt complete.\n")

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

                    if not raw:
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
                                        # Avoid unbounded growth (best-effort; does not affect correctness beyond window)
                                        if len(self.raw_target_seen) > 250000:
                                            self.raw_target_seen.clear()
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

    def run(self):
        self._start_async_pipeline()
        self.stream_ws()


def main():
    if not os.path.exists('.env'):
        print("\n❌ No .env file found")
        print("📝 Setup: create .env file in this folder")
        print("   Then edit .env and set TARGET_WALLET_ADDRESS\n")
        sys.exit(1)

    bot = CopyTradingBot()
    bot.run()


if __name__ == '__main__':
    main()
