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
        print("\n🤖 Hyperliquid Copy Trading Bot v2.7")

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
                }
            else:
                b["sum_sz"] += sz
                b["sum_px_sz"] += sz * px
                if t_ms > b["max_time"]:
                    b["max_time"] = t_ms
                b["last_ms"] = now_ms

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
        # create a unique id for bot-side dedupe
        self._agg_counter += 1
        agg["_agg_id"] = f"agg_{int(time.time()*1000)}_{self._agg_counter}"
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

                if size != 0 and coin:
                    new_positions[coin] = size
                    synced_count += 1
                    if verbose:
                        direction = "LONG" if size > 0 else "SHORT"
                        print(f"   {coin}: {abs(size):.4f} ({direction})")

            with self.state_lock:
                self.open_positions = new_positions

            if verbose:
                print(f"   📊 {synced_count} position(s)\n" if synced_count else "   No positions\n")

        except Exception as e:
            if verbose:
                print(f"   ⚠️  Sync failed: {e}\n")

    def _sync_on_miss_for_coin(self, coin: str):
        if self.dry_run or self.info is None:
            return

        now = time.time()
        with self.state_lock:
            last = self.last_sync_ts.get(coin, 0.0)
            if (now - last) < self.sync_on_miss_cooldown_sec:
                return
            self.last_sync_ts[coin] = now

        self._sync_positions_from_exchange(verbose=False)

    # ------------------------
    # Pending close processing
    # ------------------------
    def _process_pending_closes_for_coin(self, coin: str):
        while True:
            with self.state_lock:
                pend = self.pending_closes.get(coin, [])
                if not pend:
                    return
                if coin not in self.open_positions:
                    return
                our_pos = float(self.open_positions.get(coin, 0.0))
                item = pend.pop(0)
                if pend:
                    self.pending_closes[coin] = pend
                else:
                    self.pending_closes.pop(coin, None)

            frac = float(item.get("frac", 1.0))
            price = item.get("price", "0")
            close_side = 'A' if our_pos > 0 else 'B'
            our_close_sz = self._round_size(coin, abs(our_pos) * frac)
            if our_close_sz <= 0:
                continue

            print(f"\n   🧩 Pending CLOSE triggered for {coin}: frac={frac:.4f}, close_sz={our_close_sz}")
            self.place_order(coin, close_side, our_close_sz, price, is_closing=True)

    # ------------------------
    # Signals / shutdown
    # ------------------------
    def _signal_handler(self, sig, frame):
        print("\n\n🛑 Shutting down...")
        try:
            self._coalesce_flush_all()
        except Exception:
            pass
        self._stop_async_pipeline()
        print(f"📊 Processed {len(self.processed_fills)} unique target fills")
        if self.dropped_fills:
            print(f"⚠️  Dropped target fills due to queue saturation: {self.dropped_fills}")
        print("\nGoodbye! 👋\n")
        sys.exit(0)

    # ------------------------
    # Sizing logic
    # ------------------------
    def calculate_position_size(self, target_size, target_price, coin):
        target_notional_usd = float(target_size) * float(target_price)
        ratio = self.copy_percentage / 100.0
        our_value_usd = target_notional_usd * ratio

        if our_value_usd > self.max_position_usd:
            our_value_usd = self.max_position_usd

        if our_value_usd < self.min_position_usd:
            print(f"      ⏭️  SKIP: our notional ${our_value_usd:.2f} < min ${self.min_position_usd:.2f}")
            return None

        our_size_raw = our_value_usd / float(target_price)
        our_size = self._round_size(coin, our_size_raw)

        if coin in self.coin_metadata:
            decimals = self.coin_metadata[coin]["szDecimals"]
            if abs(our_size - our_size_raw) > 0.0001:
                print(f"      🔧 Rounded: {our_size_raw:.6f} → {our_size} ({decimals} decimals)")

        notional_value = our_size * float(target_price)
        if notional_value < self.min_notional_usd:
            print(f"      ⏭️  SKIP: Notional ${notional_value:.2f} < ${self.min_notional_usd} min (exchange rule)")
            return None

        return our_size

    # ------------------------
    # Target position reconstruction (for proportional closes)
    # ------------------------
    def _update_target_position(self, coin, size, direction):
        prev = self.target_positions.get(coin, 0.0)
        sz = float(size)

        new = prev
        if direction.startswith("Open"):
            if "Long" in direction:
                new = prev + sz
            elif "Short" in direction:
                new = prev - sz
        elif direction.startswith("Close"):
            if "Long" in direction:
                new = prev - sz
            elif "Short" in direction:
                new = prev + sz

        with self.state_lock:
            self.target_positions[coin] = new

    # ------------------------
    # Order placement
    # ------------------------
    def place_order(self, coin, side, size, price, is_closing=False):
        is_buy = (side == 'B')
        side_name = 'BUY' if is_buy else 'SELL'
        action = 'CLOSE' if is_closing else 'OPEN'
        notional = float(size) * float(price)

        print(f"\n   📝 {action}: {side_name} {size} {coin} @ ${price} (${notional:.2f})")

        if self.dry_run:
            print("   🔵 DRY RUN (set DRY_RUN=false to enable real trading)\n")
            fake_fill = {"coin": coin, "side": side, "sz": float(size)}
            self._apply_our_fill_to_positions(fake_fill)
            return

        if is_closing:
            self._sync_positions_from_exchange(verbose=False)
            with self.state_lock:
                _has_pos = coin in self.open_positions
                our_position = self.open_positions.get(coin, 0.0)
            if not _has_pos:
                print(f"      ⏭️  SKIP: no {coin} position found on exchange (may already be closed)")
                return
            if abs(our_position) < float(size):
                print(f"      ⚠️  Adjusting close size to our actual position: {abs(our_position)}")
                size = abs(our_position)

        try:
            order_price = float(price)

            if self.slippage_tolerance_pct > 0:
                if is_buy:
                    order_price = order_price * (1 + self.slippage_tolerance_pct / 100.0)
                else:
                    order_price = order_price * (1 - self.slippage_tolerance_pct / 100.0)

            order_price = self._round_price_aggressive(coin, order_price, is_buy)

            if order_price != float(price):
                action_txt = "pay up to" if is_buy else "accept down to"
                print(f"      💡 Slippage: {action_txt} ${order_price} (vs target's ${price})")

            order_result = self.exchange.order(
                coin,
                is_buy,
                size,
                order_price,
                {"limit": {"tif": "Ioc"}},
                reduce_only=is_closing
            )

            status = order_result.get("status", "")
            resp = order_result.get("response", {})

            if status != "ok":
                print(f"      ❌ Order API failed: {resp}")
                return

            data = resp.get("data", {}) if isinstance(resp, dict) else {}
            statuses = data.get("statuses", []) if isinstance(data, dict) else []

            if isinstance(statuses, list) and statuses:
                for st in statuses:
                    if not isinstance(st, dict):
                        continue
                    if "filled" in st:
                        f = st.get("filled", {})
                        print(f"      ✅ Order status: filled totalSz={f.get('totalSz')} avgPx={f.get('avgPx')} oid={f.get('oid')}")
                    elif "resting" in st:
                        r = st.get("resting", {})
                        print(f"      🟡 Order status: resting oid={r.get('oid')}")
                    elif "error" in st:
                        print(f"      ❌ Order status: error {st.get('error')}")
                    else:
                        print(f"      ℹ️ Order status: {st}")
            else:
                print("      ℹ️ Order accepted (no detailed status). Waiting for our WS fills to confirm execution.")

        except Exception as e:
            print(f"      ❌ Exception: {e}\n")

    # ------------------------
    # Target fill processing (runs in worker threads)
    # ------------------------
    def process_fill(self, wallet_address, fill_data):
        if wallet_address.lower() != self.target_wallet:
            return

        agg_id = fill_data.get("_agg_id")
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
