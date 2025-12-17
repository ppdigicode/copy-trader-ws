import websocket
import ssl
#!/usr/bin/env python3
"""
Hyperliquid Copy Trading Bot - Educational Example

⚠️  WARNING: This bot trades with REAL money on MAINNET
    - Always test in DRY_RUN mode first
    - Never share your private keys
    - You can lose money - use at your own risk
    - Not financial advice

This is a simple, educational example of copy trading on Hyperliquid.
The code is kept simple and well-commented to make it easy to understand.
"""

import os
import sys
import json
import signal
import math
import time
import datetime
from decimal import Decimal
from dotenv import load_dotenv
import eth_account

# Import gRPC generated code

# Import Hyperliquid SDK
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants


class CopyTradingBot:
    """
    Copy trading bot for Hyperliquid

    HOW IT WORKS:
    1. Streams real-time trades via gRPC
    2. Detects trades from target wallet
    3. Calculates position size based on copy percentage
    4. Places IOC orders (immediate execution, no waiting)
    5. Syncs positions from exchange (not local tracker)

    KEY FEATURES:
    - IOC orders: Fills immediately or cancels (perfect for copy trading)
    - Direction detection: Uses 'dir' field ("Open Long", "Close Short", etc.)
    - Size precision: Rounds to correct decimals per coin (ZORA=0, BTC=5, ETH=4)
    - Position syncing: Fetches real positions from exchange before closing
    - Safety limits: Min/max position size, max open positions

    CONFIGURATION (.env):
    - TARGET_WALLET_ADDRESS: Wallet to copy
    - COPY_PERCENTAGE: % of TARGET ORDER notional to use (e.g., 1.0 = 1% de son ordre)
    - DRY_RUN: true/false (simulate or trade)
    - MIN_POSITION_SIZE_USD: Skip trades smaller than this
    - MAX_POSITION_SIZE_USD: Cap position size
    - MAX_OPEN_POSITIONS: Maximum concurrent positions (default: 4)
    - SLIPPAGE_TOLERANCE_PCT: Allow paying X% more for better fills
    - COIN_FILTER_MODE: ALL or ENABLED (default: ALL)
    - ENABLED_COINS: Comma-separated list (only used when mode=ENABLED)
    """

    def __init__(self):
        """Load configuration and set up the bot"""

        print("\n🤖 Hyperliquid Copy Trading Bot")

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
        self.wallet_address = os.getenv('HYPERLIQUID_WALLET_ADDRESS', '')

        # === State Tracking ===
        self.processed_fills = set()      # Avoid duplicate fills
        self.open_positions = {}          # {coin: size} - positive=long, negative=short (NOTRE COMPTE)
        self.coin_metadata = {}           # Cached size precision per coin
        self.target_positions = {}        # {coin: size} - position du TRADER (reconstruite à partir de ses fills)
        self.last_recv_ts = None       # Timestamp of last received WS message (for lag indicator)

        signal.signal(signal.SIGINT, self._signal_handler)

        # === Validation ===
        self._validate_config()

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
                print(f"   Account: ${account_value:.2f}")
            except Exception as e:
                print(f"   ⚠️  Could not fetch account: {e}")

            # Load coin metadata and sync positions
            self._fetch_coin_metadata()
            self._sync_positions_from_exchange()
        else:
            # Même en DRY_RUN, on essaie d'avoir Info pour les métadonnées
            try:
                self.info = Info(constants.MAINNET_API_URL, skip_ws=True)
                self._fetch_coin_metadata()
            except Exception as e:
                print(f"   ⚠️  Could not init Info in DRY_RUN: {e}")

        # === Display Configuration ===
        print(f"   Target: {self.target_wallet[:8]}...{self.target_wallet[-6:]}")
        print(f"   Copy ratio: {self.copy_percentage}% of target order | Limits: ${self.min_position_usd}-${self.max_position_usd} | Max positions: {self.max_open_positions}")

        # Show coin filter mode
        if self.coin_filter_mode == 'ALL':
            print(f"   Coins: ALL (no filter)")
        elif self.enabled_coins:
            print(f"   Coins: {', '.join(sorted(self.enabled_coins))}")
        else:
            print(f"   Coins: ALL (ENABLED mode but no coins specified)")

        mode = "🔵 DRY RUN" if self.dry_run else "🔴 LIVE TRADING"
        print(f"   Mode: {mode}\n")

    def _validate_config(self):
        """Validate configuration from .env"""

        if not self.target_wallet:
            print("\n❌ TARGET_WALLET_ADDRESS not set in .env")
            sys.exit(1)

        if not self.target_wallet.startswith('0x'):
            print("\n❌ TARGET_WALLET_ADDRESS must start with '0x'")
            sys.exit(1)

        if not (0 < self.copy_percentage <= 100):
            print("\n❌ COPY_PERCENTAGE must be between 0 and 100")
            sys.exit(1)

        if not self.dry_run and (not self.private_key or not self.wallet_address):
            print("\n❌ Live trading requires HYPERLIQUID_PRIVATE_KEY and HYPERLIQUID_WALLET_ADDRESS")
            sys.exit(1)

    def _fetch_coin_metadata(self):
        """Fetch metadata for all coins (size decimals, price tick sizes, etc.)"""
        if not self.info:
            return
        try:
            print("🔧 Fetching coin metadata...")
            meta = self.info.meta()
            universe = meta.get("universe", [])

            # Cache metadata for each coin (used for size and price rounding)
            for asset in universe:
                name = asset.get("name", "")
                sz_decimals = asset.get("szDecimals", 4)  # Default to 4 if missing

                # tickSize en général en "basis points" (100 = 0.01)
                tick_size_raw = asset.get("tickSize", "100")
                try:
                    tick_size = float(tick_size_raw) / 10000.0
                except Exception:
                    tick_size = 0.01

                if name:
                    self.coin_metadata[name] = {
                        "szDecimals": sz_decimals,
                        "tickSize": tick_size,
                    }

            print(f"   ✅ Loaded {len(self.coin_metadata)} coins\n")

        except Exception as e:
            print(f"   ⚠️  Metadata fetch failed: {e} (using defaults)\n")

    def _round_size(self, coin, size: float) -> float:
        """Round size to the correct decimal precision for this coin."""
        if coin in self.coin_metadata:
            decimals = self.coin_metadata[coin]["szDecimals"]
        else:
            decimals = 4
        return float(f"{size:.{decimals}f}")

    def _round_price_aggressive(self, coin, price: float, is_buy: bool) -> float:
        """
        Round price so Hyperliquid accepts it (perps rules):
          - <= 5 significant figures, unless it's an integer (integers always allowed)
          - <= (MAX_DECIMALS - szDecimals) decimal places, where MAX_DECIMALS=6 for perps

        We round *aggressively*:
          - BUY  -> round UP (more likely to fill)
          - SELL -> round DOWN
        """
        MAX_DECIMALS = 6  # perps
        sz_dec = 0
        try:
            if coin in self.coin_metadata:
                sz_dec = int(self.coin_metadata[coin].get("szDecimals", 0))
        except Exception:
            sz_dec = 0

        max_dp = max(0, MAX_DECIMALS - sz_dec)

        def sig_figs(x: float) -> int:
            # Count significant figures for a non-integer price representation.
            s = f"{abs(x):.16g}"
            if "e" in s or "E" in s:
                # convert scientific notation to plain string
                s = f"{abs(x):.16f}".rstrip("0").rstrip(".")
            s = s.lstrip("0").replace(".", "")
            return len(s) if s else 1

        # Try from most precise allowed to least, until it satisfies sig-figs rule.
        for dp in range(max_dp, -1, -1):
            scale = 10 ** dp
            if is_buy:
                p = math.ceil(price * scale) / scale
            else:
                p = math.floor(price * scale) / scale

            # If dp>0 and too many significant figures, reduce precision.
            if dp > 0 and sig_figs(p) > 5:
                continue

            # dp==0 is always valid (integer price always allowed)
            return float(p)

        # Fallback: integer aggressive
        return float(math.ceil(price) if is_buy else math.floor(price))

    def _sync_positions_from_exchange(self, verbose=True):
        """
        Fetch actual positions from exchange (not local tracker)
        """
        try:
            if verbose:
                print("🔄 Syncing positions...")

            user_state = self.info.user_state(self.wallet_address)
            asset_positions = user_state.get("assetPositions", [])

            # Clear and rebuild position tracker from exchange reality
            self.open_positions = {}

            synced_count = 0
            for asset_pos in asset_positions:
                position = asset_pos.get("position", {})
                coin = position.get("coin", "")
                szi = position.get("szi", "0")  # Positive=long, negative=short

                size = float(szi)

                if size != 0 and coin:
                    self.open_positions[coin] = size
                    synced_count += 1
                    if verbose:
                        direction = "LONG" if size > 0 else "SHORT"
                        print(f"   {coin}: {abs(size):.4f} ({direction})")

            if verbose:
                print(f"   📊 {synced_count} position(s)\n" if synced_count else "   No positions\n")

        except Exception as e:
            if verbose:
                print(f"   ⚠️  Sync failed: {e}\n")

    def _signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully"""
        print("\n\n🛑 Shutting down...")
        print(f"📊 Processed {len(self.processed_fills)} unique fills")
        print("\nGoodbye! 👋\n")
        sys.exit(0)

    def calculate_position_size(self, target_size, target_price, coin):
        """
        Calculate our position size as a fixed ratio of the target trader's order size.

        - target_size  : size of the target trader's order (in coin units)
        - target_price : fill price of the target trader's order
        - COPY_PERCENTAGE : ratio in % between the target order notional and ours
            * 100 => same notional as the target order
            * 50  => half of the target order notional
            * 1   => 1% of the target order notional

        Returns our position size, or None if trade should be skipped.
        """

        # 1) Notional USD of the target trader's order
        target_notional_usd = float(target_size) * float(target_price)

        # 2) Our notional = COPY_PERCENTAGE % of the target notional
        ratio = self.copy_percentage / 100.0
        our_value_usd = target_notional_usd * ratio

        # 3) Apply our own min/max USD limits
        if our_value_usd > self.max_position_usd:
            our_value_usd = self.max_position_usd

        if our_value_usd < self.min_position_usd:
            print(f"      ⏭️  SKIP: ${our_value_usd:.2f} < ${self.min_position_usd} min")
            return None

        # 4) Convert USD value to coin size
        our_size_raw = our_value_usd / float(target_price)

        # Round to correct decimal precision
        our_size = self._round_size(coin, our_size_raw)

        # Log if significant rounding occurred
        if coin in self.coin_metadata:
            decimals = self.coin_metadata[coin]["szDecimals"]
            if abs(our_size - our_size_raw) > 0.0001:
                print(f"      🔧 Rounded: {our_size_raw:.6f} → {our_size} ({decimals} decimals)")

        # Final check: notional value must be >= $10 (Hyperliquid minimum)
        notional_value = our_size * float(target_price)
        if notional_value < self.min_notional_usd:
            print(f"      ⏭️  SKIP: Notional ${notional_value:.2f} < ${self.min_notional_usd} min (exchange rule)")
            return None

        return our_size

    def _update_target_position(self, coin: str, size_str: str, direction: str):
        """
        Met à jour la position du TRADER (wallet cible) à partir d'un fill.

        - On reconstruit sa position par coin pour connaître la taille
          AVANT un close et calculer la proportion fermée.
        """
        try:
            sz = float(size_str)
        except Exception:
            return

        prev = self.target_positions.get(coin, 0.0)
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

        self.target_positions[coin] = new

    def place_order(self, coin, side, size, price, is_closing=False):
        """
        Place order on Hyperliquid (or simulate in dry-run mode)

        Uses IOC (Immediate or Cancel) orders for instant execution
        """

        is_buy = (side == 'B')
        side_name = 'BUY' if is_buy else 'SELL'
        action = 'CLOSE' if is_closing else 'OPEN'
        notional = float(size) * float(price)

        print(f"\n   📝 {action}: {side_name} {size} {coin} @ ${price} (${notional:.2f})")

        if self.dry_run:
            # === DRY RUN MODE: Simulate only ===
            print(f"   🔵 DRY RUN (set DRY_RUN=false to enable real trading)\n")

            # Update position tracker (for dry-run simulation)
            if not is_closing:
                position_size = size if is_buy else -size
                self.open_positions[coin] = self.open_positions.get(coin, 0) + position_size
            else:
                if coin in self.open_positions:
                    del self.open_positions[coin]

        else:
            # === LIVE TRADING MODE ===

            # For closing orders, verify position exists on exchange first
            if is_closing:
                self._sync_positions_from_exchange(verbose=False)

                if coin not in self.open_positions:
                    print(f"      ⚠️  No {coin} position found on exchange (may not have filled yet)\n")
                    return

                # Verify direction matches (don't close LONG when you have SHORT)
                our_position = self.open_positions[coin]
                closing_long = our_position > 0 and not is_buy
                closing_short = our_position < 0 and is_buy

                if not (closing_long or closing_short):
                    print(f"      ⚠️  Direction mismatch: {our_position:.4f} vs {side_name}\n")
                    return

            try:
                # 1) Base = trader fill price
                order_price = float(price)

                # 2) Slippage (opens AND closes)
                if self.slippage_tolerance_pct > 0:
                    if is_buy:
                        # BUY (open long or close short): up to +X% higher
                        order_price = order_price * (1 + self.slippage_tolerance_pct / 100.0)
                    else:
                        # SELL (open short or close long): down to -X% lower
                        order_price = order_price * (1 - self.slippage_tolerance_pct / 100.0)

                # 3) Aggressive tick rounding
                order_price = self._round_price_aggressive(coin, order_price, is_buy)

                if order_price != float(price):
                    action_txt = "pay up to" if is_buy else "accept down to"
                    print(f"      💡 Slippage: {action_txt} ${order_price} (vs target's ${price})")

                # 4) Place IOC order (Immediate or Cancel)
                order_result = self.exchange.order(
                    coin,
                    is_buy,
                    size,
                    order_price,
                    {"limit": {"tif": "Ioc"}},  # IOC = Immediate or Cancel
                    reduce_only=is_closing
                )

                # Process order response
                if order_result["status"] == "ok":
                    response = order_result["response"]["data"]
                    statuses = response.get("statuses", [])

                    if statuses:
                        status = statuses[0]

                        # IOC orders: either "filled" or error (no "resting")
                        if "filled" in status:
                            filled = status["filled"]
                            total_sz = filled.get("totalSz", size)
                            avg_px = filled.get("avgPx", price)

                            # Check if partially filled
                            partial = float(total_sz) < float(size)
                            partial_str = f" ({(float(total_sz)/float(size)*100):.1f}%)" if partial else ""

                            print(f"      ✅ Filled: {total_sz} @ ${avg_px}{partial_str}")

                            # Update our position tracker
                            if not is_closing:
                                position_size = float(total_sz) if is_buy else -float(total_sz)
                                self.open_positions[coin] = self.open_positions.get(coin, 0) + position_size
                            else:
                                if coin in self.open_positions:
                                    old_pos = self.open_positions[coin]
                                    delta = float(total_sz)

                                    if old_pos > 0:
                                        # On était LONG, on ferme en vendant
                                        new_pos = old_pos - delta
                                    else:
                                        # On était SHORT, on ferme en achetant
                                        new_pos = old_pos + delta

                                    if abs(new_pos) < 1e-8:
                                        del self.open_positions[coin]
                                    else:
                                        self.open_positions[coin] = new_pos

                        else:
                            # Error or unknown status
                            if "error" in status:
                                error_msg = status["error"]
                                print(f"      ❌ Error: {error_msg}")

                                error_lower = error_msg.lower()
                                if "invalid size" in error_lower and coin in self.coin_metadata:
                                    decimals = self.coin_metadata[coin]["szDecimals"]
                                    print(f"      💡 {coin} requires {decimals} decimal places")
                                elif "reduce only" in error_lower:
                                    print(f"      💡 Position doesn't exist or hasn't filled yet")
                            else:
                                print(f"      ⚠️  Unknown status: {status}")

                    # Sync positions after opening orders
                    if not is_closing:
                        self._sync_positions_from_exchange(verbose=False)

                    # Show current positions
                    if self.open_positions:
                        positions_str = ", ".join(
                            [f"{c}: {abs(s):.4f} ({'LONG' if s > 0 else 'SHORT'})" for c, s in self.open_positions.items()]
                        )
                        print(f"      📊 Positions ({len(self.open_positions)}/{self.max_open_positions}): {positions_str}\n")
                    else:
                        print(f"      📊 No positions\n")

                else:
                    # Order failed at API level
                    error_msg = order_result.get("response", order_result)
                    print(f"      ❌ Failed: {error_msg}")

                    error_str = str(error_msg).lower()
                    if "notional" in error_str or "minimum" in error_str:
                        print(f"      💡 Increase COPY_PERCENTAGE or MIN_POSITION_SIZE_USD\n")
                    elif "reduce" in error_str or "position" in error_str:
                        print(f"      💡 Position sync issue\n")

            except Exception as e:
                print(f"      ❌ Exception: {e}\n")

                error_str = str(e).lower()
                if "notional" in error_str or "minimum" in error_str:
                    print(f"      💡 Hyperliquid minimum: $10 per order\n")

    def process_fill(self, wallet_address, fill_data):
        """
        Process a trade from the block fills stream
        """

        # === Filter: Only target wallet ===
        if wallet_address.lower() != self.target_wallet:
            return

        # === Filter: No duplicates ===
        fill_id = f"{fill_data.get('hash', '')}_{fill_data.get('tid', '')}"
        if fill_id in self.processed_fills:
            return
        self.processed_fills.add(fill_id)

        # === Extract trade data ===
        coin = fill_data.get('coin', '')
        side = fill_data.get('side', '')  # 'B'=Buy, 'A'=Sell
        size = fill_data.get('sz', '0')
        price = fill_data.get('px', '0')
        closed_pnl = fill_data.get('closedPnl', '0')
        direction = fill_data.get('dir', '')  # "Open Long", "Close Short", etc.

        # === Determine action: Open or Close ===
        is_closing = direction.startswith('Close') if direction else (closed_pnl and float(closed_pnl) != 0)

        # === Filter: Coin whitelist (only if ENABLED mode) ===
        if self.coin_filter_mode == 'ENABLED':
            if self.enabled_coins and coin not in self.enabled_coins:
                return  # Skip coins not in the enabled list

        # === Filter: Position limit (opening only) ===
        if not is_closing:
            if coin not in self.open_positions and len(self.open_positions) >= self.max_open_positions:
                print(f"\n⏭️  SKIP: Max positions ({len(self.open_positions)}/{self.max_open_positions})")
                return

        # === Log detected trade ===
        action = "CLOSE" if is_closing else "OPEN"
        side_name = 'BUY' if side == 'B' else 'SELL'
        notional = float(size) * float(price)
        pnl_str = f" | PnL: ${closed_pnl}" if is_closing else ""

        # === Lag indicator (WS recv time vs fill timestamp) ===
        try:
            fill_ts_raw = fill_data.get('time') or fill_data.get('timestamp') or fill_data.get('ts')
            if fill_ts_raw is not None and self.last_recv_ts is not None:
                fill_ts = float(fill_ts_raw)
                # Hyperliquid timestamps are often in milliseconds
                if fill_ts > 1e12:
                    fill_ts /= 1000.0
                lag_sec = self.last_recv_ts - fill_ts
                recv_dt = datetime.datetime.fromtimestamp(self.last_recv_ts)
                fill_dt = datetime.datetime.fromtimestamp(fill_ts)
                print(f"   🕒 Lag: {lag_sec:.1f}s (recv {recv_dt.strftime('%H:%M:%S')}, fill {fill_dt.strftime('%H:%M:%S')})")
        except Exception:
            pass

        print(f"\n{'='*70}")
        print(f"🎯 TARGET: {action} {direction}")
        print(f"   {coin}: {side_name} {size} @ ${price} (${notional:.2f}){pnl_str}")
        print(f"{'='*70}")

        # === Calculate our position size ===
        if is_closing:
            # 1) On doit avoir une position chez nous
            if coin not in self.open_positions:
                print(f"   ⏭️  SKIP: No {coin} position (bot may have started after open)")
                print("=" * 70)
                # On met quand même à jour la position du TRADER
                self._update_target_position(coin, size, direction)
                return

            # 2) Taille de la position du TRADER AVANT ce close
            prev_target_pos = self.target_positions.get(coin, 0.0)
            target_close_sz = float(size)

            if abs(prev_target_pos) > 0:
                frac = target_close_sz / abs(prev_target_pos)
                if frac > 1.0:
                    frac = 1.0
            else:
                # Si on ne connaît pas sa position (reboot, etc.), on ferme tout
                frac = 1.0

            # 3) Notre position actuelle
            our_pos = self.open_positions[coin]
            our_abs_pos = abs(our_pos)

            # Taille de close théorique (non arrondie)
            raw_our_size = our_abs_pos * frac

            # Si quasiment 100% → ferme tout
            if frac > 0.999 or our_abs_pos - raw_our_size < 1e-8:
                raw_our_size = our_abs_pos

            # 4) Arrondi de la taille de close selon les décimales du coin
            our_size = self._round_size(coin, raw_our_size)

            if abs(our_size - raw_our_size) > 1e-8:
                print(f"   🔧 Rounded close size: {raw_our_size:.8f} → {our_size}")

            # Si l'arrondi donne 0, on ne peut rien faire
            if our_size <= 0:
                print(f"   ⏭️  SKIP: Rounded close size is 0 for {coin}")
                print("=" * 70)
                self._update_target_position(coin, size, direction)
                return

            # 5) Check notional minimum après arrondi
            notional_value = our_size * float(price)
            if notional_value < self.min_notional_usd:
                print(f"   ⏭️  SKIP: Notional ${notional_value:.2f} < ${self.min_notional_usd} min")
                print("=" * 70)
                # Mettre à jour la position du TRADER malgré tout
                self._update_target_position(coin, size, direction)
                return

            is_long = our_pos > 0
            print(f"   📊 Our close: {our_size} ({'LONG' if is_long else 'SHORT'}, ~{frac*100:.1f}% of our pos)")

        else:
            # For opens: calculate based on copy percentage (ratio of target order)
            our_size = self.calculate_position_size(size, price, coin)

            if our_size is None:
                print("=" * 70)
                # Mettre à jour la position du TRADER malgré tout
                self._update_target_position(coin, size, direction)
                return  # Filtered out (too small, etc.)

            notional_value = our_size * float(price)
            print(f"   📊 Our open: {our_size} (${notional_value:.2f}, {self.copy_percentage}% of target order)")

        # === Mettre à jour la position du TRADER APRÈS avoir utilisé l'ancienne taille ===
        self._update_target_position(coin, size, direction)

        # === Place the order ===
        self.place_order(coin, side, our_size, price, is_closing)
        print("=" * 70)

    def stream_user_fills_ws(self):
        """Stream target user's fills using the official Hyperliquid WebSocket.

        Docs:
          - WebSocket URL: wss://api.hyperliquid.xyz/ws
          - Subscribe: { "method": "subscribe", "subscription": { "type": "userFills", "user": "<address>" } }
          - Heartbeats: { "method": "ping" } every < 60s if idle.

        We only act on *streaming* messages (isSnapshot == False). Snapshot messages are ignored.
        """

        ws_url = os.getenv("HYPERLIQUID_WS_URL", "").strip() or "wss://api.hyperliquid.xyz/ws"

        while True:
            try:
                print("🔌 Connecting to Hyperliquid WebSocket stream...")
                ws = websocket.create_connection(
                    ws_url,
                    timeout=30,
                    sslopt={"cert_reqs": ssl.CERT_REQUIRED},
                )

                # Subscribe to userFills for the target wallet
                sub_msg = {
                    "method": "subscribe",
                    "subscription": {
                        "type": "userFills",
                        "user": self.target_wallet,
                        # "aggregateByTime": False,  # optional
                    },
                }
                ws.send(json.dumps(sub_msg))
                print("✅ WebSocket stream started. Waiting for fills...")

                # reset disconnect timer on successful connect
                self.disconnect_start_time = None

                last_msg_time = time.time()

                while True:
                    try:
                        raw = ws.recv()  # blocks (timeout set in create_connection)
                        last_msg_time = time.time()
                        self.last_recv_ts = last_msg_time
                    except websocket.WebSocketTimeoutException:
                        # Keepalive ping if idle; server closes if no server->client message in 60s.
                        # We ping every 30s idle (timeout) to keep the connection alive.
                        try:
                            ws.send(json.dumps({"method": "ping"}))
                        except Exception:
                            raise
                        continue

                    if not raw:
                        continue

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    channel = msg.get("channel", "")
                    data = msg.get("data", None)

                    # Ignore ack / pong
                    if channel in ("subscriptionResponse", "pong"):
                        continue

                    if channel != "userFills" or not isinstance(data, dict):
                        continue

                    # The first message after subscribe is usually a snapshot: ignore it by default.
                    if data.get("isSnapshot", False):
                        continue

                    fills = data.get("fills", [])
                    if not isinstance(fills, list):
                        continue

                    for fill in fills:
                        if not isinstance(fill, dict):
                            continue
                        # The fill dict is compatible with our existing process_fill() expectations:
                        # coin, side ('B'/'A'), px, sz, dir, hash, tid, time, closedPnl, ...
                        self.process_fill(self.target_wallet, fill)

            except Exception as e:
                print(f"❌ WebSocket error: {e}")

                # Start / update disconnect timer
                if self.disconnect_start_time is None:
                    self.disconnect_start_time = time.time()

                # Emergency flatten if disconnected too long
                if self.safety_flatten_after_sec is not None:
                    elapsed = time.time() - self.disconnect_start_time
                    if elapsed >= self.safety_flatten_after_sec:
                        print(f"\n🚨 SAFETY: Disconnected for {elapsed:.0f}s (>= {self.safety_flatten_after_sec}s). Flattening positions...\n")
                        self.emergency_flatten_all_positions()
                        # After flatten, reset timer so we don't spam
                        self.disconnect_start_time = time.time()

                print(f"🔁 Connection lost. Reconnecting in {self.reconnect_delay_sec} seconds...\n")
                time.sleep(self.reconnect_delay_sec)
                continue
    def run(self):
        """Start the copy trading bot"""
        self.stream_user_fills_ws()


def main():
    """Entry point"""

    if not os.path.exists('.env'):
        print("\n❌ No .env file found")
        print("📝 Setup: create .env file in this folder")
        print("   Then edit .env and set TARGET_WALLET_ADDRESS\n")
        sys.exit(1)

    bot = CopyTradingBot()
    bot.run()


if __name__ == '__main__':
    main()