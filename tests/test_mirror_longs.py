import asyncio
import importlib
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest


SYMBOL = "FOOUSDT"
ENTRIES = [(50, 5, 0.90), (75, 5, 0.92), (100, 10, 1.00), (150, 20, 1.10)]


@pytest.fixture
def strategy(monkeypatch, tmp_path):
    monkeypatch.setenv("BOT_AUTOSTART", "false")
    app = importlib.import_module("app")
    for setting, value in {
        "DEFAULT_STOP_LOSS_USD": -8.0,
        "TAKE_PROFIT_FRACTION": 0.14284,
        "TP_OVERRIDES": {},
        "LONG_SL_REFERENCE": "short_entry",
        "ENTRY_LEVELS": [50, 75, 100, 150, 200, 250, 300],
        "ENTRY_NOTIONALS": [5, 5, 10, 20, 40, 80, 160],
        "MIN_GAIN_FILTER": 20.0,
        "MAX_PRICE_BLOCK": 1.5,
        "USE_LIVE_CHANGE": True,
        "KLINE_ALLOW_NO_DATA": True,
        "REVENGE_ENABLED": False,
        "REVENGE_DIRECTION": "opposite",
        "REVENGE_MULT": 2.0,
        "REVENGE_RECOVER_PCT": 50.0,
        "PAPER_MODE": False,
        "LIVE_TRADING": True,
        "EXECUTOR_URL": "",
        "STATS_FILE": str(tmp_path / "stats.jsonl"),
        "STATE_FILE": str(tmp_path / "state.json"),
        "SETTINGS_FILE": str(tmp_path / "settings.json"),
        "RECOVERY_FILE": str(tmp_path / "recovery.json"),
        "UI_SETTINGS": {},
    }.items():
        monkeypatch.setattr(app, setting, value)

    bot = app.TradingBot()
    bot.store.writes_enabled = True
    signals = []
    monkeypatch.setattr(bot.executor, "notify_async", lambda payload, **kwargs: signals.append(payload))

    async def unexpected_request(*args, **kwargs):
        pytest.fail("Las posiciones de la estrategia no deben enviar órdenes directas a Binance")

    monkeypatch.setattr(bot.client, "request", unexpected_request)
    return app, bot, signals


async def _enter(bot, entries=ENTRIES):
    for level, notional, price in entries:
        assert await bot._ensure_short(SYMBOL, level, notional, price, level, mirror_allowed=level == 100)


async def _tick(bot, price):
    bot._check_exits(SYMBOL, price)
    while bot._bg_tasks:
        await asyncio.gather(*list(bot._bg_tasks))


def _market_ticks(bot):
    open_24h = 0.50
    ticker = SimpleNamespace(open_24h=open_24h, change_pct=0.0, ts=0.0, last_price=0.0)
    cache = SimpleNamespace(price_cache={}, mark_cache={}, ticker_cache={SYMBOL: ticker},
                            ensure_symbols=lambda symbols: None)
    bot.price_cache = cache
    bot.all_symbols = [SYMBOL]
    bot._update_tradable()

    async def tick(price):
        ticker.ts = time.time()
        ticker.last_price = price
        ticker.change_pct = (price / open_24h - 1) * 100
        cache.price_cache[SYMBOL] = (price, ticker.ts)
        bot._evaluate_symbol(SYMBOL)
        while bot._bg_tasks:
            await asyncio.gather(*list(bot._bg_tasks))

    return tick


@pytest.mark.parametrize(("first_price", "cross_prices"), [
    (0.95, (1.00, 1.25)),
    (0.95, (1.25,)),
    (0.65, (1.00, 1.00, 1.25)),
])
def test_market_cross_of_100_starts_mirror_and_150_adds_to_it(strategy, first_price, cross_prices):
    _, bot, signals = strategy
    tick = _market_ticks(bot)

    async def run():
        await tick(first_price)
        assert SYMBOL not in bot.long_positions
        assert not signals
        for price in cross_prices:
            await tick(price)
            expected_notional = 10 if price < 1.25 else 30
            assert bot.long_positions[SYMBOL].notional == pytest.approx(expected_notional)

    asyncio.run(run())

    assert [(s["level"], s["notional"]) for s in signals] == [(100, 10), (150, 20)]
    assert len({s["trade_id"] for s in signals}) == 1


@pytest.mark.parametrize("stop_ticks", [(0.999,), (1.01, 1.00)])
def test_mirror_stop_at_common_entry_waits_for_price_move(strategy, stop_ticks):
    _, bot, signals = strategy
    tick = _market_ticks(bot)

    async def run():
        await tick(0.65)
        await tick(1.00)
        short = bot.positions[SYMBOL]
        mirror = bot.long_positions[SYMBOL]
        assert short.avg_entry == mirror.avg_entry == 1.00
        await tick(1.00)
        assert bot.long_positions[SYMBOL] is mirror
        for price in stop_ticks[:-1]:
            await tick(price)
            assert bot.long_positions[SYMBOL] is mirror
        await tick(stop_ticks[-1])
        assert SYMBOL not in bot.long_positions
        assert bot.positions[SYMBOL] is short

    asyncio.run(run())

    assert [(s["action"], s["direction"]) for s in signals] == [("open", "LONG"), ("close", "LONG")]
    assert signals[-1]["reason"] == "SL"


@pytest.mark.parametrize("discovery", ["already_above", "crossed_in_cooldown", "above_at_unlock"])
def test_discovery_or_unlock_above_100_can_fill_short_but_not_long(strategy, discovery):
    _, bot, signals = strategy
    tick = _market_ticks(bot)

    async def run():
        if discovery != "already_above":
            bot.symbol_cooldown[SYMBOL] = time.time() + 60
            await tick(0.95)
            if discovery == "crossed_in_cooldown":
                await tick(1.00)
            assert SYMBOL not in bot.positions
            assert SYMBOL not in bot.long_positions
            bot.symbol_cooldown[SYMBOL] = time.time() - 1

        await tick(1.00)
        assert bot.positions[SYMBOL].opened_levels() == {50, 75, 100}
        await tick(1.25)
        assert bot.positions[SYMBOL].opened_levels() == {50, 75, 100, 150}
        assert SYMBOL not in bot.long_positions

    asyncio.run(run())

    assert not signals


def test_new_cross_observed_after_unlock_can_open_mirror(strategy):
    _, bot, signals = strategy
    tick = _market_ticks(bot)

    async def run():
        bot.symbol_cooldown[SYMBOL] = time.time() + 60
        await tick(1.00)
        assert SYMBOL not in bot.positions
        bot.symbol_cooldown[SYMBOL] = time.time() - 1
        await tick(0.95)
        await tick(1.00)
        assert bot.long_positions[SYMBOL].notional == pytest.approx(10)

    asyncio.run(run())

    assert [(s["level"], s["notional"]) for s in signals] == [(100, 10)]


def test_cross_while_kline_blocks_entries_is_not_reused(strategy, monkeypatch):
    _, bot, signals = strategy
    tick = _market_ticks(bot)
    entry_allowed = True
    monkeypatch.setattr(bot, "_kline_entry_ok", lambda symbol: entry_allowed)

    async def run():
        nonlocal entry_allowed
        await tick(0.95)
        entry_allowed = False
        await tick(1.00)
        assert bot.positions[SYMBOL].opened_levels() == {50, 75}
        entry_allowed = True
        await tick(1.01)
        assert bot.positions[SYMBOL].opened_levels() == {50, 75, 100}
        assert SYMBOL not in bot.long_positions

    asyncio.run(run())

    assert not signals


def test_only_100_and_150_send_long_fills(strategy):
    _, bot, signals = strategy

    async def enter():
        await _enter(bot, ENTRIES[:2])
        assert not signals
        assert SYMBOL not in bot.long_positions
        await _enter(bot, ENTRIES[2:])
        assert not await bot._ensure_short(SYMBOL, 150, 20, 1.10, 150)
        await _enter(bot, [(200, 40, 1.11)])

    asyncio.run(enter())

    short = bot.positions[SYMBOL]
    long = bot.long_positions[SYMBOL]
    assert short.notional == pytest.approx(80)
    assert long.notional == pytest.approx(30)
    assert long.phase == "mirror"
    assert long.trade_id != short.trade_id
    assert [fill.level for fill in long.fills] == [100, 150]
    assert [(s["action"], s["direction"], s["level"], s["notional"]) for s in signals] == [
        ("open", "LONG", 100, 10),
        ("open", "LONG", 150, 20),
    ]
    assert all(s["trade_id"] == long.trade_id for s in signals)


@pytest.mark.parametrize(("entry_count", "short_loss"), [(3, -8.0), (4, -8.0), (4, -8.25)])
def test_short_stop_closes_mirror_then_reopens_until_ten_more(strategy, entry_count, short_loss):
    _, bot, signals = strategy

    async def run():
        await _enter(bot, ENTRIES[:entry_count])
        short = bot.positions[SYMBOL]
        mirror = bot.long_positions[SYMBOL]
        stop_price = short.price_for_pnl(short_loss)
        await _tick(bot, stop_price)

        assert SYMBOL not in bot.positions
        continuation = bot.long_positions[SYMBOL]
        assert continuation.phase == "continuation"
        assert continuation.trade_id != mirror.trade_id
        assert continuation.notional == pytest.approx(mirror.notional)
        assert continuation.avg_entry == pytest.approx(stop_price)
        assert continuation.target_usd() == pytest.approx(10)
        assert continuation.price_for_pnl(continuation.sl_usd) == pytest.approx(short.avg_entry)

        await _tick(bot, continuation.price_for_pnl(9.9))
        assert bot.long_positions[SYMBOL] is continuation
        await _tick(bot, continuation.price_for_pnl(10.0))
        assert SYMBOL not in bot.long_positions
        return short, mirror, continuation

    short, mirror, continuation = asyncio.run(run())

    opens = [("open", mirror.trade_id)] * len(mirror.fills)
    assert [(s["action"], s["trade_id"]) for s in signals] == opens + [
        ("close", mirror.trade_id),
        ("open", continuation.trade_id),
        ("close", continuation.trade_id),
    ]
    assert all(s["direction"] == "LONG" for s in signals)
    assert signals[-3]["reason"] == signals[-1]["reason"] == "TP"
    assert signals[-2]["notional"] == pytest.approx(mirror.notional)

    rows = {r["trade_id"]: r for r in bot.closed_trades}
    assert rows[short.trade_id]["pnl"] == pytest.approx(short_loss)
    assert rows[mirror.trade_id]["pnl"] > 0
    assert rows[continuation.trade_id]["pnl"] == pytest.approx(10.0)


def test_default_mirror_stop_uses_short_average_without_closing_short(strategy):
    _, bot, signals = strategy

    async def run():
        await _enter(bot, ENTRIES[:3])
        short = bot.positions[SYMBOL]
        mirror = bot.long_positions[SYMBOL]
        stop_price = short.avg_entry
        assert mirror.price_for_pnl(mirror.sl_usd) == pytest.approx(stop_price)
        await _tick(bot, stop_price + 0.000001)
        assert bot.long_positions[SYMBOL] is mirror
        await _tick(bot, stop_price)
        assert SYMBOL not in bot.long_positions
        assert bot.positions[SYMBOL] is short
        await _enter(bot, ENTRIES[3:])
        await _tick(bot, short.price_for_pnl(-8.0))
        assert SYMBOL not in bot.positions
        assert SYMBOL not in bot.long_positions
        return mirror

    mirror = asyncio.run(run())

    assert [(s["action"], s["trade_id"]) for s in signals] == [
        ("open", mirror.trade_id), ("close", mirror.trade_id),
    ]
    assert signals[-1]["reason"] == "SL"


def _set_long_sl(app, bot, monkeypatch, reference):
    monkeypatch.setattr(app, "bot", bot)
    response = app.app.test_client().post("/api/set-long-sl", json={"reference": reference})
    assert response.status_code == 200
    assert response.get_json()["reference"] == reference


def test_configured_short_take_profit_closes_mirror_as_stop_loss(strategy, monkeypatch):
    app, bot, signals = strategy
    monkeypatch.setattr(app, "bot", bot)
    client = app.app.test_client()
    assert client.post("/api/set-long-sl", json={"reference": "invalid"}).status_code == 400

    async def run():
        await _enter(bot)
        short = bot.positions[SYMBOL]
        mirror = bot.long_positions[SYMBOL]
        assert bot.set_take_profit(SYMBOL, 2.0)
        short_tp_price = short.price_for_pnl(2.0)
        _set_long_sl(app, bot, monkeypatch, "short_tp")
        assert mirror.price_for_pnl(mirror.sl_usd) == pytest.approx(short_tp_price)
        await _tick(bot, short_tp_price + 0.000001)
        assert bot.long_positions[SYMBOL] is mirror
        await _tick(bot, short_tp_price - 0.000001)
        assert SYMBOL not in bot.positions
        assert SYMBOL not in bot.long_positions
        return short, mirror

    short, mirror = asyncio.run(run())

    assert len(signals) == 3
    assert signals[-1]["action"] == "close"
    assert signals[-1]["direction"] == "LONG"
    assert signals[-1]["trade_id"] == mirror.trade_id
    assert signals[-1]["reason"] == "SL"
    rows = {r["trade_id"]: r for r in bot.closed_trades}
    assert rows[short.trade_id]["reason"] == "TP"
    assert rows[mirror.trade_id]["reason"] == "SL"


def test_earlier_short_stop_does_not_start_the_continuation(strategy):
    _, bot, signals = strategy

    async def run():
        await _enter(bot)
        short = bot.positions[SYMBOL]
        assert bot.set_stop_loss(SYMBOL, -4.0)
        await _tick(bot, short.price_for_pnl(-4.01))

    asyncio.run(run())

    assert not bot.positions
    assert not bot.long_positions
    assert [s["action"] for s in signals] == ["open", "open", "close"]


@pytest.mark.parametrize("close_confirmed", [True, False])
def test_executor_requires_confirmation_to_send_reentry(strategy, monkeypatch, close_confirmed):
    app, bot, _ = strategy
    bot.executor = app.ExecutorBridge(executor_url="https://executor.invalid")

    async def run():
        calls = []
        original_opens_sent = asyncio.Event()
        close_started = asyncio.Event()
        allow_close_response = asyncio.Event()

        async def send_signal(payload):
            calls.append((payload["action"], payload["trade_id"]))
            if payload["action"] == "close":
                close_started.set()
                await allow_close_response.wait()
                return close_confirmed
            elif len(calls) == 2:
                original_opens_sent.set()
            return True

        monkeypatch.setattr(bot.executor, "send_signal_async", send_signal)
        await _enter(bot)
        await asyncio.wait_for(original_opens_sent.wait(), timeout=2)
        short = bot.positions[SYMBOL]
        mirror = bot.long_positions[SYMBOL]
        exit_task = asyncio.create_task(_tick(bot, short.price_for_pnl(-8.25)))
        try:
            await asyncio.wait_for(close_started.wait(), timeout=2)
            await asyncio.sleep(0)
            assert calls == [("open", mirror.trade_id), ("open", mirror.trade_id),
                             ("close", mirror.trade_id)]
        finally:
            allow_close_response.set()
        await exit_task
        await asyncio.gather(*list(bot.executor._pending_signals.values()))
        if close_confirmed:
            assert len(calls) == 4
            assert calls[-1] == ("open", bot.long_positions[SYMBOL].trade_id)
        else:
            assert len(calls) == 3
            assert SYMBOL not in bot.long_positions
            await _tick(bot, short.price_for_pnl(-25.0))
            await asyncio.gather(*list(bot.executor._pending_signals.values()))
            assert len(calls) == 3

    asyncio.run(run())


@pytest.mark.parametrize("reference", ["short_entry", "short_tp"])
def test_continuation_respects_stop_without_triggering_revenge(strategy, monkeypatch, reference):
    app, bot, signals = strategy

    async def run():
        await _enter(bot)
        short = bot.positions[SYMBOL]
        await _tick(bot, short.price_for_pnl(-8.25))
        continuation = bot.long_positions[SYMBOL]
        assert continuation.price_for_pnl(continuation.sl_usd) == pytest.approx(short.avg_entry)
        _set_long_sl(app, bot, monkeypatch, reference)
        short_tp_price = short.price_for_pnl(short.target_usd())
        stop_price = short.avg_entry if reference == "short_entry" else short_tp_price
        assert continuation.price_for_pnl(continuation.sl_usd) == pytest.approx(stop_price)
        monkeypatch.setattr(app, "REVENGE_ENABLED", True)
        await _tick(bot, stop_price + 0.000001)
        assert bot.long_positions[SYMBOL] is continuation
        await _tick(bot, stop_price)
        return continuation

    continuation = asyncio.run(run())

    assert SYMBOL not in bot.positions
    assert SYMBOL not in bot.long_positions
    assert len(signals) == 5
    assert signals[-1]["action"] == "close"
    assert signals[-1]["trade_id"] == continuation.trade_id
    assert signals[-1]["reason"] == "SL"


@pytest.mark.parametrize(("direction", "expected_side"), [("opposite", "LONG"), ("same", "SHORT")])
def test_revenge_keeps_its_executor_signal(strategy, monkeypatch, direction, expected_side):
    app, bot, signals = strategy
    monkeypatch.setattr(app, "PAPER_MODE", True)
    monkeypatch.setattr(app, "REVENGE_ENABLED", True)
    monkeypatch.setattr(app, "REVENGE_DIRECTION", direction)

    async def run():
        await _enter(bot)
        short = bot.positions[SYMBOL]
        await _tick(bot, short.price_for_pnl(-8.25))

    asyncio.run(run())

    revenge = bot.positions[SYMBOL]
    continuation = bot.long_positions[SYMBOL]
    assert revenge.revenge and revenge.side == expected_side
    assert revenge.trade_id != continuation.trade_id
    assert revenge.notional == pytest.approx(80)
    assert any(s["action"] == "open" and s["trade_id"] == revenge.trade_id
               and s["direction"] == expected_side for s in signals)
    status = bot._build_snapshot()
    panel = "long_positions" if expected_side == "LONG" else "positions"
    assert any(row["trade_id"] == revenge.trade_id for row in status[panel])

    asyncio.run(_tick(bot, revenge.price_for_pnl(revenge.target_usd() + 0.01)))
    assert SYMBOL not in bot.positions
    assert bot.long_positions[SYMBOL] is continuation
    assert signals[-1]["action"] == "close"
    assert signals[-1]["trade_id"] == revenge.trade_id
    assert signals[-1]["direction"] == expected_side


@pytest.mark.parametrize("legacy", [False, True])
def test_long_and_separate_pnl_survive_local_recovery(strategy, monkeypatch, legacy):
    app, bot, _ = strategy
    display_price = 1.10
    monkeypatch.setattr(bot, "_display_price", lambda symbol: display_price)

    async def run():
        nonlocal display_price
        await _enter(bot)
        short = bot.positions[SYMBOL]
        mirror = bot.long_positions[SYMBOL]
        status = bot._build_snapshot()
        assert status["positions"][0]["trade_id"] == short.trade_id
        assert status["long_positions"][0]["trade_id"] == mirror.trade_id
        assert status["short_unrealized"] == pytest.approx(short.unrealized_pnl(display_price))
        assert status["long_unrealized"] == pytest.approx(mirror.unrealized_pnl(display_price))
        display_price = short.price_for_pnl(-8.25)
        await _tick(bot, display_price)

    asyncio.run(run())

    original = bot.long_positions[SYMBOL]
    short_entry_price = original.price_for_pnl(original.sl_usd)
    _set_long_sl(app, bot, monkeypatch, "short_tp")
    short_tp_price = original.price_for_pnl(original.sl_usd)
    assert bot.store.close()
    if legacy:
        recovery_file = Path(app.RECOVERY_FILE)
        doc = json.loads(recovery_file.read_text(encoding="utf-8"))
        doc["settings"].pop("long_sl_reference")
        doc["long_positions"][SYMBOL].pop("parent_entry_price")
        doc["long_positions"][SYMBOL].pop("parent_tp_price")
        recovery_file.write_text(json.dumps(doc), encoding="utf-8")
    monkeypatch.setattr(app, "LONG_SL_REFERENCE", "short_entry")
    monkeypatch.setattr(app, "UI_SETTINGS", {})
    restored = app.TradingBot()
    restored_signals = []
    monkeypatch.setattr(restored.executor, "notify_async",
                        lambda payload, **kwargs: restored_signals.append(payload))
    monkeypatch.setattr(restored, "_display_price", lambda symbol: display_price)
    restored.store.start()
    try:
        assert restored._restore_from_store()
        continuation = restored.long_positions[SYMBOL]
        assert continuation.phase == "continuation"
        assert continuation.trade_id == original.trade_id
        assert continuation.notional == pytest.approx(original.notional)
        assert continuation.target_usd() == pytest.approx(10)
        assert app.LONG_SL_REFERENCE == ("short_entry" if legacy else "short_tp")
        stop_price = short_entry_price if legacy else short_tp_price
        assert continuation.price_for_pnl(continuation.sl_usd) == pytest.approx(stop_price)
        assert restored._open_position_symbols() == [SYMBOL]
        assert restored_signals == []
        Path(app.STATE_FILE).write_text(json.dumps({"scan_count": 1000}), encoding="utf-8")
        status = restored.snapshot()
        assert status["long_positions"][0]["trade_id"] == continuation.trade_id
        assert status["short_realized_pnl"] == pytest.approx(-8.25)
        assert status["long_realized_pnl"] == pytest.approx(bot._build_snapshot()["long_realized_pnl"])
        assert status["long_realized_pnl"] > 0
        live = restored.live_payload()
        assert live["long_positions"][str(continuation.trade_id)]["pnl"] == pytest.approx(0)

        display_price = continuation.price_for_pnl(10.0)
        monkeypatch.setattr(restored, "_price_for", lambda symbol: display_price)
        restored.price_cache = SimpleNamespace(ticker_cache={}, ensure_symbols=lambda symbols: None)

        async def tick_restored():
            restored._evaluate_symbol(SYMBOL)
            while restored._bg_tasks:
                await asyncio.gather(*list(restored._bg_tasks))

        asyncio.run(tick_restored())
    finally:
        restored.store.close()

    assert [(s["action"], s["trade_id"]) for s in restored_signals] == [
        ("close", continuation.trade_id),
    ]
