"""
=============================================================
  Bot de Sinais de Trading v8.0
  AUTO-SCANNER + AUTO-APRENDE + HELIUS + DEXSCREENER
=============================================================

NOVIDADES v7 (baseadas em análise de 11+ moedas reais):
  ✅ Cada moeda alertada UMA SÓ VEZ
  ✅ CA em bloco de código — copy-paste direto no Discord
  ✅ Monitoriza pump.fun E DexScreener em simultâneo
  ✅ Filtro de Market Cap ($100K–$500K = sweet spot)
  ✅ Filtro de Liquidity ($25K–$65K = ideal)
  ✅ Ratio Vol1H/Vol24H > 15% = momentum forte
  ✅ Atualização a cada 20 minutos após alerta
  ✅ Aprende sozinho 2h após cada alerta

FILTROS COMBINADOS (dados reais):
  Market Cap:  $100K–$500K
  Liquidity:   $25K–$65K
  Vol1H/24H:   >15%
  Categoria:   só ROCKET e BOM (+50% potencial)

INSTALAÇÃO:  pip install websockets aiohttp
COMO USAR:   python trading_bot_v7.py
⚠️  Não garante lucro. Investe só o que podes perder.
=============================================================
"""

import asyncio, csv, json, os, time, aiohttp, websockets
from datetime import datetime
from collections import deque

# ─────────────────────────────────────────────
# ⚙️  CONFIGURAÇÃO
# ─────────────────────────────────────────────

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "COLA_AQUI_O_TEU_WEBHOOK_URL")
HELIUS_API_KEY      = os.environ.get("HELIUS_API_KEY", "COLA_AQUI_A_TUA_HELIUS_KEY")

CONFIG = {
    "min_confidence":      55,
    "min_category":        ["ROCKET", "BOM"],
    "volume_spike_x":       3,
    "buy_sell_ratio_bull":  0.65,
    "window_trades":        20,
    "micropump_seconds":    120,
    "momentum_window":      300,
    "token_lifetime":      3600,
    "log_file":        "alertas.csv",
    "weights_file":    "pesos.json",
    "check_after":        7200,
    "success_threshold":   0.50,
    "update_interval":    1200,    # 20 minutos
    "dexscreener_poll":     30,    # polling DexScreener
    # Updates por milestone em vez de tempo fixo
    "milestones":     [0.25, 0.50, 1.00, 2.00, 5.00],  # +25%, +50%, +100%, +200%, +500%
    "drop_alert_pct": -0.20,   # avisa se cair -20% desde o pico

    # ── NOVOS FILTROS v7 (baseados nos prints do GEM HUNTERS) ──
    "mcap_min":        80_000,   # Market Cap mínimo $80K (Lupe tinha $96K → +114%)
    "mcap_max":       500_000,   # Market Cap máximo $500K
    "liq_min":         25_000,   # Liquidez mínima $25K
    "liq_max":         65_000,   # Liquidez máxima $65K
    "vol_ratio_min":    0.15,    # Vol1H/Vol24H mínimo 15%
}

HOT_WINDOW_START, HOT_WINDOW_END = 23, 3

# ─────────────────────────────────────────────
# 🧠  PESOS (APRENDIZAGEM)
# ─────────────────────────────────────────────

DEFAULT_WEIGHTS = {
    "hot_window":      20.0,
    "price_low":       15.0,
    "price_high_pen": -20.0,
    "buy_pressure":    25.0,
    "volume_spike":    25.0,
    "momentum":        20.0,
    "momentum_pen":   -25.0,
    "price_rise":      20.0,
    "price_fall_pen": -10.0,
    "trade_freq":      10.0,
    # Novos pesos v7
    "mcap_good":       20.0,    # market cap no sweet spot
    "mcap_bad":       -20.0,    # market cap fora do range
    "liq_good":        15.0,    # liquidez ideal
    "liq_bad":        -15.0,    # liquidez fora do range
    "vol_ratio_good":  20.0,    # ratio vol1h/24h forte
    # Novos pesos v8 — Helius (holders, dev, concentração)
    "holders_good":    20.0,    # >100 holders = mais seguro
    "holders_bad":    -20.0,    # <50 holders = risco rugpull
    "dev_sold":       -35.0,    # dev vendeu tudo = sinal muito negativo
    "dev_holds":       15.0,    # dev ainda tem tokens = confiança
    "concentration_ok": 15.0,  # top 10 holders < 30% = saudável
    "concentration_bad":-25.0, # top 10 holders > 50% = risco dump
    "buy_sell_5m":     25.0,    # ratio compras/vendas em 5min forte
}

def load_weights():
    if os.path.exists(CONFIG["weights_file"]):
        try:
            saved = json.load(open(CONFIG["weights_file"]))
            w = {**DEFAULT_WEIGHTS, **saved}
            print(f"[Aprendizagem] ✅ Pesos carregados de {CONFIG['weights_file']}")
            return w
        except Exception: pass
    print("[Aprendizagem] Usando pesos iniciais (baseados em 11+ moedas reais)")
    return dict(DEFAULT_WEIGHTS)

def save_weights(w):
    json.dump(w, open(CONFIG["weights_file"], "w"), indent=2)

def adjust_weights(weights, active_signals, success, change_pct=0):
    """
    Aprendizagem inteligente — ajusta pesos proporcionalmente ao resultado.
    
    Sucesso grande (>100%): reforça muito os sinais ativos
    Sucesso médio (50-100%): reforça moderadamente
    Falha ligeira (0-50%): penaliza ligeiramente
    Falha grande (<0%): penaliza muito os sinais ativos
    """
    if success:
        # Quanto mais subiu, mais aprende
        if change_pct >= 2.0:   factor = 0.15   # +200% → aprende muito
        elif change_pct >= 1.0: factor = 0.10   # +100% → aprende bem
        else:                   factor = 0.07   # +50%  → aprende normal
    else:
        # Quanto mais caiu, mais penaliza
        if change_pct <= -0.3:  factor = -0.12  # caiu >30% → penaliza muito
        elif change_pct <= 0:   factor = -0.07  # não subiu → penaliza normal
        else:                   factor = -0.04  # subiu mas <50% → penaliza pouco

    for sig in active_signals:
        if sig not in weights: continue
        cur   = weights[sig]
        delta = cur * factor
        weights[sig] = max(5.0, min(40.0, cur+delta)) if cur > 0 else max(-40.0, min(-5.0, cur+delta))
    return weights

WEIGHTS        = load_weights()
token_data     = {}
alerted_tokens = set()   # ← NUNCA repete o mesmo token

# ── BLACKLIST — moedas já conhecidas/que já explodiram ────────
# Estas moedas nunca serão alertadas (já atingiram o seu pico)
BLACKLIST = {
    # 11 moedas analisadas — já explodiram
    "7xKXNez8D22NwBssQbkzjy4s2ipFrzpmn5hfvWVe2aY5p",  # TOTO (exemplo)
    "AWpD39myXc7emw5M9cMCofkbo92x5FKWdGHWYpZFdoge",    # KIMCHI
    "HdvZF538TerjMWc1cmtWVKUaGkS8d77sGH5QaDCCpump",    # TST
    "Dt7rNPRfpmf4P8QvWQgyh5S7pnp4nQWYUmUwrrarwpump",   # BabyPippin
    "GkoMQAaSDCkNPJwKwoH6WoLPQYwpjK4mvVWSDS3Npump",    # JUICESTNKS
    "ETqhkFpa6oWK7mNexTNS9vKaRbZw3yzjTT14oKgpump",    # NOBODY
    "6VHXipL9pSV5MCYuB9tMR4xkwojhc1gQ51ku2GR7pump",   # MOMO
    "4MdtwK7ezBemvAWsW32HuvVD7o7j89Y3poYwJpWopump",   # MOG
    "bDz6NVehx5mr8E1e5d2c8ZAqjDe7DQWZeEKPpyvdogs",    # Dogs
    "EtL967sgMgeDDSUrNhtQrbhe4T92idzqf4VuA6H7uGfQ",   # MAYA
    "6jfRbgs3B1KrhohSZ7KbhJckHZektQs1rRUSwWeZpump",   # TOTO
    "8QH9scze2hPJjbVmQEnVuV1r59DDcUvQSRQ9ybagpump",  # Lupe (+114%)
    # Adiciona aqui mais moedas que já explodiram
}
tokens_seen    = 0
alerts_sent    = 0
learns_done    = 0
pending_checks = {}

# ─────────────────────────────────────────────
# 📊  TOKEN DATA
# ─────────────────────────────────────────────

def init_token(mint):
    if mint not in token_data:
        token_data[mint] = {
            "mint": mint, "name": "Unknown", "symbol": "???",
            "trades":           deque(maxlen=CONFIG["window_trades"]),
            "prices":           deque(maxlen=100),
            "volumes":          deque(maxlen=100),
            "volume_timeline":  [],
            "alert_price":      None,
            "buy_count":        0,
            "sell_count":       0,
            "first_seen":       time.time(),
            "first_trade_time": None,
            "peak_price":       0,
            "source":           "unknown",
            # Campos Helius v8
            "holders":          0,
            "top10_pct":        0,
            "dev_still_holds":  None,
            "dev_pct":          0,
            # Novos campos v7
            "market_cap":       0,
            "liquidity":        0,
            "vol_1h":           0,
            "vol_24h":          0,
        }

def cleanup_old_tokens():
    now   = time.time()
    todel = [m for m, d in token_data.items()
             if now - d["first_seen"] > CONFIG["token_lifetime"]
             and m not in pending_checks]
    for m in todel:
        del token_data[m]

def is_hot_window():
    h = datetime.now().hour
    return h >= HOT_WINDOW_START or h < HOT_WINDOW_END

def check_micropump(mint):
    d = token_data[mint]; prices = list(d["prices"])
    if len(prices) < 3 or not d["first_trade_time"]: return False
    if time.time() - d["first_trade_time"] < CONFIG["micropump_seconds"] and len(prices) >= 5:
        peak = max(prices)
        return peak > 0 and prices[-1] < peak * 0.85
    return False

# ─────────────────────────────────────────────
# 🔍  FILTROS DE MARKET CAP / LIQUIDITY / VOL
# ─────────────────────────────────────────────

def check_mcap_liq_vol(mint) -> tuple:
    """
    Verifica Market Cap, Liquidity e ratio Vol1H/24H.
    Retorna (passou_filtro, pontos, sinais, active_signals)
    Baseado nos padrões do GEM HUNTERS AI:
      Sweet spot mcap: $100K–$500K
      Sweet spot liq:  $25K–$65K
      Vol ratio:       >15%
    """
    d       = token_data[mint]
    w       = WEIGHTS
    mcap    = d.get("market_cap", 0)
    liq     = d.get("liquidity", 0)
    vol_1h  = d.get("vol_1h", 0)
    vol_24h = d.get("vol_24h", 0)

    score   = 0
    signals = []
    active  = []
    passed  = True  # se falhar filtro duro, bloqueia o alerta

    # ── MARKET CAP ──────────────────────────────
    if mcap > 0:
        if CONFIG["mcap_min"] <= mcap <= CONFIG["mcap_max"]:
            pts = int(w["mcap_good"]); score += pts
            signals.append(f"💎 Market Cap ${mcap/1000:.0f}K — sweet spot (+{pts}pts)")
            active.append("mcap_good")
        elif mcap > CONFIG["mcap_max"]:
            score += int(w["mcap_bad"])
            signals.append(f"🔴 Market Cap ${mcap/1000:.0f}K — muito alto ({w['mcap_bad']:.0f}pts)")
            active.append("mcap_bad")
            if mcap > 800_000:
                passed = False  # bloqueia completamente acima de $800K
        else:
            signals.append(f"⚠️ Market Cap ${mcap/1000:.0f}K — muito baixo (cuidado)")

    # ── LIQUIDITY ───────────────────────────────
    if liq > 0:
        if CONFIG["liq_min"] <= liq <= CONFIG["liq_max"]:
            pts = int(w["liq_good"]); score += pts
            signals.append(f"💧 Liquidez ${liq/1000:.0f}K — ideal (+{pts}pts)")
            active.append("liq_good")
        elif liq > CONFIG["liq_max"]:
            score += int(w["liq_bad"])
            signals.append(f"💧 Liquidez ${liq/1000:.0f}K — alta, já comprado ({w['liq_bad']:.0f}pts)")
            active.append("liq_bad")
        else:
            score += int(w["liq_bad"])
            signals.append(f"💧 Liquidez ${liq/1000:.0f}K — baixa, risco alto ({w['liq_bad']:.0f}pts)")
            active.append("liq_bad")

    # ── RATIO VOL 1H / 24H ──────────────────────
    if vol_24h > 0 and vol_1h > 0:
        ratio = vol_1h / vol_24h
        if ratio >= CONFIG["vol_ratio_min"]:
            pts = int(w["vol_ratio_good"]); score += pts
            signals.append(f"📊 Vol1H/24H: {ratio*100:.0f}% — momentum forte (+{pts}pts)")
            active.append("vol_ratio_good")
        else:
            signals.append(f"📊 Vol1H/24H: {ratio*100:.0f}% — momentum fraco")

    return passed, score, signals, active

# ─────────────────────────────────────────────
# 🔢  MOTOR DE CONFIANÇA
# ─────────────────────────────────────────────

def calculate_confidence(mint):
    d = token_data.get(mint)
    if not d or len(d["trades"]) < 3:
        return {"score": 0, "signals": [], "verdict": "SEM DADOS", "category": None,
                "active_signals": [], "blocked": False}
    if check_micropump(mint):
        return {"score": 0, "signals": ["⚡ MICRO-PUMP"], "verdict": "⚡ MICRO-PUMP",
                "category": "MICROPUMP", "active_signals": [], "blocked": False}

    signals, active, score = [], [], 0
    w       = WEIGHTS
    prices  = list(d["prices"])
    volumes = list(d["volumes"])
    trades  = list(d["trades"])
    in_hot  = is_hot_window()

    # 1. Janela horária
    if in_hot:
        pts = int(w["hot_window"]); score += pts
        signals.append(f"🕐 Janela 23h–03h (+{pts}pts)"); active.append("hot_window")
    else:
        signals.append("🕑 Fora da janela 23h–03h")

    # 2. Preço
    ap = d.get("alert_price") or (prices[-1] if prices else 0)
    if ap > 0:
        if ap < 0.00030:
            pts = int(w["price_low"]); score += pts
            signals.append(f"💚 Preço baixo ${ap:.7f} (+{pts}pts)"); active.append("price_low")
        elif ap < 0.00050:
            signals.append(f"🟡 Preço médio ${ap:.7f}")
        elif not in_hot:
            score += int(w["price_high_pen"])
            signals.append(f"🔴 Preço alto ${ap:.7f} ({w['price_high_pen']:.0f}pts)"); active.append("price_high_pen")
        else:
            signals.append(f"🟠 Preço alto ${ap:.7f} na janela (neutro)")

    # 3. Market Cap + Liquidity + Vol ratio (NOVOS FILTROS v7)
    passed, mcap_score, mcap_signals, mcap_active = check_mcap_liq_vol(mint)
    if not passed:
        return {"score": 0, "signals": [f"🚫 Bloqueado — Market Cap demasiado alto"],
                "verdict": "🚫 BLOQUEADO", "category": "FRACO",
                "active_signals": [], "blocked": True}
    score += mcap_score
    signals.extend(mcap_signals)
    active.extend(mcap_active)

    # 4. Pressão de compra
    total = d["buy_count"] + d["sell_count"]
    if total > 0:
        br = d["buy_count"] / total
        if br >= 0.65:
            pts = int(br * w["buy_pressure"]); score += pts
            signals.append(f"📈 Compra: {br*100:.0f}% buys (+{pts}pts)"); active.append("buy_pressure")
        elif br < 0.35:
            score -= 15; signals.append(f"📉 Venda: {br*100:.0f}% buys (-15pts)")

    # 5. Spike de volume
    if len(volumes) >= 4:
        avg = sum(list(volumes)[:-2]) / max(len(volumes)-2, 1); lat = volumes[-1]
        if avg > 0 and lat > avg * CONFIG["volume_spike_x"]:
            pts = min(int(w["volume_spike"]), int((lat/avg)*7)); score += pts
            signals.append(f"🔥 Volume spike: {lat/avg:.1f}x (+{pts}pts)"); active.append("volume_spike")

    # 6. Momentum
    now = time.time(); tl = d["volume_timeline"]; mw = CONFIG["momentum_window"]
    rec = [v for t, v in tl if now-t <= mw]; old = [v for t, v in tl if mw < now-t <= mw*2]
    if len(rec) >= 2 and len(old) >= 1:
        ar, ao = sum(rec)/len(rec), sum(old)/len(old)
        if ao > 0:
            ratio = ar / ao
            if ratio >= 1.5:
                pts = min(int(w["momentum"]), int(ratio*8)); score += pts
                signals.append(f"📊 Momentum: {ratio:.1f}x (+{pts}pts)"); active.append("momentum")
            elif ratio < 0.6:
                score += int(w["momentum_pen"])
                signals.append(f"📉 Momentum a cair ({w['momentum_pen']:.0f}pts)"); active.append("momentum_pen")

    # 7. Subida de preço
    if len(prices) >= 4 and prices[-4] > 0:
        pct = (prices[-1] - prices[-4]) / prices[-4]
        if pct >= 0.05:
            pts = min(int(w["price_rise"]), int(pct*150)); score += pts
            signals.append(f"🚀 Preço +{pct*100:.1f}% (+{pts}pts)"); active.append("price_rise")
        elif pct <= -0.05:
            score += int(w["price_fall_pen"])
            signals.append(f"⚠️ Preço {pct*100:.1f}% ({w['price_fall_pen']:.0f}pts)"); active.append("price_fall_pen")

    # 8. Frequência de trades
    rt = [t["time"] for t in trades[-10:]]
    if len(rt) >= 2:
        dur = rt[-1] - rt[0]
        if dur > 0:
            tps = len(rt) / dur
            if tps > 0.5:
                pts = min(int(w["trade_freq"]), int(tps*8)); score += pts
                signals.append(f"⚡ Freq: {tps:.2f}/s (+{pts}pts)"); active.append("trade_freq")

    # 8b. Helius — holders, dev, concentração
    holders   = d.get("holders", 0)
    top10_pct = d.get("top10_pct", 0)
    dev_holds = d.get("dev_still_holds", None)

    if holders > 0:
        if holders >= 100:
            pts = int(w["holders_good"]); score += pts
            signals.append(f"👥 Holders: {holders} — seguro (+{pts}pts)"); active.append("holders_good")
        elif holders < 50:
            score += int(w["holders_bad"])
            signals.append(f"👥 Holders: {holders} — risco rugpull ({w['holders_bad']:.0f}pts)"); active.append("holders_bad")

    if top10_pct > 0:
        if top10_pct <= 30:
            pts = int(w["concentration_ok"]); score += pts
            signals.append(f"📊 Top10: {top10_pct:.0f}% — concentração saudável (+{pts}pts)"); active.append("concentration_ok")
        elif top10_pct > 50:
            score += int(w["concentration_bad"])
            signals.append(f"📊 Top10: {top10_pct:.0f}% — muito concentrado ({w['concentration_bad']:.0f}pts)"); active.append("concentration_bad")

    if dev_holds is not None:
        if dev_holds:
            pts = int(w["dev_holds"]); score += pts
            signals.append(f"👨‍💻 Dev ainda tem tokens (+{pts}pts)"); active.append("dev_holds")
        else:
            score += int(w["dev_sold"])
            signals.append(f"👨‍💻 Dev já vendeu ({w['dev_sold']:.0f}pts) ⚠️"); active.append("dev_sold")

    score = max(0, min(100, score))
    if   score >= 70 and in_hot: cat, v = "ROCKET", "🟢 ROCKET — alto potencial"
    elif score >= 55:             cat, v = "BOM",    "🟡 BOM SINAL — potencial moderado"
    elif score >= 35:             cat, v = "FRACO",  "🟠 FRACO — ignorado"
    else:                         cat, v = "FRACO",  "🔴 SEM SINAL"
    return {"score": score, "signals": signals, "verdict": v, "category": cat,
            "active_signals": active, "blocked": False}

# ─────────────────────────────────────────────
# 🌐  DEXSCREENER — buscar market cap, liq, vol
# ─────────────────────────────────────────────

async def fetch_dexscreener(mint):
    """Busca dados de Market Cap, Liquidity e Volume do DexScreener."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status != 200: return None
                data  = await r.json()
                pairs = data.get("pairs") or []
                if not pairs: return None
                p = pairs[0]  # primeiro par (mais líquido)
                return {
                    "price":      float(p.get("priceUsd") or 0),
                    "market_cap": float(p.get("fdv") or p.get("marketCap") or 0),
                    "liquidity":  float((p.get("liquidity") or {}).get("usd") or 0),
                    "vol_1h":     float((p.get("volume") or {}).get("h1") or 0),
                    "vol_24h":    float((p.get("volume") or {}).get("h24") or 0),
                    "name":       p.get("baseToken", {}).get("name", ""),
                    "symbol":     p.get("baseToken", {}).get("symbol", ""),
                }
    except Exception:
        return None

async def enrich_token_from_dex(mint):
    """Atualiza o token com dados do DexScreener."""
    data = await fetch_dexscreener(mint)
    if not data: return
    d = token_data.get(mint)
    if not d: return
    if data["price"] > 0:     d["prices"].append(data["price"])
    if data["market_cap"] > 0: d["market_cap"] = data["market_cap"]
    if data["liquidity"] > 0:  d["liquidity"]  = data["liquidity"]
    if data["vol_1h"] > 0:     d["vol_1h"]      = data["vol_1h"]
    if data["vol_24h"] > 0:    d["vol_24h"]     = data["vol_24h"]
    if data["name"]:           d["name"]        = data["name"]
    if data["symbol"]:         d["symbol"]      = data["symbol"]

# ─────────────────────────────────────────────
# 🔬  HELIUS — holders, dev wallet, concentração
# ─────────────────────────────────────────────

async def fetch_helius_data(mint):
    """
    Busca dados avançados via Helius API:
    - Número de holders
    - Se o dev ainda tem tokens
    - Concentração dos top holders
    Usado para afinar os pesos automaticamente — não mostrado ao utilizador.
    """
    if "COLA" in HELIUS_API_KEY: return None
    try:
        async with aiohttp.ClientSession() as s:
            # Busca top holders
            url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getTokenAccounts",
                "params": {
                    "mint": mint,
                    "limit": 20,
                    "options": {"showZeroBalance": False}
                }
            }
            async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return None
                data    = await r.json()
                accounts = data.get("result", {}).get("token_accounts", [])
                if not accounts: return None

                total_holders = len(accounts)
                # Calcula concentração dos top 10
                amounts  = sorted([float(a.get("amount", 0)) for a in accounts], reverse=True)
                total_supply = sum(amounts) or 1
                top10_pct    = sum(amounts[:10]) / total_supply * 100

                # Verifica se o dev (primeiro criador) ainda tem tokens
                # O dev normalmente é o maior holder inicial
                dev_amount = amounts[0] if amounts else 0
                dev_pct    = dev_amount / total_supply * 100
                # Se o maior holder tem < 1% provavelmente o dev já vendeu
                dev_still_holds = dev_pct >= 2.0

                return {
                    "holders":        total_holders,
                    "top10_pct":      top10_pct,
                    "dev_still_holds": dev_still_holds,
                    "dev_pct":        dev_pct,
                }
    except Exception as e:
        print(f"[Helius] ⚠️ {e}")
        return None

async def enrich_token_helius(mint):
    """Enriquece token com dados Helius para afinação automática."""
    data = await fetch_helius_data(mint)
    if not data: return
    d = token_data.get(mint)
    if not d: return
    d["holders"]         = data["holders"]
    d["top10_pct"]       = data["top10_pct"]
    d["dev_still_holds"] = data["dev_still_holds"]
    d["dev_pct"]         = data["dev_pct"]
    print(f"[Helius] {d.get('name','?')} — {data['holders']} holders | top10: {data['top10_pct']:.0f}% | dev: {'✅' if data['dev_still_holds'] else '❌ vendeu'}")

# ─────────────────────────────────────────────
# 🧠  APRENDIZAGEM (2h depois)
# ─────────────────────────────────────────────

async def check_and_learn(mint, check_data):
    """
    Verifica resultado 2h após alerta e aprende automaticamente.
    Analisa PORQUÊ falhou ou teve sucesso e ajusta os pesos certos.
    """
    global WEIGHTS, learns_done
    alert_price    = check_data["alert_price"]
    name           = check_data["name"]
    active         = check_data["active_signals"]
    alert_mcap     = check_data.get("alert_mcap", 0)
    alert_liq      = check_data.get("alert_liq", 0)
    alert_vol_rat  = check_data.get("alert_vol_ratio", 0)
    alert_in_hot   = check_data.get("alert_in_hot", False)

    # ── 1. BUSCA PREÇO ATUAL ─────────────────────────────────
    current_price = None
    dex = await fetch_dexscreener(mint)
    if dex and dex["price"] > 0:
        current_price = dex["price"]
        # Atualiza métricas atuais para análise
        current_mcap    = dex.get("market_cap", 0)
        current_liq     = dex.get("liquidity", 0)
    elif mint in token_data:
        prices = list(token_data[mint]["prices"])
        if prices: current_price = prices[-1]
        current_mcap = current_liq = 0

    if not current_price or alert_price <= 0:
        print(f"[Aprendizagem] ⚠️ {name} — sem preço para verificar"); return

    # ── 2. CALCULA RESULTADO ─────────────────────────────────
    change     = (current_price - alert_price) / alert_price
    success    = change >= CONFIG["success_threshold"]
    result_str = f"+{change*100:.1f}%" if change >= 0 else f"{change*100:.1f}%"

    # ── 3. DIAGNÓSTICO — porquê falhou? ─────────────────────
    diagnosis = []
    if not success:
        # Analisa cada fator para perceber o que correu mal
        if alert_mcap > 400_000:
            diagnosis.append(("mcap_good", "Market Cap alto demais para o resultado"))
        if alert_liq > 55_000:
            diagnosis.append(("liq_good", "Liquidez já muito alta → já comprado"))
        if alert_vol_rat < 0.10:
            diagnosis.append(("vol_ratio_good", "Vol1H/24H baixo → momentum fraco"))
        if not alert_in_hot:
            diagnosis.append(("hot_window", "Fora da janela 23h–03h"))
        # Diagnóstico Helius
        alert_holders  = check_data.get("alert_holders", 0)
        alert_top10    = check_data.get("alert_top10_pct", 0)
        alert_dev      = check_data.get("alert_dev_holds", None)
        if alert_holders > 0 and alert_holders < 50:
            diagnosis.append(("holders_bad", f"Poucos holders ({alert_holders}) → fácil manipular"))
        if alert_top10 > 50:
            diagnosis.append(("concentration_bad", f"Top10 muito concentrado ({alert_top10:.0f}%) → risco dump"))
        if alert_dev is False:
            diagnosis.append(("dev_sold", "Dev já tinha vendido → sinal negativo confirmado"))
        if change < -0.1:
            # Caiu mesmo → todos os sinais ativos são suspeitos
            for sig in active:
                if not any(sig == d[0] for d in diagnosis):
                    diagnosis.append((sig, "Sinal não preveniu a queda"))

    # ── 4. AJUSTA PESOS ─────────────────────────────────────
    old_w   = dict(WEIGHTS)
    # Penaliza os sinais do diagnóstico com força extra
    diag_sigs = [d[0] for d in diagnosis] if not success else []
    all_sigs  = list(set(active + diag_sigs))
    WEIGHTS   = adjust_weights(WEIGHTS, all_sigs, success, change)
    save_weights(WEIGHTS)
    learns_done += 1

    # ── 5. LOG ──────────────────────────────────────────────
    icon = "✅" if success else "❌"
    print(f"\n{'═'*55}")
    print(f"  {icon} [Aprendizagem #{learns_done}] {name} — {result_str}")
    print(f"  Preço alerta: ${alert_price:.8f} → Agora: ${current_price:.8f}")
    if diagnosis:
        print(f"  🔍 Diagnóstico:")
        for sig, reason in diagnosis:
            print(f"     • {reason}")
    changed = {k: f"{old_w[k]:.1f}→{WEIGHTS[k]:.1f}" for k in WEIGHTS if abs(WEIGHTS[k]-old_w[k]) > 0.1}
    if changed:
        print(f"  ⚖️  Pesos ajustados: {changed}")
    print(f"{'═'*55}\n")

    # ── 6. LOG CSV ───────────────────────────────────────────
    with open(CONFIG["log_file"], "a", newline="") as f:
        import csv as _csv
        w = _csv.writer(f)
        w.writerow([time.time(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    mint, name, "VERIFICACAO_2H",
                    alert_price, current_price, result_str,
                    "SUCESSO" if success else "FALHOU",
                    str([d[1] for d in diagnosis])])

# ─────────────────────────────────────────────
# 🔔  DISCORD — ALERTA (formato inspirado no GEM HUNTERS)
# ─────────────────────────────────────────────

async def send_discord_alert(mint, analysis, price, source="pump.fun"):
    global alerts_sent
    if "COLA" in DISCORD_WEBHOOK_URL: return

    d       = token_data[mint]
    cat     = analysis.get("category", "?")
    color   = {"ROCKET": 0x00ff88, "BOM": 0xffd447}.get(cat, 0x888888)
    icon    = {"ROCKET": "🚀", "BOM": "✅"}.get(cat, "🔔")
    mcap    = d.get("market_cap", 0)
    liq     = d.get("liquidity", 0)
    vol_1h  = d.get("vol_1h", 0)
    vol_24h = d.get("vol_24h", 0)
    ratio   = f"{vol_1h/vol_24h*100:.0f}%" if vol_24h > 0 else "N/A"



    embed = {
        "title":       f"{icon} {cat} — {d.get('name','?')} | {d.get('symbol','?')}",
        "description": f"{analysis['verdict']}\n🔵 Fonte: {source}",
        "color":       color,
        "fields": [
            # Linha 1: métricas principais
            {"name": "💲 Preço",      "value": f"`${price:.8f}`",                              "inline": True},
            {"name": "📊 Score",      "value": f"**{analysis['score']}%**",                    "inline": True},
            {"name": "🕐 Janela",     "value": "🟢 23h–03h" if is_hot_window() else "⚪ fora", "inline": True},
            # Linha 2: market cap, liq, vol
            {"name": "💎 Market Cap", "value": f"${mcap/1000:.0f}K" if mcap else "N/A",        "inline": True},
            {"name": "💧 Liquidez",   "value": f"${liq/1000:.0f}K" if liq else "N/A",          "inline": True},
            {"name": "📈 Vol1H/24H",  "value": ratio,                                          "inline": True},
            # Linha 3: buys/sells, hora, aprendizagens
            {"name": "🛒 Buys/Sells", "value": f"{d['buy_count']} / {d['sell_count']}",        "inline": True},
            {"name": "⏱️ Hora",       "value": datetime.now().strftime("%H:%M:%S"),            "inline": True},
            {"name": "🧠 Aprend.",    "value": str(learns_done),                               "inline": True},
            # Sinais
            {"name": "🔍 Sinais",     "value": "\n".join(analysis["signals"])[:1000],         "inline": False},
            # CA limpo — copia e cola diretamente nas plataformas
            {"name": "📋 CA",
             "value": mint,
             "inline": False},
        ],
        "footer":    {"text": f"Trading Bot v8.0 • Alerta #{alerts_sent+1} • Verifica resultado em 2h"},
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]},
                             timeout=aiohttp.ClientTimeout(total=5))
            if r.status in (200, 204):
                alerts_sent += 1
                print(f"[Discord] ✅ #{alerts_sent} — {d.get('name','?')} score:{analysis['score']}% mcap:${mcap/1000:.0f}K via {source}")
    except Exception as e:
        print(f"[Discord] ❌ {e}")

# ─────────────────────────────────────────────
# 📊  DISCORD — ATUALIZAÇÃO 20 MINUTOS
# ─────────────────────────────────────────────

def calc_sell_probability(mint, dex, pct):
    """
    Calcula probabilidade de continuar a subir ou corrigir.
    Baseado em sinais técnicos reais — não é garantia, é orientação.
    """
    d = token_data.get(mint, {})
    warnings  = []
    positives = []
    risk_score = 0  # quanto maior, maior risco de correção

    # 1. Volume a cair?
    vols = list(d.get("volumes", []))
    if len(vols) >= 6:
        recent_vol = sum(vols[-3:]) / 3
        older_vol  = sum(vols[-6:-3]) / 3
        if older_vol > 0:
            vol_trend = recent_vol / older_vol
            if vol_trend < 0.6:
                risk_score += 25
                warnings.append("📉 Volume a cair nas últimas atualizações")
            elif vol_trend > 1.3:
                risk_score -= 15
                positives.append("📈 Volume a crescer — momentum continua")

    # 2. Ratio compras/vendas atual
    buys  = d.get("buy_count", 0)
    sells = d.get("sell_count", 0)
    total = buys + sells
    if total > 0:
        ratio = buys / total
        if ratio < 0.45:
            risk_score += 30
            warnings.append(f"🔴 Vendas dominam: {ratio*100:.0f}% compras")
        elif ratio < 0.55:
            risk_score += 10
            warnings.append(f"⚠️ Pressão de venda a aumentar: {ratio*100:.0f}% compras")
        else:
            risk_score -= 10
            positives.append(f"✅ Compras ainda dominam: {ratio*100:.0f}%")

    # 3. Dev vendeu?
    if d.get("dev_still_holds") is False:
        risk_score += 30
        warnings.append("⚠️ Dev já vendeu os tokens")
    elif d.get("dev_still_holds") is True:
        risk_score -= 10
        positives.append("✅ Dev ainda tem tokens")

    # 4. Concentração de holders
    top10 = d.get("top10_pct", 0)
    if top10 > 50:
        risk_score += 25
        warnings.append(f"⚠️ Top 10 holders têm {top10:.0f}% — risco dump")
    elif top10 > 0 and top10 <= 30:
        risk_score -= 10
        positives.append(f"✅ Distribuição saudável ({top10:.0f}%)")

    # 5. Já subiu muito? Quanto mais subiu, maior risco de correção
    if pct >= 200:
        risk_score += 30
        warnings.append("⚠️ Já subiu muito (+200%) — possível correção")
    elif pct >= 100:
        risk_score += 15
        warnings.append("⚠️ Subida grande (+100%) — considera realizar lucro")
    elif pct >= 50:
        risk_score += 5

    # 6. DexScreener — liquidez atual vs alerta
    if dex:
        curr_liq = dex.get("liquidity", 0)
        if curr_liq > 0 and curr_liq < 15000:
            risk_score += 20
            warnings.append("⚠️ Liquidez muito baixa — difícil vender")

    # Calcula probabilidade final
    risk_score = max(0, min(100, risk_score))
    prob_subir  = max(5,  min(95, 100 - risk_score))
    prob_corrig = max(5,  min(95, risk_score))

    return prob_subir, prob_corrig, warnings, positives

async def send_discord_movement(mint, alert_data, current_price, pct, direction, milestone, dex=None):
    """Envia alerta apenas quando há movimento significativo de preço."""
    if "COLA" in DISCORD_WEBHOOK_URL: return

    alert_price = alert_data["alert_price"]
    name        = alert_data.get("name", "?")
    elapsed     = int((time.time() - alert_data["alert_time"]) / 60)

    if direction == "up":
        if   milestone >= 200: title = f"🚀🚀🚀 {name} — +{milestone}% ATINGIDO!"
        elif milestone >= 100: title = f"🚀🚀 {name} — +{milestone}% ATINGIDO!"
        elif milestone >= 50:  title = f"🚀 {name} — +{milestone}% ATINGIDO!"
        else:                  title = f"📈 {name} — +{milestone}%"
        color = 0x00ff88
    else:
        if   milestone >= 40: title = f"🔴🔴 {name} — QUEDA -{milestone}%! CONSIDERA SAIR"
        else:                 title = f"🔴 {name} — caiu -{milestone}%"
        color = 0xff3355

    # Calcula probabilidade
    prob_up, prob_down, warnings, positives = calc_sell_probability(mint, dex, pct)

    if prob_down >= 70:   rec = "🔴 CONSIDERA VENDER AGORA"
    elif prob_down >= 50: rec = "🟡 CUIDADO — risco elevado"
    else:                 rec = "🟢 AGUENTA — sinais positivos"

    analysis_lines = []
    if warnings:
        analysis_lines.append("**⚠️ Sinais de alerta:**")
        analysis_lines.extend([f"• {w}" for w in warnings])
    if positives:
        analysis_lines.append("**✅ Sinais positivos:**")
        analysis_lines.extend([f"• {p}" for p in positives])
    if not warnings and not positives:
        analysis_lines.append("• Sem sinais claros — mantém atenção")
    analysis_text = "\n".join(analysis_lines)[:1000]

    dex_url = f"https://dexscreener.com/solana/{mint}"

    embed = {
        "title":       title,
        "color":       color,
        "fields": [
            {"name": "💲 Preço alerta",    "value": f"`${alert_price:.8f}`",   "inline": True},
            {"name": "💲 Preço atual",     "value": f"`${current_price:.8f}`", "inline": True},
            {"name": "📊 Variação total",  "value": f"**{pct:+.1f}%**",        "inline": True},
            {"name": "⏱️ Desde alerta",    "value": f"{elapsed} min",          "inline": True},
            {"name": "🟢 Prob. continuar", "value": f"**{prob_up}%**",         "inline": True},
            {"name": "🔴 Prob. corrigir",  "value": f"**{prob_down}%**",       "inline": True},
            {"name": rec,                  "value": analysis_text,              "inline": False},
            {"name": "📋 CA",              "value": f"`{mint}`",               "inline": False},
            {"name": "🔗 Chart",           "value": f"[Abre no DexScreener]({dex_url})", "inline": False},
        ],
        "footer":    {"text": f"Trading Bot v8.0 • {trigger_label} • probabilidades são orientação"},
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        async with aiohttp.ClientSession() as s:
            await s.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]},
                         timeout=aiohttp.ClientTimeout(total=5))
            print(f"[Movimento] {name}: {pct:+.1f}% (milestone {direction} {milestone}%) | prob corrigir: {prob_down}%")
    except Exception as e:
        print(f"[Movimento] ❌ {e}")

# ─────────────────────────────────────────────
# 💾  LOG / TERMINAL
# ─────────────────────────────────────────────

def log_alert(mint, analysis, price, source):
    exists = os.path.exists(CONFIG["log_file"])
    with open(CONFIG["log_file"], "a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["timestamp","hora","mint","nome","preco","mcap","liq","vol1h","vol24h",
                        "score","categoria","janela","fonte","sinais"])
        d = token_data[mint]
        w.writerow([time.time(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    mint, d.get("name","?"), price,
                    d.get("market_cap",0), d.get("liquidity",0),
                    d.get("vol_1h",0), d.get("vol_24h",0),
                    analysis["score"], analysis.get("category","?"),
                    is_hot_window(), source, " | ".join(analysis["signals"])])

def print_alert(mint, analysis, price, source):
    d    = token_data[mint]
    cat  = analysis.get("category", "?")
    icon = {"ROCKET": "🚀", "BOM": "✅"}.get(cat, "🔔")
    mcap = d.get("market_cap", 0)
    liq  = d.get("liquidity", 0)
    print("\n" + "═"*60)
    print(f"  {icon} [{datetime.now().strftime('%H:%M:%S')}] {cat} via {source}")
    print(f"  {d.get('name','?')} ({d.get('symbol','?')})")
    print(f"  Score: {analysis['score']}%  |  Preço: ${price:.8f}")
    print(f"  MCap: ${mcap/1000:.0f}K  |  Liq: ${liq/1000:.0f}K")
    print(f"  {analysis['verdict']}")
    print("─"*60)
    for s in analysis["signals"]: print(f"  {s}")
    print("─"*60)
    print(f"  CA: {mint}")
    print(f"  Alertas: {alerts_sent} | Aprendizagens: {learns_done}")
    print("═"*60)

# ─────────────────────────────────────────────
# 🔄  PROCESSAR TRADE
# ─────────────────────────────────────────────

async def process_trade(msg, source="pump.fun"):
    global tokens_seen
    mint = msg.get("mint") or msg.get("token")
    if not mint: return

    # ── BLACKLIST — moeda conhecida/já explodiu → ignora sempre ──
    if mint in BLACKLIST:
        return

    # ── NUNCA REPETE — se já foi alertado, ignora ──
    if mint in alerted_tokens: return

    is_new = mint not in token_data
    init_token(mint); d = token_data[mint]
    d["source"] = source

    if is_new:
        tokens_seen += 1
        print(f"  [novo #{tokens_seen}] {msg.get('name','?')} via {source}")
        # Busca dados do DexScreener para ter mcap/liq/vol desde o início
        asyncio.create_task(enrich_token_from_dex(mint))
        # Busca dados Helius para holders/dev/concentração
        asyncio.create_task(enrich_token_helius(mint))

    if "name"   in msg: d["name"]   = msg["name"]
    if "symbol" in msg: d["symbol"] = msg["symbol"]

    trade_type = str(msg.get("txType", msg.get("type",""))).lower()
    sol_amount = float(msg.get("solAmount", msg.get("amount", 0)) or 0)

    if d["first_trade_time"] is None: d["first_trade_time"] = time.time()
    d["trades"].append({"time": time.time(), "type": trade_type, "sol": sol_amount})
    if "buy"  in trade_type: d["buy_count"]  += 1
    elif "sell" in trade_type: d["sell_count"] += 1

    if sol_amount > 0:
        d["volumes"].append(sol_amount)
        d["volume_timeline"].append((time.time(), sol_amount))
        d["volume_timeline"] = [(t,v) for t,v in d["volume_timeline"] if time.time()-t <= 600]

    token_amt = float(msg.get("tokenAmount", 0) or 0)
    if sol_amount > 0 and token_amt > 0:
        est = sol_amount / token_amt; d["prices"].append(est)
        if est > d["peak_price"]: d["peak_price"] = est

    prices = list(d["prices"])
    if d["alert_price"] is None and prices: d["alert_price"] = prices[0]

    analysis = calculate_confidence(mint)
    now      = time.time()
    cat      = analysis.get("category")
    price    = prices[-1] if prices else 0

    if (cat in CONFIG["min_category"] and
            analysis["score"] >= CONFIG["min_confidence"] and
            not analysis.get("blocked", False)):

        # ── MARCA COMO ALERTADO — nunca mais repete ──
        alerted_tokens.add(mint)

        print_alert(mint, analysis, price, source)
        log_alert(mint, analysis, price, source)
        await send_discord_alert(mint, analysis, price, source)

        pending_checks[mint] = {
            "alert_price":     price,
            "alert_time":      now,
            "active_signals":  analysis["active_signals"],
            "name":            d.get("name","?"),
            "check_at":        now + CONFIG["check_after"],
            # Guarda contexto para diagnóstico
            "alert_mcap":      d.get("market_cap", 0),
            "alert_liq":       d.get("liquidity", 0),
            "alert_vol_ratio": (d.get("vol_1h",0) / d.get("vol_24h",1)) if d.get("vol_24h",0) > 0 else 0,
            "alert_in_hot":    is_hot_window(),
            "alert_score":     analysis["score"],
            "alert_category":  analysis.get("category","?"),
            # Dados Helius para diagnóstico
            "alert_holders":   d.get("holders", 0),
            "alert_top10_pct": d.get("top10_pct", 0),
            "alert_dev_holds": d.get("dev_still_holds", None),
            # Tracking de milestones
            "milestones_hit":  set(),
            "peak_change":     0,
            "drop_alerted_at": None,
        }

# ─────────────────────────────────────────────
# 🌐  WEBSOCKET — pump.fun
# ─────────────────────────────────────────────

async def pumpfun_scanner():
    url = "wss://pumpportal.fun/api/data"
    print("[pump.fun] Conectando...")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print("[pump.fun] ✅ Conectado! A varrer novas moedas...")
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeTokenTrade"}))
                async for raw in ws:
                    try: await process_trade(json.loads(raw), source="pump.fun")
                    except Exception: pass
        except Exception as e:
            print(f"[pump.fun] ❌ {e} — Reconectando em 5s..."); await asyncio.sleep(5)

# ─────────────────────────────────────────────
# 🌐  POLLING — DexScreener (moedas trending)
# ─────────────────────────────────────────────

async def dexscreener_scanner():
    """Busca moedas trending no DexScreener para Solana."""
    print("[DexScreener] Iniciando scanner de trending...")
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                # Trending tokens em Solana
                async with s.get(
                    "https://api.dexscreener.com/latest/dex/search?q=solana",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        data  = await r.json()
                        pairs = data.get("pairs") or []
                        for p in pairs[:20]:  # top 20
                            if p.get("chainId") != "solana": continue
                            mint = p.get("baseToken", {}).get("address")
                            if not mint or mint in alerted_tokens: continue

                            # Cria mensagem no formato do process_trade
                            msg = {
                                "mint":   mint,
                                "name":   p.get("baseToken", {}).get("name", "?"),
                                "symbol": p.get("baseToken", {}).get("symbol", "?"),
                            }
                            init_token(mint)
                            d = token_data[mint]
                            # Atualiza com dados do DexScreener diretamente
                            d["market_cap"] = float((p.get("fdv") or p.get("marketCap") or 0))
                            d["liquidity"]  = float((p.get("liquidity") or {}).get("usd") or 0)
                            d["vol_1h"]     = float((p.get("volume") or {}).get("h1") or 0)
                            d["vol_24h"]    = float((p.get("volume") or {}).get("h24") or 0)
                            price = float(p.get("priceUsd") or 0)
                            if price > 0: d["prices"].append(price)

                            await process_trade(msg, source="DexScreener")

        except Exception as e:
            print(f"[DexScreener] ❌ {e}")
        await asyncio.sleep(CONFIG["dexscreener_poll"])

# ─────────────────────────────────────────────
# ⏱️  LOOPS PERIÓDICOS
# ─────────────────────────────────────────────

async def update_loop():
    """
    Monitoriza preço de cada alerta a cada 30s.
    Só avisa quando há movimento significativo:
    SOBE → avisa em +25%, +50%, +100%, +200%
    DESCE → avisa em -20% e -40%, depois SILENCIA até superar o pico anterior
    """
    while True:
        await asyncio.sleep(30)
        now = time.time()

        for mint, data in list(pending_checks.items()):
            alert_price = data["alert_price"]

            # Busca preço atual
            dex = await fetch_dexscreener(mint)
            if dex and dex["price"] > 0:
                current_price = dex["price"]
            elif mint in token_data:
                prices = list(token_data[mint]["prices"])
                current_price = prices[-1] if prices else None
            else:
                current_price = None
            if not current_price: continue

            pct      = (current_price - alert_price) / alert_price * 100
            peak_pct = data.get("peak_pct", 0)
            silenced = data.get("silenced", False)

            # Atualiza pico máximo
            if pct > peak_pct:
                data["peak_pct"] = pct
                peak_pct = pct
                if silenced:
                    # Superou o último máximo → volta a monitorizar
                    data["silenced"] = False
                    silenced = False
                    print(f"[Monitor] {data.get('name','?')} superou máximo → volta a monitorizar")

            # Milestones de SUBIDA — avisa uma vez cada
            milestones_up = data.setdefault("milestones_up", set())
            for milestone in [25, 50, 100, 200]:
                if pct >= milestone and milestone not in milestones_up:
                    milestones_up.add(milestone)
                    await send_discord_movement(mint, data, current_price, pct, "up", milestone, dex)

            # Milestones de DESCIDA — só se não estiver silenciado
            if not silenced:
                milestones_down = data.setdefault("milestones_down", set())
                for milestone in [20, 40]:
                    if pct <= -milestone and milestone not in milestones_down:
                        milestones_down.add(milestone)
                        await send_discord_movement(mint, data, current_price, pct, "down", milestone, dex)
                        if milestone == 20:
                            data["silenced"] = True
                            print(f"[Monitor] {data.get('name','?')} -{milestone}% → silenciado até superar {peak_pct:.0f}%")

            await asyncio.sleep(1)  # pequena pausa entre moedas

async def maintenance_loop():
    while True:
        await asyncio.sleep(60)
        now = time.time()

        # Verifica alertas 2h depois → aprende
        for mint in [m for m,c in list(pending_checks.items()) if now >= c["check_at"]]:
            await check_and_learn(mint, pending_checks.pop(mint))

        # Status a cada 5 min
        if int(now) % 300 < 61:
            cleanup_old_tokens()
            hot = "🟢 ATIVA" if is_hot_window() else "⚪ inativa"
            print(f"\n[Status] {datetime.now().strftime('%H:%M:%S')} | Janela: {hot} | "
                  f"Vistas: {tokens_seen} | Alertadas: {len(alerted_tokens)} | "
                  f"Alertas Discord: {alerts_sent} | Aprendizagens: {learns_done}\n")

# ─────────────────────────────────────────────
# 🚀  MAIN
# ─────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("  🤖 TRADING BOT v8.0")
    print("  pump.fun + DexScreener | Auto-scanner | Auto-aprende")
    print("=" * 60)
    print(f"  Filtros    : MCap $100K–$500K | Liq $25K–$65K | Vol1H>15%")
    print(f"  Categoria  : só ROCKET e BOM (+50% potencial)")
    print(f"  Updates    : a cada 20 minutos por moeda alertada")
    print(f"  Aprende    : verifica resultado 2h após cada alerta")
    print(f"  Repetição  : NUNCA — cada moeda alertada só 1 vez")
    print(f"  Discord    : {'✅ configurado' if 'COLA' not in DISCORD_WEBHOOK_URL else '⚠️  não configurado'}")
    print(f"  Log        : {CONFIG['log_file']}")
    print("=" * 60 + "\n")

    await asyncio.gather(
        pumpfun_scanner(),
        dexscreener_scanner(),
        update_loop(),
        maintenance_loop(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n👋 Bot parado. Alertas: {alerts_sent} | Aprendizagens: {learns_done}")
        save_weights(WEIGHTS)
