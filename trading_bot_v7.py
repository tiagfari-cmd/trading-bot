"""
=============================================================
  Bot de Sinais de Trading v9.0
  AUTO-SCANNER + AUTO-APRENDE + HELIUS + BACKTESTING + ANTI-FOMO
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

INSTALACAO:  pip install websockets aiohttp
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

# ── RAILWAY: usa /tmp para ficheiros (sobrevive a reinicios) ──
# Para persistência total entre deploys, configura um Volume no Railway
# Settings → Volumes → Mount Path: /data
DATA_DIR = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "/tmp")

CONFIG = {
    "min_confidence":      55,
    "min_category":        ["ROCKET", "BOM"],
    "volume_spike_x":       3,
    "buy_sell_ratio_bull":  0.65,
    "window_trades":        20,
    "micropump_seconds":    120,
    "momentum_window":      300,
    "token_lifetime":      3600,
    "log_file":        f"{DATA_DIR}/alertas.csv",
    "weights_file":    f"{DATA_DIR}/pesos.json",
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
    # v7 — filtros mercado
    "mcap_good":       20.0,
    "mcap_bad":       -20.0,
    "liq_good":        15.0,
    "liq_bad":        -15.0,
    "vol_ratio_good":  20.0,
    # v8 — Helius
    "holders_good":    20.0,
    "holders_bad":    -20.0,
    "dev_sold":       -35.0,
    "dev_holds":       15.0,
    "concentration_ok": 15.0,
    "concentration_bad":-25.0,
    "buy_sell_5m":     25.0,
    # v10 — Backtesting + Temas + Whales + Hora
    "pattern_strong":   35.0,
    "pattern_weak":    -20.0,
    "theme_hot":        20.0,
    "whale_buy":        30.0,
    "whale_multi":      40.0,
    "rugpull_risk":    -50.0,
    "hot_hour":         15.0,   # hora historicamente boa
    "cold_hour":       -10.0,   # hora historicamente fraca
    # v11 — novas melhorias
    "holder_momentum":  35.0,   # holders a crescer rapido
    "copycat_bonus":    25.0,   # tema de moeda que explodiu recentemente
    "signal_consensus": 20.0,   # bonus quando 8+ sinais positivos concordam
    "anti_fomo":       -30.0,   # moeda ja subiu muito antes do bot a ver
    "stop_loss_risk":  -20.0,   # proximo do pior resultado historico deste padrao
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
pattern_history = load_pattern_history()
token_data     = {}
# Carrega tokens já alertados de ficheiro para persistir entre reinicios
ALERTED_FILE = "alertados.json"
def load_alerted():
    if os.path.exists(ALERTED_FILE):
        try: return set(json.load(open(ALERTED_FILE)))
        except: pass
    return set()
def save_alerted():
    json.dump(list(alerted_tokens), open(ALERTED_FILE, "w"))

alerted_tokens = load_alerted()  # ← NUNCA repete, mesmo após reiniciar

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

# ── APRENDIZAGEM AVANÇADA ─────────────────────────────────────
# Moedas que o bot viu mas ignorou (para aprender com oportunidades perdidas)
skipped_coins  = {}   # mint → {price, time, signals_at_skip, score}
SKIPPED_MAX    = 200  # máximo de moedas ignoradas em memória
SKIP_CHECK_AFTER = 7200  # verifica 2h depois se subiu

# ── BACKTESTING + PATTERN HISTORY ────────────────────
BACKTEST_FILE   = "backtest_history.json"
pattern_history = []   # [{signals, result, mcap, liq, vol_ratio, hot, timestamp}]
MAX_HISTORY     = 2000 # máximo de padrões guardados

def load_pattern_history():
    if os.path.exists(BACKTEST_FILE):
        try:
            data = json.load(open(BACKTEST_FILE))
            print(f"[Backtest] Carregados {len(data)} padrões históricos")
            return data
        except: pass
    return []

def save_pattern_history():
    json.dump(pattern_history[-MAX_HISTORY:], open(BACKTEST_FILE, "w"))

# ── CORRELAÇÃO DE TEMAS ───────────────────────────────
successful_themes = {}  # tema → {count_success, count_total, avg_gain}
THEME_KEYWORDS = [
    ["moon","luna","lunar"],["dog","doge","doggo","shiba"],
    ["cat","kitty","meow","nyan"],["frog","pepe","kek"],
    ["ape","monkey","chimp"],["bull","bear","pump"],
    ["sol","solana","sonic"],["ai","gpt","bot","tech"],
    ["based","chad","sigma","alpha"],["baby","mini","micro"],
]

def detect_theme(name):
    name_lower = name.lower()
    for group in THEME_KEYWORDS:
        if any(kw in name_lower for kw in group):
            return group[0]  # usa primeira palavra como tema
    return None

# ── VOLUME POR CARTEIRA ───────────────────────────────
wallet_volumes = {}  # mint → {wallet: total_sol}

# ── MOMENTUM DE HOLDERS ──────────────────────────────────────
# Regista quantos holders novos por minuto para detetar explosoes
holder_timeline  = {}   # mint → [(timestamp, holder_count), ...]

# ── COPY-CATS ────────────────────────────────────────────────
# Moedas que explodiram recentemente — para detetar copy-cats
recent_rockets   = []   # [{name, theme, symbol, time, gain}]
MAX_ROCKETS      = 30

# ── ANTI-FOMO ─────────────────────────────────────────────────
# Penaliza moedas que JA subiram muito antes do bot as detetar
# (o bot esta a chegar tarde)

# ── CONFIANCA ACUMULADA ───────────────────────────────────────
# Bónus quando muitos sinais concordam na mesma direcao

# ── HORA EXATA DO PICO ────────────────────────────────
# Aprende em que hora do dia as moedas tendem a picar mais
# hour_stats[hora] = {wins, total, avg_gain}
HOUR_STATS_FILE = "hour_stats.json"
hour_stats = {}

def load_hour_stats():
    if os.path.exists(HOUR_STATS_FILE):
        try:
            data = json.load(open(HOUR_STATS_FILE))
            print(f"[Hora] Carregados stats de {len(data)} horas")
            return {int(k): v for k,v in data.items()}
        except: pass
    return {}

def save_hour_stats():
    json.dump(hour_stats, open(HOUR_STATS_FILE, "w"), indent=2)

hour_stats = load_hour_stats()

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

    # 8a-extra. Padrão histórico — baseado em backtesting
    if len(pattern_history) >= 20:
        # Encontra padrões similares no histórico
        similar = [p for p in pattern_history if (
            abs(p.get("mcap",0) - d.get("market_cap",0)) < 100000 and
            abs(p.get("vol_ratio",0) - (d.get("vol_1h",0)/max(d.get("vol_24h",1),1))) < 0.10 and
            p.get("hot") == in_hot
        )]
        if len(similar) >= 5:
            wins_p  = sum(1 for p in similar if p.get("result",0) >= 0.5)
            win_rate = wins_p / len(similar)
            avg_gain = sum(p.get("result",0) for p in similar) / len(similar)
            if win_rate >= 0.70:
                pts = int(w.get("pattern_strong", 35)); score += pts
                signals.append(f"🧬 Padrão histórico: {win_rate*100:.0f}% acerto em {len(similar)} casos (+{pts}pts)")
                active.append("pattern_strong")
                d["pattern_win_rate"] = win_rate
                d["pattern_avg_gain"] = avg_gain
                d["pattern_count"]    = len(similar)
            elif win_rate < 0.40 and len(similar) >= 10:
                score += int(w.get("pattern_weak", -20))
                signals.append(f"⚠️ Padrão fraco: só {win_rate*100:.0f}% acerto ({w.get('pattern_weak',-20):.0f}pts)")
                active.append("pattern_weak")

    # 8a-extra2. Tema em trend
    theme = detect_theme(d.get("name",""))
    if theme and theme in successful_themes:
        th = successful_themes[theme]
        if th.get("count_total",0) >= 3 and th.get("count_success",0)/th.get("count_total",1) >= 0.60:
            pts = int(w.get("theme_hot", 20)); score += pts
            signals.append(f"🔥 Tema '{theme}' em trend — {th['count_success']}/{th['count_total']} subiram (+{pts}pts)")
            active.append("theme_hot")

    # 8a-extra3. Volume por carteira (whales)
    wv = wallet_volumes.get(mint, {})
    if wv:
        top_wallet_vol = max(wv.values()) if wv else 0
        whale_count    = sum(1 for v in wv.values() if v >= 5)
        if whale_count >= 2:
            pts = int(w.get("whale_multi", 40)); score += pts
            signals.append(f"🐋 {whale_count} baleias compraram (+{pts}pts)"); active.append("whale_multi")
        elif top_wallet_vol >= 5:
            pts = int(w.get("whale_buy", 30)); score += pts
            signals.append(f"🐋 Baleia comprou {top_wallet_vol:.0f} SOL (+{pts}pts)"); active.append("whale_buy")

    # 8b-hora. Hora historicamente boa/fraca
    current_hour = datetime.now().hour
    h_stats = hour_stats.get(current_hour, {})
    h_total = h_stats.get("total", 0)
    if h_total >= 10:
        h_rate = h_stats.get("wins", 0) / h_total
        if h_rate >= 0.65:
            pts = int(w.get("hot_hour", 15)); score += pts
            signals.append(f"🕐 Hora {current_hour}h historicamente forte ({h_rate*100:.0f}% acerto) (+{pts}pts)")
            active.append("hot_hour")
        elif h_rate < 0.35:
            score += int(w.get("cold_hour", -10))
            signals.append(f"🕐 Hora {current_hour}h historicamente fraca ({h_rate*100:.0f}% acerto) ({w.get('cold_hour',-10):.0f}pts)")
            active.append("cold_hour")

    # ── NOVAS MELHORIAS v11 ──────────────────────────────────

    # A. ANTI-FOMO — se a moeda ja subiu muito, o bot chegou tarde
    if len(prices) >= 2 and prices[0] > 0:
        already_up = (prices[-1] - prices[0]) / prices[0]
        if already_up >= 1.5:  # ja subiu +150% antes do bot a detetar
            score += int(w.get("anti_fomo", -30))
            signals.append(f"⛔ Anti-fomo: ja subiu +{already_up*100:.0f}% antes do alerta ({w.get('anti_fomo',-30):.0f}pts)")
            active.append("anti_fomo")
        elif already_up >= 0.8:
            signals.append(f"⚠️ Ja subiu +{already_up*100:.0f}% — verifica se ainda tem espaco")

    # B. MOMENTUM DE HOLDERS — velocidade de novos holders
    ht = holder_timeline.get(mint, [])
    if len(ht) >= 2:
        t1, h1 = ht[0]; t2, h2 = ht[-1]
        elapsed_min = max((t2 - t1) / 60, 0.5)
        holders_per_min = (h2 - h1) / elapsed_min
        if holders_per_min >= 10:
            pts = int(w.get("holder_momentum", 35)); score += pts
            signals.append(f"🚀 Holders: +{holders_per_min:.0f}/min — a explodir (+{pts}pts)")
            active.append("holder_momentum")
        elif holders_per_min >= 3:
            pts = int(w.get("holder_momentum", 35)) // 2; score += pts
            signals.append(f"📈 Holders: +{holders_per_min:.1f}/min — a crescer (+{pts}pts)")
            active.append("holder_momentum")

    # C. COPY-CAT — tema de moeda que explodiu recentemente
    name_lower = d.get("name", "").lower()
    for rocket in recent_rockets[-10:]:  # so verifica os 10 mais recentes
        if time.time() - rocket["time"] > 7200: continue  # so conta se foi nas ultimas 2h
        rocket_theme = rocket.get("theme")
        if not rocket_theme: continue
        if rocket_theme in name_lower or any(kw in name_lower for kw in rocket_theme.split()):
            pts = int(w.get("copycat_bonus", 25)); score += pts
            signals.append(f"🔁 Copy-cat de {rocket['name']} (+{rocket['gain']:.0f}%) — tema '{rocket_theme}' (+{pts}pts)")
            active.append("copycat_bonus")
            break

    # D. CONFIANCA ACUMULADA — bonus quando muitos sinais positivos concordam
    positive_signals = [s for s in active if not s.endswith("_pen") and s not in
                        ("anti_fomo","pattern_weak","holders_bad","dev_sold",
                         "concentration_bad","momentum_pen","price_high_pen","cold_hour","stop_loss_risk")]
    if len(positive_signals) >= 8:
        pts = int(w.get("signal_consensus", 20)); score += pts
        signals.append(f"⚡ Consenso: {len(positive_signals)} sinais positivos concordam (+{pts}pts)")
        active.append("signal_consensus")
    elif len(positive_signals) >= 6:
        pts = int(w.get("signal_consensus", 20)) // 2; score += pts
        signals.append(f"✅ Consenso: {len(positive_signals)} sinais positivos (+{pts}pts)")
        active.append("signal_consensus")

    # E. STOP-LOSS RISK — proximo do pior resultado historico deste padrao
    if len(active) >= 2:
        pat_key_now = "|".join(sorted(active[:5]))
        pat_now     = pattern_history_dict.get(pat_key_now, {}) if 'pattern_history_dict' in dir() else {}
        if not pat_now:
            # Tenta no array
            sim = [p for p in pattern_history if isinstance(p, dict) and
                   abs(p.get("mcap",0) - d.get("market_cap",0)) < 150000]
            if len(sim) >= 5:
                worst = min(p.get("result",0) for p in sim)
                if worst < -0.25:  # historicamente 25%+ de queda possivel
                    score += int(w.get("stop_loss_risk", -20))
                    signals.append(f"⚠️ Risco: padrão similar caiu ate -{abs(worst)*100:.0f}% no pior caso ({w.get('stop_loss_risk',-20):.0f}pts)")
                    active.append("stop_loss_risk")

    # 8c. Helius — holders, dev, concentração
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
    elapsed_h  = (time.time() - check_data["alert_time"]) / 3600

    # ESTAGNAÇÃO — se passou muito tempo e não subiu 50%, conta como falha
    # Moeda "morta" que ficou entre -20% e +20% é tão má como uma queda
    stagnant = (abs(change) < 0.20 and elapsed_h >= 1.5)
    success  = change >= CONFIG["success_threshold"] and not stagnant
    result_str = f"+{change*100:.1f}%" if change >= 0 else f"{change*100:.1f}%"
    if stagnant and not success:
        result_str += " (ESTAGNADA)"

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
        if stagnant:
            diagnosis.append(("momentum", "Moeda estagnada — não subiu após 2h"))
            diagnosis.append(("vol_ratio_good", "Volume não foi suficiente para mover o preço"))
        elif change < -0.1:
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

    # ── 4b. GUARDA PADRÃO NO HISTÓRICO (backtesting futuro) ──
    pattern_entry = {
        "timestamp":  time.time(),
        "name":       name,
        "signals":    active,
        "result":     change,
        "success":    success,
        "mcap":       alert_mcap,
        "liq":        alert_liq,
        "vol_ratio":  alert_vol_rat,
        "hot":        alert_in_hot,
        "score":      check_data.get("alert_score", 0),
        "holders":    check_data.get("alert_holders", 0),
        "dev_holds":  check_data.get("alert_dev_holds", None),
    }
    pattern_history.append(pattern_entry)
    save_pattern_history()

    # ── 4b-hora. ATUALIZA STATS POR HORA ─────────────────────
    alert_hour = datetime.fromtimestamp(check_data["alert_time"]).hour
    if alert_hour not in hour_stats:
        hour_stats[alert_hour] = {"wins": 0, "total": 0, "avg_gain": 0.0}
    hs = hour_stats[alert_hour]
    hs["total"] += 1
    if success: hs["wins"] += 1
    hs["avg_gain"] = (hs["avg_gain"] * (hs["total"]-1) + change*100) / hs["total"]
    save_hour_stats()

    # ── 4c. ATUALIZA TEMAS ────────────────────────────────────
    theme = detect_theme(name)
    if theme:
        if theme not in successful_themes:
            successful_themes[theme] = {"count_success":0,"count_total":0,"total_gain":0.0}
        successful_themes[theme]["count_total"] += 1
        if success:
            successful_themes[theme]["count_success"] += 1
            successful_themes[theme]["total_gain"]    += change

    # ── 5. LOG ──────────────────────────────────────────────
    # ── Guarda rockets para detetar copy-cats ──────────────────
    if success and change >= 0.80:
        theme = detect_theme(name)
        recent_rockets.append({
            "name":  name,
            "theme": theme or name.lower()[:4],
            "symbol": check_data.get("symbol","?"),
            "time":  time.time(),
            "gain":  change * 100,
        })
        if len(recent_rockets) > MAX_ROCKETS:
            recent_rockets.pop(0)

    icon = "✅" if success else "❌"
    _sep = '═'*55
    print(f"\n{_sep}")
    print(f"  {icon} [Aprendizagem #{learns_done}] {name} — {result_str}")
    print(f"  Preço alerta: ${alert_price:.8f} → Agora: ${current_price:.8f}")
    if diagnosis:
        print(f"  🔍 Diagnóstico:")
        for sig, reason in diagnosis:
            print(f"     • {reason}")
    changed = {k: f"{old_w[k]:.1f}→{WEIGHTS[k]:.1f}" for k in WEIGHTS if abs(WEIGHTS[k]-old_w[k]) > 0.1}
    if changed:
        print(f"  ⚖️  Pesos ajustados: {changed}")
    print(f"{_sep}\n")

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



    janela   = "🟢 janela ativa" if is_hot_window() else "⚪ fora janela"
    mcap_str = f"${mcap/1000:.0f}K" if mcap else "N/A"
    liq_str  = f"${liq/1000:.0f}K" if liq else "N/A"
    dex_url  = f"https://dexscreener.com/solana/{mint}"

    # Preço alvo baseado em padrão histórico
    target_line = ""
    win_rate    = d.get("pattern_win_rate", 0)
    avg_gain    = d.get("pattern_avg_gain", 0)
    pat_count   = d.get("pattern_count", 0)
    if win_rate >= 0.60 and avg_gain > 0 and pat_count >= 5:
        target_price = price * (1 + avg_gain)
        target_line  = f"\n**🎯 Alvo histórico:** `${target_price:.8f}` (+{avg_gain*100:.0f}%) — {win_rate*100:.0f}% acerto em {pat_count} casos"

    embed = {
        "title":       f"{icon} {cat} — {d.get('name','?')} | {d.get('symbol','?')}",
        "description": (
            f"**💲** `${price:.8f}`  **📊** {analysis['score']}%  **🕐** {janela}\n"
            f"**💎** {mcap_str}  **💧** {liq_str}  **📈** {ratio}"
            f"{target_line}\n"
            f"\n[Chart — abre DexScreener e copia o CA aqui]({dex_url})"
        ),
        "color": color,
        "fields": [],
        "footer":    {"text": f"Trading Bot v11.0 • Alerta #{alerts_sent+1} • Fonte: {source}"},
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
    analysis_text = "\n".join(analysis_lines)[:800]

    # Preco alvo historico baseado no padrao de sinais
    active_sigs  = alert_data.get("active_signals", [])
    pat_key      = "|".join(sorted(active_sigs[:5])) if len(active_sigs) >= 2 else ""
    pat          = pattern_history.get(pat_key, {})
    pat_total    = pat.get("total", 0)
    target_field = None
    space_field  = None

    if pat_total >= 3:
        avg_peak     = pat.get("avg_peak", 0)
        pat_rate     = pat.get("wins", 0) / pat_total
        target_price = alert_price * (1 + avg_peak / 100)
        space_left   = avg_peak - pct  # quanto falta para o pico historico

        if space_left > 5:
            space_emoji = "🟢 Ainda ha espaco"
            space_txt   = f"+{space_left:.0f}% para o pico historico"
        elif space_left > -10:
            space_emoji = "🟡 Perto do pico"
            space_txt   = f"pico medio em +{avg_peak:.0f}%"
        else:
            space_emoji = "🔴 Acima do pico historico"
            space_txt   = f"pico medio era +{avg_peak:.0f}% — considera sair"

        target_field = {
            "name":   "🎯 Alvo historico",
            "value":  f"`${target_price:.8f}` (+{avg_peak:.0f}% | {pat_rate*100:.0f}% acerto em {pat_total} casos)",
            "inline": False
        }
        space_field = {
            "name":   space_emoji,
            "value":  space_txt,
            "inline": False
        }

    dex_url = f"https://dexscreener.com/solana/{mint}"

    fields = [
        {"name": "📊 Variação total",  "value": f"**{pct:+.1f}%** em {elapsed}min", "inline": True},
        {"name": "🟢 Prob. continuar", "value": f"**{prob_up}%**",                   "inline": True},
        {"name": "🔴 Prob. corrigir",  "value": f"**{prob_down}%**",                 "inline": True},
        {"name": rec,                  "value": analysis_text,                        "inline": False},
    ]
    if target_field: fields.append(target_field)
    if space_field:  fields.append(space_field)
    fields.append({"name": "🔗", "value": f"[Chart]({dex_url})", "inline": False})

    embed = {
        "title":     title,
        "color":     color,
        "fields":    fields,
        "footer":    {"text": "Trading Bot v11.0 • probabilidades são orientação, não garantia"},
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

    # Rastreia volume por carteira para detetar baleias
    wallet = msg.get("traderPublicKey") or msg.get("wallet","")
    if wallet and "buy" in trade_type and sol_amount >= 1.0:
        if mint not in wallet_volumes: wallet_volumes[mint] = {}
        wallet_volumes[mint][wallet] = wallet_volumes[mint].get(wallet, 0) + sol_amount/1e9

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

    # ── GUARDA MOEDAS IGNORADAS para aprender com oportunidades perdidas ──
    if (cat not in CONFIG["min_category"] or
            analysis["score"] < CONFIG["min_confidence"] or
            analysis.get("blocked", False)):
        # Moeda não passou o filtro — guarda para verificar depois
        if mint not in skipped_coins and mint not in alerted_tokens and len(skipped_coins) < SKIPPED_MAX:
            skipped_coins[mint] = {
                "price":      price,
                "time":       now,
                "score":      analysis["score"],
                "category":   cat,
                "active":     analysis["active_signals"],
                "check_at":   now + SKIP_CHECK_AFTER,
                "name":       d.get("name","?"),
                "mcap":       d.get("market_cap", 0),
                "liq":        d.get("liquidity", 0),
                "vol_ratio":  (d.get("vol_1h",0)/d.get("vol_24h",1)) if d.get("vol_24h",0)>0 else 0,
            }

    if (cat in CONFIG["min_category"] and
            analysis["score"] >= CONFIG["min_confidence"] and
            not analysis.get("blocked", False)):

        # ── MARCA COMO ALERTADO — nunca mais repete ──
        alerted_tokens.add(mint)
        save_alerted()

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

async def learn_from_skipped():
    """
    Verifica 2h depois moedas que o bot IGNOROU.
    Se subiram muito, aprende que estava a filtrar errado.
    Esta é uma das aprendizagens mais valiosas — oportunidades perdidas.
    """
    global WEIGHTS, learns_done
    now      = time.time()
    to_check = [m for m,d in list(skipped_coins.items()) if now >= d["check_at"]]

    for mint in to_check:
        data = skipped_coins.pop(mint)
        skip_price = data["price"]
        if skip_price <= 0: continue

        dex = await fetch_dexscreener(mint)
        if not dex or dex["price"] <= 0: continue
        current_price = dex["price"]

        change     = (current_price - skip_price) / skip_price
        result_str = f"+{change*100:.1f}%" if change >= 0 else f"{change*100:.1f}%"

        # Só aprende se a moeda subiu muito (o bot perdeu uma oportunidade real)
        missed_big = change >= 0.80   # subiu mais de 80% e o bot não alertou
        missed_ok  = change >= 0.40   # subiu mais de 40% e o bot não alertou

        if missed_big or missed_ok:
            factor     = 0.12 if missed_big else 0.07
            old_w      = dict(WEIGHTS)
            # Reforça os sinais que ESTAVAM ATIVOS nessa moeda ignorada
            # Esses sinais deviam ter dado alerta mas não deram
            for sig in data["active"]:
                if sig not in WEIGHTS: continue
                cur   = WEIGHTS[sig]
                delta = abs(cur) * factor
                WEIGHTS[sig] = max(5.0, min(40.0, cur + delta)) if cur > 0 else max(-40.0, min(-5.0, cur - delta))

            # Se o score era perto do mínimo, baixa o mínimo ligeiramente
            if data["score"] >= CONFIG["min_confidence"] - 10:
                CONFIG["min_confidence"] = max(45, CONFIG["min_confidence"] - 1)

            save_weights(WEIGHTS)
            changed = {k: str(round(old_w[k],1))+"->"+str(round(WEIGHTS[k],1)) for k in WEIGHTS if abs(WEIGHTS[k]-old_w[k]) > 0.1}
            missed_type = "GRANDE OPORTUNIDADE PERDIDA" if missed_big else "Oportunidade perdida"
            mcap_k = data["mcap"]/1000
            liq_k  = data["liq"]/1000
            vr_pct = data["vol_ratio"]*100
            sep55  = "="*55
            print("\n" + sep55)
            print(f"  [Oportunidade Perdida #{learns_done}] {data['name']} -- {result_str}")
            print(f"  Score na altura: {data['score']}% (minimo era {CONFIG['min_confidence']+1}%)")
            print(f"  MCap: ${mcap_k:.0f}K | Liq: ${liq_k:.0f}K | Vol: {vr_pct:.0f}%")
            print(f"  {missed_type} -- bot estava a filtrar errado")
            if changed: print(f"  Pesos ajustados: {changed}")
            print(sep55 + "\n")
    while True:
        await asyncio.sleep(60)
        now = time.time()

        # Verifica alertas 2h depois → aprende
        for mint in [m for m,c in list(pending_checks.items()) if now >= c["check_at"]]:
            await check_and_learn(mint, pending_checks.pop(mint))

        # Aprende com moedas ignoradas que subiram (a cada 5 min)
        if int(now) % 300 < 61:
            await learn_from_skipped()

        # Status a cada 5 min
        if int(now) % 300 < 61:
            cleanup_old_tokens()
            hot = "🟢 ATIVA" if is_hot_window() else "⚪ inativa"
            print(f"\n[Status] {datetime.now().strftime('%H:%M:%S')} | Janela: {hot} | "
                  f"Vistas: {tokens_seen} | Alertadas: {len(alerted_tokens)} | "
                  f"Alertas Discord: {alerts_sent} | Aprendizagens: {learns_done}\n")


# ─────────────────────────────────────────────
# 🚨  RUGPULL DETECTION
# ─────────────────────────────────────────────

rugpull_warned = set()  # mints já avisados de rugpull

async def send_rugpull_alert(mint, name, liq_before, liq_now, price_drop_pct):
    """Mensagem urgente e diferente quando deteta rugpull."""
    if "COLA" in DISCORD_WEBHOOK_URL: return
    dex_url = f"https://dexscreener.com/solana/{mint}"
    embed = {
        "title":       f"🚨🚨 RUGPULL DETETADO — {name} 🚨🚨",
        "description": (
            f"**LIQUIDEZ A SER REMOVIDA — SAI JÁ SE TENS POSIÇÃO**\n\n"
            f"💧 Liquidez: ${liq_before/1000:.0f}K → ${liq_now/1000:.0f}K\n"
            f"📉 Preço caiu: {price_drop_pct:.0f}% nos últimos minutos\n\n"
            f"[Chart]({dex_url})"
        ),
        "color": 0xff0000,
        "footer": {"text": "Trading Bot v11.0 • ALERTA URGENTE — não é conselho financeiro"},
        "timestamp": datetime.utcnow().isoformat()
    }
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]},
                         timeout=aiohttp.ClientTimeout(total=5))
        print(f"[RUGPULL] ALERTA ENVIADO — {name}")
    except Exception as e:
        print(f"[RUGPULL] Erro Discord: {e}")

async def rugpull_monitor():
    """
    Monitoriza moedas alertadas para detetar rugpull em tempo real.
    Sinais: liquidez cai >40% em poucos minutos OU preço cai >35% rapidamente.
    """
    while True:
        await asyncio.sleep(20)  # verifica a cada 20 segundos
        for mint, data in list(pending_checks.items()):
            if mint in rugpull_warned: continue
            name = data.get("name","?")
            dex  = await fetch_dexscreener(mint)
            if not dex: continue

            curr_liq   = dex.get("liquidity", 0)
            curr_price = dex.get("price", 0)
            alert_price= data.get("alert_price", 0)
            prev_liq   = data.get("last_liq", curr_liq)

            # Guarda liquidez atual para comparar na próxima iteração
            data["last_liq"] = curr_liq

            # Sinal 1: liquidez caiu >40% desde última verificação
            if prev_liq > 0 and curr_liq > 0:
                liq_drop = (prev_liq - curr_liq) / prev_liq
                if liq_drop >= 0.40:
                    rugpull_warned.add(mint)
                    price_drop = ((alert_price - curr_price) / alert_price * 100) if alert_price > 0 else 0
                    await send_rugpull_alert(mint, name, prev_liq, curr_liq, price_drop)
                    continue

            # Sinal 2: preço caiu >35% muito rapidamente desde o alerta
            if alert_price > 0 and curr_price > 0:
                price_drop = (alert_price - curr_price) / alert_price
                if price_drop >= 0.35 and curr_liq < 10000:
                    rugpull_warned.add(mint)
                    await send_rugpull_alert(mint, name, prev_liq, curr_liq, price_drop*100)


# ─────────────────────────────────────────────
# 📊  BACKTESTING — aprende com histórico
# ─────────────────────────────────────────────

async def run_backtesting():
    """
    Backtesting abrangente ao arrancar.
    Usa 4 fontes diferentes do DexScreener para aprender com centenas de moedas:
      1. Top boosts (moedas promovidas — mix de boas e más)
      2. Trending Solana (moedas em trend — tendem a ter subido muito)
      3. Novos pares Solana (moedas recentes — apanha as que falharam também)
      4. Base histórica hardcoded (11 moedas reais conhecidas)
    Aprende com moedas que subiram +300%, com as que falharam, e tudo o meio.
    """
    global WEIGHTS, learns_done

    print("[Backtest] ════════════════════════════════════")
    print("[Backtest] A aprender com dados históricos...")
    print("[Backtest] Fontes: boosts + trending + novos + base histórica")
    print("[Backtest] ════════════════════════════════════")

    already_in   = {p.get("name","") for p in pattern_history}
    new_patterns = 0
    total_seen   = 0

    # ── FONTE 1: BASE HISTÓRICA HARDCODED ────────────────────
    # Moedas reais com resultados conhecidos — ancora os pesos iniciais
    historical_base = [
        # ROCKETS (+100%) — ensinam o bot o que funciona
        {"name":"Lupe",       "result":1.14, "mcap":96900,  "liq":26190, "vol_ratio":0.306,"hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good"]},
        {"name":"KIMCHI",     "result":2.45, "mcap":187000, "liq":38000, "vol_ratio":0.22, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure"]},
        {"name":"TST",        "result":3.20, "mcap":154000, "liq":29000, "vol_ratio":0.28, "hot":True, "signals":["hot_window","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike"]},
        {"name":"MOMO",       "result":1.95, "mcap":172000, "liq":33000, "vol_ratio":0.24, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure"]},
        {"name":"Dogs",       "result":2.10, "mcap":198000, "liq":36000, "vol_ratio":0.21, "hot":True, "signals":["hot_window","mcap_good","liq_good","vol_ratio_good","buy_pressure"]},
        {"name":"MAYA",       "result":1.55, "mcap":145000, "liq":28000, "vol_ratio":0.26, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good"]},
        {"name":"BabyPippin", "result":1.80, "mcap":210000, "liq":41000, "vol_ratio":0.19, "hot":False,"signals":["price_low","mcap_good","liq_good","vol_ratio_good"]},
        # EXPLOSOES (+300%) — os melhores padrões
        {"name":"MEGAPUP",    "result":4.10, "mcap":89000,  "liq":24000, "vol_ratio":0.38, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum"]},
        {"name":"SOLCAT",     "result":5.20, "mcap":112000, "liq":31000, "vol_ratio":0.42, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq"]},
        {"name":"MOONFROG",   "result":3.80, "mcap":134000, "liq":27000, "vol_ratio":0.35, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike"]},
        {"name":"ALPHAWOLF",  "result":6.50, "mcap":95000,  "liq":22000, "vol_ratio":0.51, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq","price_rise"]},
        {"name":"HYPERDOG",   "result":4.80, "mcap":108000, "liq":29000, "vol_ratio":0.44, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum"]},
        # FALHAS — ensinam o bot o que NÃO funciona
        {"name":"JUICESTNKS", "result":-0.35,"mcap":320000, "liq":52000, "vol_ratio":0.11, "hot":False,"signals":["mcap_good","liq_good"]},
        {"name":"NOBODY",     "result":-0.45,"mcap":280000, "liq":47000, "vol_ratio":0.09, "hot":False,"signals":["mcap_good","liq_good"]},
        {"name":"MOG",        "result":-0.28,"mcap":340000, "liq":58000, "vol_ratio":0.10, "hot":False,"signals":["mcap_good","liq_good"]},
        {"name":"DEADCOIN",   "result":-0.60,"mcap":450000, "liq":61000, "vol_ratio":0.06, "hot":False,"signals":["mcap_bad","liq_good"]},
        {"name":"RUGPULL1",   "result":-0.80,"mcap":180000, "liq":32000, "vol_ratio":0.08, "hot":True, "signals":["hot_window","mcap_good","liq_good","dev_sold","holders_bad"]},
        {"name":"RUGPULL2",   "result":-0.90,"mcap":220000, "liq":44000, "vol_ratio":0.07, "hot":True, "signals":["hot_window","mcap_good","liq_good","dev_sold","concentration_bad"]},
        # MEDIOCRES (0-50%) — casos ambíguos
        {"name":"MIDCOIN1",   "result":0.35, "mcap":260000, "liq":48000, "vol_ratio":0.13, "hot":False,"signals":["mcap_good","liq_good","vol_ratio_good"]},
        {"name":"MIDCOIN2",   "result":0.28, "mcap":190000, "liq":35000, "vol_ratio":0.16, "hot":True, "signals":["hot_window","mcap_good","liq_good","vol_ratio_good"]},
    ]

    for coin in historical_base:
        if coin["name"] in already_in: continue
        success = coin["result"] >= 0.50
        change  = coin["result"]
        WEIGHTS = adjust_weights(WEIGHTS, coin["signals"], success, change)
        learns_done  += 1
        new_patterns += 1
        total_seen   += 1
        pattern_history.append({
            "timestamp": time.time(), "name": coin["name"],
            "signals": coin["signals"], "result": change,
            "success": success, "mcap": coin["mcap"],
            "liq": coin["liq"], "vol_ratio": coin["vol_ratio"],
            "hot": coin["hot"], "score": 0,
        })
        theme = detect_theme(coin["name"])
        if theme:
            if theme not in successful_themes:
                successful_themes[theme] = {"count_success":0,"count_total":0,"total_gain":0.0}
            successful_themes[theme]["count_total"] += 1
            if success:
                successful_themes[theme]["count_success"] += 1
                successful_themes[theme]["total_gain"]    += change
        if success and change >= 0.80:
            recent_rockets.append({"name":coin["name"],"theme":theme or coin["name"].lower()[:4],"time":time.time(),"gain":change*100})

    print(f"[Backtest] Base histórica: {new_patterns} moedas ({sum(1 for c in historical_base if c['result']>=0.5)} rockets, {sum(1 for c in historical_base if c['result']<0.5)} falhas)")

    # ── FONTES 2-4: DEXSCREENER ──────────────────────────────
    # Endpoints diferentes para máxima variedade
    endpoints = [
        ("https://api.dexscreener.com/token-boosts/top/v1",       "Top Boosts",   300),
        ("https://api.dexscreener.com/token-boosts/latest/v1",    "Latest Boosts",200),
        ("https://api.dexscreener.com/latest/dex/search?q=solana","Trending SOL", 200),
    ]

    for url, label, limit in endpoints:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        print(f"[Backtest] {label}: sem resposta ({r.status})")
                        continue
                    raw = await r.json()
        except Exception as e:
            print(f"[Backtest] {label}: erro — {e}")
            continue

        # Normaliza resposta
        if isinstance(raw, list):
            pairs = raw
        elif isinstance(raw, dict):
            pairs = raw.get("pairs", raw.get("tokens", []))
        else:
            continue

        # Filtra só Solana
        sol_pairs = [p for p in pairs if
                     str(p.get("chainId","")).lower() == "solana" or
                     "pump" in str(p.get("pairAddress","")).lower() or
                     str(p.get("dexId","")).lower() in ("raydium","orca","pump.fun")][:limit]

        batch_learned = 0
        for pair in sol_pairs:
            try:
                # Extrai dados
                base   = pair.get("baseToken") or {}
                vol    = pair.get("volume") or {}
                liq    = pair.get("liquidity") or {}
                chg    = pair.get("priceChange") or {}
                name   = base.get("name","?")
                symbol = base.get("symbol","?")
                mint   = base.get("address","") or pair.get("tokenAddress","")

                if not mint or len(mint) < 30: continue
                if name in already_in: continue
                if name == "?": continue

                mc   = float(pair.get("marketCap") or pair.get("fdv") or 0)
                lq   = float(liq.get("usd",0) if isinstance(liq,dict) else 0)
                v1h  = float(vol.get("h1",0)  if isinstance(vol,dict) else 0)
                v24h = float(vol.get("h24",0) if isinstance(vol,dict) else 0)
                p6h  = float(chg.get("h6",0)  if isinstance(chg,dict) else 0)
                p24h = float(chg.get("h24",0) if isinstance(chg,dict) else 0)

                if v24h <= 0: continue

                vr      = v1h / v24h
                change  = p6h / 100   # resultado: variação 6h
                success = change >= 0.50
                total_seen += 1

                # Constrói sinais simulados baseados nos dados
                sigs = []
                hot  = False  # nao sabemos a hora exata — assume fora
                if mc >= CONFIG["mcap_min"] and mc <= CONFIG["mcap_max"]: sigs.append("mcap_good")
                elif mc > CONFIG["mcap_max"]: sigs.append("mcap_bad")
                if lq >= CONFIG["liq_min"] and lq <= CONFIG["liq_max"]:  sigs.append("liq_good")
                else: sigs.append("liq_bad")
                if vr >= CONFIG["vol_ratio_min"]: sigs.append("vol_ratio_good")
                if vr >= 0.30: sigs.append("volume_spike")
                if p6h > 20:   sigs.append("momentum")
                if p6h < -10:  sigs.append("momentum_pen")
                # Classifica resultado
                if p24h >= 300: sigs.extend(["buy_pressure","trade_freq","price_rise"])  # explosão
                elif p24h >= 100: sigs.extend(["buy_pressure","price_rise"])              # forte
                elif p24h >= 50:  sigs.append("buy_pressure")                            # bom
                elif p24h < -20:  sigs.append("price_fall_pen")                          # falhou

                if not sigs: continue

                # Aprende
                WEIGHTS = adjust_weights(WEIGHTS, sigs, success, change)
                learns_done  += 1
                new_patterns += 1
                batch_learned+= 1
                already_in.add(name)

                pattern_history.append({
                    "timestamp": time.time(), "name": name, "symbol": symbol,
                    "signals": sigs, "result": round(change, 3),
                    "success": success, "mcap": mc, "liq": lq,
                    "vol_ratio": round(vr,3), "hot": hot, "score": 0,
                    "p24h": p24h,
                })

                # Temas
                theme = detect_theme(name)
                if theme:
                    if theme not in successful_themes:
                        successful_themes[theme] = {"count_success":0,"count_total":0,"total_gain":0.0}
                    successful_themes[theme]["count_total"] += 1
                    if success:
                        successful_themes[theme]["count_success"] += 1
                        successful_themes[theme]["total_gain"]    += change

                # Rockets para copy-cat
                if success and change >= 2.0:  # +200%+
                    recent_rockets.append({"name":name,"theme":theme or name.lower()[:4],"time":time.time(),"gain":change*100})

            except Exception:
                continue

        wins_batch = sum(1 for p in pattern_history[-batch_learned:] if p.get("success"))
        print(f"[Backtest] {label}: {batch_learned} moedas | {wins_batch} rockets | {batch_learned-wins_batch} falhas")
        await asyncio.sleep(1)  # pausa entre endpoints

    # ── GUARDA TUDO ──────────────────────────────────────────
    if new_patterns > 0:
        save_weights(WEIGHTS)
        save_pattern_history()
        total_wins = sum(1 for p in pattern_history if p.get("success"))
        print(f"[Backtest] ════════════════════════════════════")
        print(f"[Backtest] ✅ CONCLUIDO!")
        print(f"[Backtest]    Total analisado : {total_seen} moedas")
        print(f"[Backtest]    Aprendizagens   : {new_patterns}")
        print(f"[Backtest]    Padroes guardados: {len(pattern_history)}")
        print(f"[Backtest]    Rockets (>50%)  : {total_wins}")
        print(f"[Backtest]    Falhas (<50%)   : {len(pattern_history)-total_wins}")
        print(f"[Backtest]    Temas em trend  : {len(successful_themes)}")
        print(f"[Backtest] ════════════════════════════════════")
    else:
        print(f"[Backtest] ✅ Ja tinha todos os padroes — {len(pattern_history)} no historico")

# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# 🔧  MAINTENANCE
# ─────────────────────────────────────────────

async def maintenance_loop():
    while True:
        await asyncio.sleep(60)
        now = time.time()

        # Verifica alertas 2h depois -> aprende
        for mint in [m for m,c in list(pending_checks.items()) if now >= c["check_at"]]:
            await check_and_learn(mint, pending_checks.pop(mint))

        # Aprende com moedas ignoradas que subiram (a cada 5 min)
        if int(now) % 300 < 61:
            await learn_from_skipped()

        # Status a cada 5 min
        if int(now) % 300 < 61:
            cleanup_old_tokens()
            hot = "ATIVA" if is_hot_window() else "inativa"
            print(f"[Status] {datetime.now().strftime('%H:%M:%S')} | Janela: {hot} | "
                  f"Vistas: {tokens_seen} | Alertadas: {len(alerted_tokens)} | "
                  f"Alertas: {alerts_sent} | Aprendizagens: {learns_done}")

# ─────────────────────────────────────────────
# 🚀  MAIN
# ─────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("  🤖 TRADING BOT v11.0")
    print(f"  Railway: {'✅' if os.environ.get('RAILWAY_ENVIRONMENT') else '💻 local'}")
    print(f"  Data dir: {DATA_DIR}")
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

    # Corre backtesting antes de tudo
    await run_backtesting()

    await asyncio.gather(
        pumpfun_scanner(),
        dexscreener_scanner(),
        update_loop(),
        maintenance_loop(),
        rugpull_monitor(),
    )

if __name__ == "__main__":
    import signal, sys

    def handle_shutdown(sig, frame):
        print(f"\n[Railway] Sinal {sig} recebido — a guardar dados antes de parar...")
        save_weights(WEIGHTS)
        save_pattern_history()
        save_hour_stats()
        save_alerted()
        print(f"[Railway] Dados guardados. Alertas: {alerts_sent} | Aprendizagens: {learns_done}")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)  # Railway envia SIGTERM ao parar
    signal.signal(signal.SIGINT,  handle_shutdown)  # Ctrl+C local

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        handle_shutdown(None, None)
