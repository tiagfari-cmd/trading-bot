# Trading Bot v11.5 - Auto-Scanner Solana
# Filtros: MCap $80K-$500K | Liq $12K-$70K | Vol>5%


import asyncio, csv, json, os, time, aiohttp, websockets
from datetime import datetime, timezone
from collections import deque

# ---------------------------------------------
#   CONFIGURAO
# ---------------------------------------------

BOT_VERSION  = "v11.6.0 - 27/02/2026"
# Muda este valor sempre que fizeres update para identificar a versao a correr

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "COLA_AQUI_O_TEU_WEBHOOK_URL")
RESULTS_WEBHOOK_URL = os.environ.get("RESULTS_WEBHOOK_URL", "")  # canal #resultados
HELIUS_API_KEY      = os.environ.get("HELIUS_API_KEY", "COLA_AQUI_A_TUA_HELIUS_KEY")

# -- RAILWAY API - para guardar aprendizagem como varivel de ambiente --
# Adiciona no Railway: RAILWAY_API_TOKEN (Settings -> Tokens -> New Token)
# e RAILWAY_SERVICE_ID (est no URL do teu servio)
RAILWAY_API_TOKEN   = os.environ.get("RAILWAY_API_TOKEN", "")
RAILWAY_SERVICE_ID  = os.environ.get("RAILWAY_SERVICE_ID", "")
RAILWAY_PROJECT_ID  = os.environ.get("RAILWAY_PROJECT_ID", "")
RAILWAY_ENV_ID      = os.environ.get("RAILWAY_ENVIRONMENT_ID", "")  # Railway injeta isto automaticamente

# -- RAILWAY: usa /tmp para ficheiros (sobrevive a reinicios) --
# Para persistncia total entre deploys, configura um Volume no Railway
# Settings -> Volumes -> Mount Path: /data
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
    "check_after":        3600,   # aprende 1h depois
    "monitor_for":       86400,   # monitoriza por 24h (1 dia completo)
    "success_threshold":   1.00,  # sucesso = +100% (objetivo real)
    "update_interval":    1200,    # 20 minutos
    "dexscreener_poll":     30,    # polling DexScreener
    # Updates por milestone em vez de tempo fixo
    "milestones":     [0.25, 0.50, 1.00, 2.00, 5.00],  # +25%, +50%, +100%, +200%, +500%
    "drop_alert_pct": -0.20,   # avisa se cair -20% desde o pico

    # -- NOVOS FILTROS v7 (baseados nos prints do GEM HUNTERS) --
    "mcap_min":        80_000,   # Market Cap mnimo $80K (protege Lupe $96K que fez +120%)
    "mcap_max":       500_000,   # Market Cap mximo $500K
    "liq_min":         12_000,   # Liquidez mnima $12K (baixado para apanhar moedas cedo)
    "liq_max":         70_000,   # Liquidez mxima $70K
    "vol_ratio_min":    0.05,    # Vol1H/Vol24H mnimo 5% (baixado para apanhar mais moedas)
}

HOT_WINDOW_START, HOT_WINDOW_END = 23, 3

# ---------------------------------------------
#   PESOS (APRENDIZAGEM)
# ---------------------------------------------

DEFAULT_WEIGHTS = {
    "hot_window":      20.0,
    "price_low":       15.0,
    "price_high_pen": -20.0,
    "buy_pressure":    25.0,
    "volume_spike":    25.0,
    "momentum":        20.0,
    "momentum_pen":   -25.0,
    "price_rise":      20.0,
    "price_fall_pen": -20.0,  # aumentado - queda de preo  sinal muito negativo
    "trade_freq":      10.0,
    # v7 - filtros mercado
    "mcap_good":       20.0,
    "mcap_bad":       -20.0,
    "liq_good":        15.0,
    "liq_growing":      8.0,   # liquidez baixa mas a crescer - moeda cedo
    "vol_abs_high":    12.0,  # volume absoluto 1h alto (>$100K)
    "vol_abs_mid":      6.0,  # volume absoluto 1h moderado (>$50K)
    "liq_bad":        -15.0,
    "vol_ratio_good":  20.0,
    # v8 - Helius
    "holders_good":    20.0,
    "holders_bad":    -20.0,
    "dev_sold":       -35.0,
    "dev_holds":       15.0,
    "concentration_ok": 15.0,
    "concentration_bad":-25.0,
    "buy_sell_5m":     25.0,
    # v10 - Backtesting + Temas + Whales + Hora
    "pattern_strong":   35.0,
    "pattern_weak":    -20.0,
    "theme_hot":        20.0,
    "whale_buy":        30.0,
    "whale_multi":      40.0,
    "rugpull_risk":    -50.0,
    "hot_hour":         15.0,   # hora historicamente boa
    "cold_hour":       -10.0,   # hora historicamente fraca
    # v11 - novas melhorias
    "holder_momentum":  35.0,   # holders a crescer rapido
    "copycat_bonus":    25.0,   # tema de moeda que explodiu recentemente
    "signal_consensus": 20.0,   # bonus quando 8+ sinais positivos concordam
    "anti_fomo":       -30.0,   # moeda ja subiu muito antes do bot a ver
    "stop_loss_risk":  -20.0,   # proximo do pior resultado historico deste padrao
    # Rugpull intelligence
    "bad_dev":         -60.0,   # dev ja fez rugpull antes - bloqueia quase sempre
    "bundle_buy":      -40.0,   # primeiras compras identicas = bot a simular procura
    "liq_price_diverge":-30.0, # preco sobe mas liquidez nao acompanha = red flag
}

def load_weights():
    """
    Carrega pesos por ordem de prioridade:
    1. Variavel de ambiente BOT_WEIGHTS (Railway - persiste entre deploys)
    2. Ficheiro local pesos.json (persiste entre reinicios)
    3. Pesos iniciais hardcoded
    """
    # Prioridade 1: varivel de ambiente (persiste entre deploys no Railway)
    env_weights = os.environ.get("BOT_WEIGHTS", "")
    if env_weights:
        try:
            saved = json.loads(env_weights)
            w = {**DEFAULT_WEIGHTS, **saved}
            print(f"[Aprendizagem] ? Pesos carregados da variavel BOT_WEIGHTS ({len(saved)} pesos)")
            return w
        except Exception as e:
            print(f"[Aprendizagem] ?? BOT_WEIGHTS invalido: {e}")

    # Prioridade 2: ficheiro local
    if os.path.exists(CONFIG["weights_file"]):
        try:
            saved = json.load(open(CONFIG["weights_file"]))
            w = {**DEFAULT_WEIGHTS, **saved}
            print(f"[Aprendizagem] ? Pesos carregados de {CONFIG['weights_file']}")
            return w
        except Exception: pass

    print("[Aprendizagem] Usando pesos iniciais (baseados em 11+ moedas reais)")
    return dict(DEFAULT_WEIGHTS)

# Controlo para no spammar a Railway API - s guarda se mudou significativamente
_last_railway_save = 0
_last_saved_weights = {}

async def save_weights_to_railway(w):
    """Guarda pesos como variavel de ambiente no Railway via GraphQL API."""
    global _last_railway_save, _last_saved_weights

    if not RAILWAY_API_TOKEN or not RAILWAY_SERVICE_ID or not RAILWAY_ENV_ID:
        return  # Railway API no configurada - falha silenciosa

    # S guarda se passaram 10 minutos desde o ltimo save
    now = time.time()
    if now - _last_railway_save < 600:
        return

    # S guarda se os pesos mudaram significativamente
    if _last_saved_weights:
        max_change = max(abs(w.get(k,0) - _last_saved_weights.get(k,0)) for k in w)
        if max_change < 0.5:
            return

    try:
        weights_json = json.dumps(w, separators=(',', ':'))
        # GraphQL mutation para upsert de varivel
        query = """
        mutation upsertVariables($input: VariableCollectionUpsertInput!) {
            variableCollectionUpsert(input: $input)
        }
        """
        variables = {
            "input": {
                "projectId":     RAILWAY_PROJECT_ID,
                "environmentId": RAILWAY_ENV_ID,
                "serviceId":     RAILWAY_SERVICE_ID,
                "variables":     {"BOT_WEIGHTS": weights_json}
            }
        }
        headers = {
            "Authorization": f"Bearer {RAILWAY_API_TOKEN}",
            "Content-Type":  "application/json",
        }
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://backboard.railway.com/graphql/v2",
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    _last_railway_save   = now
                    _last_saved_weights  = dict(w)
                    print(f"[Railway] ? Pesos guardados na variavel BOT_WEIGHTS")
                else:
                    print(f"[Railway] ?? Erro ao guardar pesos: {r.status}")
    except Exception as e:
        print(f"[Railway] ?? Erro API: {e}")

def save_weights(w):
    """Guarda pesos no ficheiro local (sempre) e agenda save no Railway."""
    json.dump(w, open(CONFIG["weights_file"], "w"), indent=2)
    # Agenda save no Railway de forma assncrona sem bloquear
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(save_weights_to_railway(w))
    except Exception:
        pass  # fora de contexto async - s guarda no ficheiro

def adjust_weights(weights, active_signals, success, change_pct=0):
    """
    Aprendizagem inteligente - ajusta pesos proporcionalmente ao resultado.
    
    Sucesso grande (>100%): reforca muito os sinais ativos
    Sucesso medio (50-100%): reforca moderadamente
    Falha ligeira (0-50%): penaliza ligeiramente
    Falha grande (<0%): penaliza muito os sinais ativos
    """
    if success:
        # Quanto mais subiu, mais aprende
        if change_pct >= 2.0:   factor = 0.15   # +200% -> aprende muito
        elif change_pct >= 1.0: factor = 0.10   # +100% -> aprende bem
        else:                   factor = 0.07   # +50%  -> aprende normal
    else:
        # Quanto mais caiu, mais penaliza
        if change_pct <= -0.3:  factor = -0.12  # caiu >30% -> penaliza muito
        elif change_pct <= 0:   factor = -0.07  # no subiu -> penaliza normal
        else:                   factor = -0.04  # subiu mas <50% -> penaliza pouco

    for sig in active_signals:
        if sig not in weights: continue
        cur   = weights[sig]
        delta = cur * factor
        weights[sig] = max(5.0, min(40.0, cur+delta)) if cur > 0 else max(-40.0, min(-5.0, cur+delta))
    return weights

WEIGHTS        = load_weights()
token_data     = {}
# Carrega tokens j alertados de ficheiro para persistir entre reinicios
ALERTED_FILE = f"{DATA_DIR}/alertados.json"
def load_alerted():
    if os.path.exists(ALERTED_FILE):
        try: return set(json.load(open(ALERTED_FILE)))
        except: pass
    return set()
def save_alerted():
    json.dump(list(alerted_tokens), open(ALERTED_FILE, "w"))

alerted_tokens = load_alerted()  # <- NUNCA repete, mesmo aps reiniciar

# -- BLACKLIST - moedas j conhecidas/que j explodiram --------
# Estas moedas nunca sero alertadas (j atingiram o seu pico)
BLACKLIST = {
    # 11 moedas analisadas - j explodiram
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
    # Adiciona aqui mais moedas que j explodiram
}
tokens_seen    = 0
alerts_sent    = 0
learns_done    = 0
pending_checks = {}

# -- APRENDIZAGEM AVANADA -------------------------------------
# Moedas que o bot viu mas ignorou (para aprender com oportunidades perdidas)
skipped_coins  = {}   # mint -> {price, time, signals_at_skip, score}
SKIPPED_MAX    = 200  # mximo de moedas ignoradas em memria

SKIP_CHECK_AFTER  = 3600  # verifica 1h depois se subiu


# -- ESTADO DO BOT - snapshot para recuperao aps crash -----
STATE_FILE       = f"{DATA_DIR}/bot_state.json"
bot_start_time   = time.time()   # quando o bot arrancou esta sesso
last_state_save  = 0             # ultimo snapshot de estado

async def save_state_cloud():
    """Guarda estado no JSONBin - persiste entre deploys."""
    if not JSONBIN_KEY or not JSONBIN_ID: return
    try:
        state = {
            "pattern_history": pattern_history[-500:],  # max 500 padroes
            "weights":         WEIGHTS,
            "alerted_tokens":  list(alerted_tokens)[-200:],
            "learns_done":     learns_done,
            "alerts_sent":     alerts_sent,
            "saved_at":        time.time()
        }
        async with aiohttp.ClientSession() as s:
            await s.put(
                JSONBIN_URL,
                json=state,
                headers={"X-Master-Key": JSONBIN_KEY, "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10)
            )
    except Exception as e:
        print(f"[JSONBin] Erro ao guardar: {e}")

async def load_state_cloud():
    """Carrega estado do JSONBin ao arrancar."""
    global pattern_history, WEIGHTS, alerted_tokens, learns_done, alerts_sent
    if not JSONBIN_KEY or not JSONBIN_ID: return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                JSONBIN_URL,
                headers={"X-Master-Key": JSONBIN_KEY},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200: return False
                data = (await r.json()).get("record", {})
                if not data or data.get("state") == "init": return False

                if data.get("pattern_history"):
                    pattern_history = data["pattern_history"]
                if data.get("weights"):
                    WEIGHTS = data["weights"]
                if data.get("alerted_tokens"):
                    alerted_tokens.update(data["alerted_tokens"])
                if data.get("learns_done"):
                    learns_done  = data["learns_done"]
                if data.get("alerts_sent"):
                    alerts_sent  = data["alerts_sent"]
                print(f"[JSONBin] Estado carregado: {len(pattern_history)} padroes, {learns_done} aprendizagens")
                return True
    except Exception as e:
        print(f"[JSONBin] Erro ao carregar: {e}")
        return False

def save_state():
    """Guarda estado critico em disco a cada 60s - recupera apos crash."""
    try:
        state = {
            "saved_at":       time.time(),
            "alerts_sent":    alerts_sent,
            "learns_done":    learns_done,
            "tokens_seen":    tokens_seen,
            "alerted_count":  len(alerted_tokens),
            "pending_count":  len(pending_checks),
            "patterns_count": len(pattern_history),
        }
        json.dump(state, open(STATE_FILE, "w"))
    except Exception as e:
        print(f"[Estado] Erro ao guardar: {e}")

def load_last_state():
    """Carrega estado anterior para saber quanto tempo esteve em baixo."""
    try:
        if os.path.exists(STATE_FILE):
            return json.load(open(STATE_FILE))
    except: pass
    return None

# -- RUGPULL INTELLIGENCE -------------------------------------
# Carteiras de devs conhecidos por fazer rugpull
# O bot vai expandindo esta lista automaticamente
BAD_DEV_WALLETS  = set()   # carteiras de devs que fizeram rugpull
bad_dev_file     = f"{DATA_DIR}/bad_devs.json"

def load_bad_devs():
    if os.path.exists(bad_dev_file):
        try: return set(json.load(open(bad_dev_file)))
        except: pass
    return set()

def save_bad_devs():
    json.dump(list(BAD_DEV_WALLETS), open(bad_dev_file, "w"))

BAD_DEV_WALLETS = load_bad_devs()

# Regista o dev de cada token alertado
token_dev_wallet = {}   # mint -> dev_wallet
# Regista primeiras compras de cada token (para bundle detection)
token_early_buys = {}   # mint -> [sol_amounts das primeiras 10 compras]

# -- SILENT TRACKS - aprende fora da janela sem alertar --------
# Moedas que passaram os filtros fora da janela 23h-03h
# O bot nao alerta mas aprende com o resultado
silent_tracks    = {}   # mint -> {price, time, active, check_at, ...}

# -- BACKTESTING + PATTERN HISTORY --------------------
BACKTEST_FILE   = f"{DATA_DIR}/backtest_history.json"
pattern_history = []   # [{signals, result, mcap, liq, vol_ratio, hot, timestamp}]
MAX_HISTORY     = 2000 # mximo de padres guardados

def load_pattern_history():
    if os.path.exists(BACKTEST_FILE):
        try:
            data = json.load(open(BACKTEST_FILE))
            print(f"[Backtest] Carregados {len(data)} padroes historicos")
            return data
        except: pass
    return []

def save_pattern_history():
    json.dump(pattern_history[-MAX_HISTORY:], open(BACKTEST_FILE, "w"))

pattern_history = load_pattern_history()  # carrega DEPOIS de definir a funo

# -- CORRELAO DE TEMAS -------------------------------
successful_themes = {}  # tema -> {count_success, count_total, avg_gain}
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

# -- VOLUME POR CARTEIRA -------------------------------
wallet_volumes = {}  # mint -> {wallet: total_sol}

# -- MOMENTUM DE HOLDERS --------------------------------------
# Regista quantos holders novos por minuto para detetar explosoes
holder_timeline  = {}   # mint -> [(timestamp, holder_count), ...]

# -- COPY-CATS ------------------------------------------------
# Moedas que explodiram recentemente - para detetar copy-cats
recent_rockets   = []   # [{name, theme, symbol, time, gain}]
MAX_ROCKETS      = 30

# -- ANTI-FOMO -------------------------------------------------
# Penaliza moedas que JA subiram muito antes do bot as detetar
# (o bot esta a chegar tarde)

# -- CONFIANCA ACUMULADA ---------------------------------------
# Bnus quando muitos sinais concordam na mesma direcao

# -- HORA EXATA DO PICO --------------------------------
# Aprende em que hora do dia as moedas tendem a picar mais
# hour_stats[hora] = {wins, total, avg_gain}
HOUR_STATS_FILE = f"{DATA_DIR}/hour_stats.json"
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

hour_stats = load_hour_stats()  # carrega DEPOIS de definir a funo

# ---------------------------------------------
#   TOKEN DATA
# ---------------------------------------------

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

# ---------------------------------------------
#   FILTROS DE MARKET CAP / LIQUIDITY / VOL
# ---------------------------------------------

def check_mcap_liq_vol(mint) -> tuple:
    """
    Verifica Market Cap, Liquidity e ratio Vol1H/24H.
    Retorna (passou_filtro, pontos, sinais, active_signals)
    Baseado nos padroes do GEM HUNTERS AI:
      Sweet spot mcap: $100K-$500K
      Sweet spot liq:  $25K-$65K
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

    # -- MARKET CAP ------------------------------
    if mcap > 0:
        if CONFIG["mcap_min"] <= mcap <= CONFIG["mcap_max"]:
            pts = int(w["mcap_good"]); score += pts
            signals.append(f"💎 Market Cap ${mcap/1000:.0f}K - sweet spot (+{pts}pts)")
            active.append("mcap_good")
        elif mcap > CONFIG["mcap_max"]:
            score += int(w["mcap_bad"])
            signals.append(f"⚠️ Market Cap ${mcap/1000:.0f}K - muito alto ({w['mcap_bad']:.0f}pts)")
            active.append("mcap_bad")
            if mcap > 800_000:
                passed = False  # bloqueia completamente acima de $800K
        else:
            signals.append(f"?? Market Cap ${mcap/1000:.0f}K - muito baixo (cuidado)")

    # -- LIQUIDITY -------------------------------
    # Bloqueia moedas sem liquidez ou demasiado baixa ($2K nao chega para mover nada)
    if liq < CONFIG["liq_min"]:
        motivo = "N/A" if liq <= 0 else f"${liq/1000:.1f}K (minimo ${CONFIG['liq_min']//1000}K)"
        return {
            "score": 0,
            "signals": [f"Bloqueado liquidez {motivo}"],
            "verdict": "BLOQUEADO",
            "category": "FRACO",
            "active_signals": [],
            "blocked": True
        }
    if liq > 0:
        if 25_000 <= liq <= CONFIG["liq_max"]:
            # Sweet spot ideal $25K-$65K
            pts = int(w["liq_good"]); score += pts
            signals.append(f"💧 Liquidez ${liq/1000:.0f}K - ideal (+{pts}pts)")
            active.append("liq_good")
        elif CONFIG["liq_min"] <= liq < 25_000:
            # Zona cedo $12K-$25K - liquidez baixa mas moeda nova com potencial
            # Precisa de compensar com outros sinais fortes
            pts = int(w.get("liq_growing", 8)); score += pts
            signals.append(f"💧 Liquidez ${liq/1000:.0f}K - cedo, a crescer (+{pts}pts) ??")
            active.append("liq_growing")
        elif liq > CONFIG["liq_max"]:
            score += int(w["liq_bad"])
            signals.append(f"💧 Liquidez ${liq/1000:.0f}K - alta, ja comprado ({w['liq_bad']:.0f}pts)")
            active.append("liq_bad")
        else:
            # Abaixo de $12K - muito arriscado
            score += int(w["liq_bad"])
            signals.append(f"💧 Liquidez ${liq/1000:.0f}K - muito baixa, risco alto ({w['liq_bad']:.0f}pts)")
            active.append("liq_bad")

    # -- RATIO VOL 1H / 24H ----------------------
    if vol_24h > 0 and vol_1h > 0:
        ratio = vol_1h / vol_24h
        if ratio >= 0.30:
            # Volume muito alto - moeda a explodir agora
            pts = int(w["vol_ratio_good"]) + 10; score += pts
            signals.append(f"📈 Vol1H/24H: {ratio*100:.0f}% - momentum explosivo (+{pts}pts)")
            active.append("vol_ratio_good"); active.append("volume_spike")
        elif ratio >= 0.15:
            # Volume alto - bom sinal
            pts = int(w["vol_ratio_good"]); score += pts
            signals.append(f"📈 Vol1H/24H: {ratio*100:.0f}% - momentum forte (+{pts}pts)")
            active.append("vol_ratio_good")
        elif ratio >= CONFIG["vol_ratio_min"]:
            # Volume moderado - passa mas com menos pontos
            pts = int(w["vol_ratio_good"]) // 2; score += pts
            signals.append(f"📈 Vol1H/24H: {ratio*100:.0f}% - momentum moderado (+{pts}pts)")
            active.append("vol_ratio_good")
        else:
            signals.append(f"📉 Vol1H/24H: {ratio*100:.0f}% - momentum fraco")

    return passed, score, signals, active

# ---------------------------------------------
#   MOTOR DE CONFIANA
# ---------------------------------------------

def calculate_confidence(mint):
    d = token_data.get(mint)
    if not d or len(d["trades"]) < 3:
        return {"score": 0, "signals": [], "verdict": "SEM DADOS", "category": None,
                "active_signals": [], "blocked": False}
    if check_micropump(mint):
        return {"score": 0, "signals": ["⚠️ MICRO-PUMP"], "verdict": "⚠️ MICRO-PUMP",
                "category": "MICROPUMP", "active_signals": [], "blocked": False}

    signals, active, score = [], [], 0
    w       = WEIGHTS
    prices  = list(d["prices"])
    volumes = list(d["volumes"])
    trades  = list(d["trades"])
    in_hot  = is_hot_window()

    # 1. Janela horaria - apenas informativo, nao afeta score
    if in_hot:
        signals.append("🌙 Janela 23h-03h")
    else:
        signals.append("🕐 Fora da janela 23h-03h")

    # 2. Preo
    ap = d.get("alert_price") or (prices[-1] if prices else 0)
    if ap > 0:
        if ap < 0.00030:
            pts = int(w["price_low"]); score += pts
            signals.append(f"💰 Preco baixo ${ap:.7f} (+{pts}pts)"); active.append("price_low")
        elif ap < 0.00050:
            signals.append(f"💰 Preco medio ${ap:.7f}")
        elif not in_hot:
            score += int(w["price_high_pen"])
            signals.append(f"⚠️ Preco alto ${ap:.7f} ({w['price_high_pen']:.0f}pts)"); active.append("price_high_pen")
        else:
            signals.append(f"⚠️ Preco alto ${ap:.7f} na janela (neutro)")

    # 3. Market Cap + Liquidity + Vol ratio (NOVOS FILTROS v7)
    # -- FILTRO DE MOMENTUM - 3 janelas (5m + 1h + 6h) --------
    # Bloqueia tokens permanentes (nao sao memecoins)
    p5m  = d.get("p5m",  0)
    p1h  = d.get("p1h",  0)
    p6h  = d.get("p6h",  0)
    p24h = d.get("p24h", 0)

    # BLOQUEIO 1 - caindo em 5m E fraca em 1h = sem momentum
    if p5m < -5 and p1h < 2:
        return {
            "score": 0,
            "signals": [f"Bloqueado - caindo (5m:{p5m:.1f}% 1h:{p1h:.1f}%)"],
            "verdict": "BLOQUEADO", "category": "FRACO",
            "active_signals": [], "blocked": True
        }

    # BLOQUEIO 1b - momentum negativo
    # 24h tem de ser positivo, 5m tem de ser positivo, 1h so bloqueia se < -10%
    p5m_raw = d.get("p5m", 0)  # definido aqui para uso nos bloqueios
    if p24h < 0:
        return {"score": 0,
                "signals": [f"Bloqueado - 24h negativo ({p24h:.1f}%)"],
                "verdict": "BLOQUEADO", "category": "FRACO",
                "active_signals": [], "in_hot": in_hot, "blocked": True}
    if p5m_raw <= 0:
        return {"score": 0,
                "signals": [f"Bloqueado - 5m negativo ou zero ({p5m_raw:.1f}%)"],
                "verdict": "BLOQUEADO", "category": "FRACO",
                "active_signals": [], "in_hot": in_hot, "blocked": True}
    if p1h < -10:
        return {"score": 0,
                "signals": [f"Bloqueado - 1h em queda forte ({p1h:.1f}%)"],
                "verdict": "BLOQUEADO", "category": "FRACO",
                "active_signals": [], "in_hot": in_hot, "blocked": True}

    # BLOQUEIO 2 - estagnada: 1h quase plana E 5m negativo ou zero
    if -5 <= p1h <= 5 and p5m <= 0:
        return {
            "score": 0,
            "signals": [f"Bloqueado - estagnada (5m:{p5m:.1f}% 1h:{p1h:.1f}%)"],
            "verdict": "BLOQUEADO", "category": "FRACO",
            "active_signals": [], "blocked": True
        }

    # BLOQUEIO 2b - moeda velha sem novo impulso: subiu muito em 24h mas agora fraca
    # Exemplo: SOL +74% 24h mas apenas +8% 1h e +1% 5m = pump ja acabou
    p5m_raw = d.get("p5m", 0)
    if p24h > 50 and p1h < 10 and p5m_raw < 3:
        return {"score": 0,
                "signals": [f"Bloqueado - pump antigo sem novo impulso (24h:+{p24h:.0f}% mas 1h:{p1h:.1f}% 5m:{p5m_raw:.1f}%)"],
                "verdict": "BLOQUEADO", "category": "FRACO",
                "active_signals": [], "in_hot": False}

    # BLOQUEIO 3 - ja bateu o topo: subiu muito em 6h mas agora fraca em 1h
    # Ex: +300% em 6h mas so +3% em 1h = pump ja aconteceu
    if p6h >= 200 and p1h < 20:
        return {
            "score": 0,
            "signals": [f"Bloqueado - topo atingido sem forca (6h:+{p6h:.0f}% e 1h:{p1h:.1f}%<20%)"],
            "verdict": "BLOQUEADO", "category": "FRACO",
            "active_signals": [], "blocked": True
        }

    # BLOQUEIO 4 - pump muito antigo: subiu muito em 24h mas 6h e 1h fracas
    if p24h >= 500 and p6h < 10 and p1h < 5:
        return {
            "score": 0,
            "signals": [f"Bloqueado - pump antigo (24h:+{p24h:.0f}% mas 6h:{p6h:.1f}% 1h:{p1h:.1f}%)"],
            "verdict": "BLOQUEADO", "category": "FRACO",
            "active_signals": [], "blocked": True
        }

    passed, mcap_score, mcap_signals, mcap_active = check_mcap_liq_vol(mint)
    if not passed:
        return {"score": 0, "signals": [f"🚫 Bloqueado - Market Cap demasiado alto"],
                "verdict": "🚫 BLOQUEADO", "category": "FRACO",
                "active_signals": [], "blocked": True}
    score += mcap_score
    signals.extend(mcap_signals)
    active.extend(mcap_active)

    # 4. Presso de compra
    total = d["buy_count"] + d["sell_count"]
    if total > 0:
        br = d["buy_count"] / total
        if br >= 0.65:
            pts = int(br * w["buy_pressure"]); score += pts
            signals.append(f"🟢 Compra: {br*100:.0f}% buys (+{pts}pts)"); active.append("buy_pressure")
        elif br < 0.35:
            score -= 15; signals.append(f"🔴 Venda: {br*100:.0f}% buys (-15pts)")

    # 5. Spike de volume
    if len(volumes) >= 4:
        avg = sum(list(volumes)[:-2]) / max(len(volumes)-2, 1); lat = volumes[-1]
        if avg > 0 and lat > avg * CONFIG["volume_spike_x"]:
            pts = min(int(w["volume_spike"]), int((lat/avg)*7)); score += pts
            signals.append(f"⚡ Volume spike: {lat/avg:.1f}x (+{pts}pts)"); active.append("volume_spike")

    # 6. Momentum
    now = time.time(); tl = d["volume_timeline"]; mw = CONFIG["momentum_window"]
    rec = [v for t, v in tl if now-t <= mw]; old = [v for t, v in tl if mw < now-t <= mw*2]
    if len(rec) >= 2 and len(old) >= 1:
        ar, ao = sum(rec)/len(rec), sum(old)/len(old)
        if ao > 0:
            ratio = ar / ao
            if ratio >= 1.5:
                pts = min(int(w["momentum"]), int(ratio*8)); score += pts
                signals.append(f"📈 Momentum: {ratio:.1f}x (+{pts}pts)"); active.append("momentum")
            elif ratio < 0.6:
                score += int(w["momentum_pen"])
                signals.append(f"📉 Momentum a cair ({w['momentum_pen']:.0f}pts)"); active.append("momentum_pen")

    # 7. Tendncia de preo - bloqueia moedas em queda
    if len(prices) >= 2 and prices[0] > 0:
        # Tendncia geral desde que o bot comeou a ver a moeda
        trend_overall = (prices[-1] - prices[0]) / prices[0]
    else:
        trend_overall = 0

    if len(prices) >= 4 and prices[-4] > 0:
        # Tendncia recente (ltimas 4 observaes)
        trend_recent = (prices[-1] - prices[-4]) / prices[-4]
    else:
        trend_recent = 0

    # BLOQUEIO DURO - moeda em queda clara no passa
    if trend_recent <= -0.10 and trend_overall <= -0.05:
        # Caindo -10% recentemente E tendncia geral negativa = bloqueia
        return {
            "score": 0,
            "signals": [f"🚫 Bloqueado - preco em queda ({trend_recent*100:.1f}% recente, {trend_overall*100:.1f}% geral)"],
            "verdict": "🚫 BLOQUEADO",
            "category": "FRACO",
            "active_signals": [],
            "blocked": True
        }
    elif trend_recent >= 0.05:
        # Subida recente - bom sinal
        pts = min(int(w["price_rise"]), int(trend_recent*150)); score += pts
        signals.append(f"📈 Preco +{trend_recent*100:.1f}% (+{pts}pts)"); active.append("price_rise")
    elif trend_recent <= -0.05:
        # Queda leve - penaliza mas no bloqueia
        score += int(w["price_fall_pen"])
        signals.append(f"?? Preco {trend_recent*100:.1f}% ({w['price_fall_pen']:.0f}pts)"); active.append("price_fall_pen")

    # 8. Frequncia de trades
    rt = [t["time"] for t in trades[-10:]]
    if len(rt) >= 2:
        dur = rt[-1] - rt[0]
        if dur > 0:
            tps = len(rt) / dur
            if tps > 0.5:
                pts = min(int(w["trade_freq"]), int(tps*8)); score += pts
                signals.append(f"⚡ Freq: {tps:.2f}/s (+{pts}pts)"); active.append("trade_freq")

    # 8a-extra. Padro histrico - baseado em backtesting
    if len(pattern_history) >= 20:
        # Encontra padres similares no histrico
        similar = [p for p in pattern_history if (
            abs(p.get("mcap",0) - d.get("market_cap",0)) < 100000 and
            abs(p.get("vol_ratio",0) - (d.get("vol_1h",0)/max(d.get("vol_24h",1),1))) < 0.10 and
            p.get("hot") == in_hot
        )]
        if len(similar) >= 5:
            wins_p  = sum(1 for p in similar if p.get("result",0) >= 1.0)  # sucesso = +100%
            win_rate = wins_p / len(similar)
            avg_gain = sum(p.get("result",0) for p in similar) / len(similar)
            if win_rate >= 0.70:
                pts = int(w.get("pattern_strong", 35)); score += pts
                signals.append(f"🎯 Padrao historico: {win_rate*100:.0f}% acerto em {len(similar)} casos (+{pts}pts)")
                active.append("pattern_strong")
                d["pattern_win_rate"] = win_rate
                d["pattern_avg_gain"] = avg_gain
                d["pattern_count"]    = len(similar)
            elif win_rate < 0.40 and len(similar) >= 10:
                score += int(w.get("pattern_weak", -20))
                signals.append(f"?? Padrao fraco: so {win_rate*100:.0f}% acerto ({w.get('pattern_weak',-20):.0f}pts)")
                active.append("pattern_weak")

    # 8a-extra2. Tema em trend
    theme = detect_theme(d.get("name",""))
    if theme and theme in successful_themes:
        th = successful_themes[theme]
        if th.get("count_total",0) >= 3 and th.get("count_success",0)/th.get("count_total",1) >= 0.60:
            pts = int(w.get("theme_hot", 20)); score += pts
            signals.append(f"🔥 Tema '{theme}' em trend - {th['count_success']}/{th['count_total']} subiram (+{pts}pts)")
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
            signals.append(f"• Hora {current_hour}h historicamente forte ({h_rate*100:.0f}% acerto) (+{pts}pts)")
            active.append("hot_hour")
        elif h_rate < 0.35:
            score += int(w.get("cold_hour", -10))
            signals.append(f"• Hora {current_hour}h historicamente fraca ({h_rate*100:.0f}% acerto) ({w.get('cold_hour',-10):.0f}pts)")
            active.append("cold_hour")

    # -- NOVAS MELHORIAS v11 ----------------------------------

    # A. ANTI-FOMO - se a moeda ja subiu muito, o bot chegou tarde
    if len(prices) >= 2 and prices[0] > 0:
        already_up = (prices[-1] - prices[0]) / prices[0]
        if already_up >= 1.5:  # ja subiu +150% antes do bot a detetar
            score += int(w.get("anti_fomo", -30))
            signals.append(f"• Anti-fomo: ja subiu +{already_up*100:.0f}% antes do alerta ({w.get('anti_fomo',-30):.0f}pts)")
            active.append("anti_fomo")
        elif already_up >= 0.8:
            signals.append(f"?? Ja subiu +{already_up*100:.0f}% - verifica se ainda tem espaco")

    # B. MOMENTUM DE HOLDERS - velocidade de novos holders
    ht = holder_timeline.get(mint, [])
    if len(ht) >= 2:
        t1, h1 = ht[0]; t2, h2 = ht[-1]
        elapsed_min = max((t2 - t1) / 60, 0.5)
        holders_per_min = (h2 - h1) / elapsed_min
        if holders_per_min >= 10:
            pts = int(w.get("holder_momentum", 35)); score += pts
            signals.append(f"• Holders: +{holders_per_min:.0f}/min - a explodir (+{pts}pts)")
            active.append("holder_momentum")
        elif holders_per_min >= 3:
            pts = int(w.get("holder_momentum", 35)) // 2; score += pts
            signals.append(f"• Holders: +{holders_per_min:.1f}/min - a crescer (+{pts}pts)")
            active.append("holder_momentum")

    # C. COPY-CAT - tema de moeda que explodiu recentemente
    name_lower = d.get("name", "").lower()
    for rocket in recent_rockets[-10:]:  # so verifica os 10 mais recentes
        if time.time() - rocket["time"] > 3600: continue  # so conta se foi na ultima 1h
        rocket_theme = rocket.get("theme")
        if not rocket_theme: continue
        if rocket_theme in name_lower or any(kw in name_lower for kw in rocket_theme.split()):
            pts = int(w.get("copycat_bonus", 25)); score += pts
            signals.append(f"⚠️ Copy-cat de {rocket['name']} (+{rocket['gain']:.0f}%) - tema '{rocket_theme}' (+{pts}pts)")
            active.append("copycat_bonus")
            break

    # D. CONFIANCA ACUMULADA - bonus quando muitos sinais positivos concordam
    positive_signals = [s for s in active if not s.endswith("_pen") and s not in
                        ("anti_fomo","pattern_weak","holders_bad","dev_sold",
                         "concentration_bad","momentum_pen","price_high_pen","cold_hour","stop_loss_risk")]
    if len(positive_signals) >= 8:
        pts = int(w.get("signal_consensus", 20)); score += pts
        signals.append(f"• Consenso: {len(positive_signals)} sinais positivos concordam (+{pts}pts)")
        active.append("signal_consensus")
    elif len(positive_signals) >= 6:
        pts = int(w.get("signal_consensus", 20)) // 2; score += pts
        signals.append(f"• Consenso: {len(positive_signals)} sinais positivos (+{pts}pts)")
        active.append("signal_consensus")

    # F. BAD DEV - carteira do dev j fez rugpull antes
    dev_wallet = token_dev_wallet.get(mint, "")
    if dev_wallet and dev_wallet in BAD_DEV_WALLETS:
        score += int(w.get("bad_dev", -60))
        signals.append(f"• Dev ja fez rugpull antes! ({w.get('bad_dev',-60):.0f}pts)")
        active.append("bad_dev")
        # Se dev  conhecido por rugpull, bloqueia diretamente
        return {
            "score": 0,
            "signals": [f"🚫 BLOQUEADO - dev desta carteira ja fez rugpull ({dev_wallet[:16]}...)"],
            "verdict": "🚫 BLOQUEADO",
            "category": "FRACO",
            "active_signals": [],
            "blocked": True
        }

    # G. BUNDLE BUY DETECTION - primeiras compras muito parecidas = bot
    early = token_early_buys.get(mint, [])
    if len(early) >= 6:
        avg_buy = sum(early) / len(early)
        if avg_buy > 0:
            # Coeficiente de variao - quanto mais baixo mais uniformes so as compras
            variance  = sum((x - avg_buy)**2 for x in early) / len(early)
            std_dev   = variance ** 0.5
            cv        = std_dev / avg_buy  # 0 = todas iguais, 1+ = muito variadas
            if cv < 0.15:  # compras quase todas iguais = bot
                score += int(w.get("bundle_buy", -40))
                signals.append(f"• Bundle buy detetado - {len(early)} compras identicas (cv={cv:.2f}) ({w.get('bundle_buy',-40):.0f}pts)")
                active.append("bundle_buy")

    # H. LIQUIDEZ vs PREO DIVERGNCIA - preo sobe mas liquidez no acompanha
    if len(prices) >= 3:
        price_change = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0
        liq_now      = d.get("liquidity", 0)
        liq_start    = d.get("liq_at_start", liq_now)
        liq_change   = (liq_now - liq_start) / liq_start if liq_start > 0 else 0
        # Preo subiu muito mas liquidez ficou igual ou caiu = red flag
        if price_change >= 0.30 and liq_change < 0.05:
            score += int(w.get("liq_price_diverge", -30))
            signals.append(f"?? Preco +{price_change*100:.0f}% mas liquidez nao acompanha ({w.get('liq_price_diverge',-30):.0f}pts)")
            active.append("liq_price_diverge")

    # E. STOP-LOSS RISK - proximo do pior resultado historico deste padrao
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
                    signals.append(f"?? Risco: padrao similar caiu ate -{abs(worst)*100:.0f}% no pior caso ({w.get('stop_loss_risk',-20):.0f}pts)")
                    active.append("stop_loss_risk")

    # 8c. Helius - holders, dev, concentrao
    holders   = d.get("holders", 0)
    top10_pct = d.get("top10_pct", 0)
    dev_holds = d.get("dev_still_holds", None)

    # Dev suspeito (muitas transaes = serial creator)
    if d.get("dev_suspicious"):
        score -= 25
        signals.append("?? Dev suspeito - historico de muitas transacoes (-25pts)")

    if holders > 0:
        # Bloqueia moedas com pouquissimos holders - rugpull quase certo
        if holders < 10:
            return {
                "score": 0,
                "signals": [f"Bloqueado - apenas {holders} holders (concentracao extrema)"],
                "verdict": "BLOQUEADO",
                "category": "FRACO",
                "active_signals": [],
                "blocked": True
            }
        if holders >= 100:
            pts = int(w["holders_good"]); score += pts
            signals.append(f"👥 Holders: {holders} - seguro (+{pts}pts)"); active.append("holders_good")
        elif holders < 50:
            score += int(w["holders_bad"])
            signals.append(f"👥 Holders: {holders} - risco rugpull ({w['holders_bad']:.0f}pts)"); active.append("holders_bad")

    if top10_pct > 0:
        if top10_pct <= 30:
            pts = int(w["concentration_ok"]); score += pts
            signals.append(f"• Top10: {top10_pct:.0f}% - concentracao saudavel (+{pts}pts)"); active.append("concentration_ok")
        elif top10_pct > 50:
            score += int(w["concentration_bad"])
            signals.append(f"• Top10: {top10_pct:.0f}% - muito concentrado ({w['concentration_bad']:.0f}pts)"); active.append("concentration_bad")

    if dev_holds is not None:
        if dev_holds:
            pts = int(w["dev_holds"]); score += pts
            signals.append(f"??? Dev ainda tem tokens (+{pts}pts)"); active.append("dev_holds")
        else:
            score += int(w["dev_sold"])
            signals.append(f"??? Dev ja vendeu ({w['dev_sold']:.0f}pts) ??"); active.append("dev_sold")

    score = max(0, min(100, score))
    if   score >= 70:            cat, v = "ROCKET", "🚀 ROCKET - alto potencial"
    elif score >= 55:             cat, v = "BOM",    "✅ BOM SINAL - potencial moderado"
    elif score >= 35:             cat, v = "FRACO",  "? FRACO - ignorado"
    else:                         cat, v = "FRACO",  "? SEM SINAL"
    return {"score": score, "signals": signals, "verdict": v, "category": cat,
            "active_signals": active, "blocked": False}

# ---------------------------------------------
#   DEXSCREENER - buscar market cap, liq, vol
# ---------------------------------------------

async def fetch_dexscreener(mint):
    """Busca dados de Market Cap, Liquidity e Volume do DexScreener."""
    for attempt in range(2):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status == 429:
                        await asyncio.sleep(2)
                        continue
                    if r.status != 200:
                        return None
                    data  = await r.json()
                    pairs = data.get("pairs") or []
                    if not pairs: return None
                    p   = pairs[0]
                    chg = p.get("priceChange") or {}
                    return {
                        "price":      float(p.get("priceUsd") or 0),
                        "market_cap": float(p.get("fdv") or p.get("marketCap") or 0),
                        "liquidity":  float((p.get("liquidity") or {}).get("usd") or 0),
                        "vol_1h":     float((p.get("volume") or {}).get("h1") or 0),
                        "vol_24h":    float((p.get("volume") or {}).get("h24") or 0),
                        "name":       p.get("baseToken", {}).get("name", ""),
                        "symbol":     p.get("baseToken", {}).get("symbol", ""),
                        "p5m":        float(chg.get("m5") or 0),
                        "p1h":        float(chg.get("h1") or 0),
                        "p6h":        float(chg.get("h6") or 0),
                        "p24h":       float(chg.get("h24") or 0),
                    }
        except Exception:
            if attempt == 0:
                await asyncio.sleep(1)
            continue
    return None

async def fetch_solscan(mint):
    """Busca dados de holders do Solscan - gratuito, sem chave API."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://api.solscan.io/v2/token/holders?tokenAddress={mint}&limit=10&offset=0",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=6)
            ) as r:
                if r.status != 200: return None
                data = await r.json()
                total = data.get("data", {}).get("total", 0)
                if total:
                    return {"holders": int(total)}
    except Exception:
        pass
    # Fallback: Jupiter API para dados basicos
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://price.jup.ag/v6/price?ids={mint}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    return {"holders": 0}  # Jupiter nao tem holders mas confirma existencia
    except Exception:
        pass
    return None

async def enrich_token_from_dex(mint):
    """Atualiza o token com dados do DexScreener e Birdeye."""
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
    # Guarda variacao de preco para filtro de momentum
    d["p5m"]  = data.get("p5m",  0)
    d["p1h"]  = data.get("p1h",  0)
    d["p6h"]  = data.get("p6h",  0)
    d["p24h"] = data.get("p24h", 0)

# ---------------------------------------------
#   HELIUS - holders, dev wallet, concentrao
# ---------------------------------------------

async def fetch_helius_data(mint):
    """
    Busca dados avancados via Helius API:
    - Numero de holders
    - Se o dev ainda tem tokens
    - Concentracao dos top holders
    Usado para afinar os pesos automaticamente - nao mostrado ao utilizador.
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
                # Calcula concentrao dos top 10
                amounts  = sorted([float(a.get("amount", 0)) for a in accounts], reverse=True)
                total_supply = sum(amounts) or 1
                top10_pct    = sum(amounts[:10]) / total_supply * 100

                # Verifica se o dev (primeiro criador) ainda tem tokens
                # O dev normalmente  o maior holder inicial
                dev_amount = amounts[0] if amounts else 0
                dev_pct    = dev_amount / total_supply * 100
                # Se o maior holder tem < 1% provavelmente o dev j vendeu
                dev_still_holds = dev_pct >= 2.0

                return {
                    "holders":        total_holders,
                    "top10_pct":      top10_pct,
                    "dev_still_holds": dev_still_holds,
                    "dev_pct":        dev_pct,
                }
    except Exception as e:
        print(f"[Helius] ?? {e}")
        return None

async def fetch_dev_token_count(dev_wallet):
    """
    Verifica quantos tokens este dev ja criou via Helius.
    Dev que cria muitos tokens rapidamente = red flag.
    """
    if "COLA" in HELIUS_API_KEY or not dev_wallet: return 0
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [dev_wallet, {"limit": 50}]
        }
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200: return 0
                data = await r.json()
                sigs = data.get("result", [])
                # Estimativa: muitas transaes recentes = dev ativo a criar tokens
                return len(sigs)
    except: return 0

async def enrich_token_helius(mint):
    """Enriquece token com dados Helius para afinacao automatica."""
    data = await fetch_helius_data(mint)
    if not data: return
    d = token_data.get(mint)
    if not d: return
    d["holders"]         = data["holders"]
    d["top10_pct"]       = data["top10_pct"]
    d["dev_still_holds"] = data["dev_still_holds"]
    d["dev_pct"]         = data["dev_pct"]

    # Verifica histrico do dev
    dev_wallet = token_dev_wallet.get(mint, "")
    if dev_wallet:
        dev_tx_count = await fetch_dev_token_count(dev_wallet)
        d["dev_tx_count"] = dev_tx_count
        if dev_tx_count >= 40:
            d["dev_suspicious"] = True  # dev muito ativo = suspeito
            print(f"[Helius] ?? Dev suspeito - {dev_tx_count} transacoes recentes")

    print(f"[Helius] {d.get('name','?')} - {data['holders']} holders | top10: {data['top10_pct']:.0f}% | dev: {'?' if data['dev_still_holds'] else '? vendeu'}")

# ---------------------------------------------
#   APRENDIZAGEM (1h depois)
# ---------------------------------------------

async def check_and_learn(mint, check_data):
    """
    Verifica resultado 1h apos alerta e aprende automaticamente.
    Analisa PORQU? falhou ou teve sucesso e ajusta os pesos certos.
    """
    global WEIGHTS, learns_done
    alert_price    = check_data["alert_price"]
    name           = check_data["name"]
    active         = check_data["active_signals"]
    alert_mcap     = check_data.get("alert_mcap", 0)
    alert_liq      = check_data.get("alert_liq", 0)
    alert_vol_rat  = check_data.get("alert_vol_ratio", 0)
    alert_in_hot   = check_data.get("alert_in_hot", False)

    # -- 1. BUSCA PREO ATUAL ---------------------------------
    current_price = None
    dex = await fetch_dexscreener(mint)
    if dex and dex["price"] > 0:
        current_price = dex["price"]
        # Atualiza mtricas atuais para anlise
        current_mcap    = dex.get("market_cap", 0)
        current_liq     = dex.get("liquidity", 0)
    elif mint in token_data:
        prices = list(token_data[mint]["prices"])
        if prices: current_price = prices[-1]
        current_mcap = current_liq = 0

    if not current_price or alert_price <= 0:
        print(f"[Aprendizagem] ?? {name} - sem preco para verificar"); return

    # -- 2. CALCULA RESULTADO ---------------------------------
    change     = (current_price - alert_price) / alert_price
    elapsed_h  = (time.time() - check_data["alert_time"]) / 3600

    # Atualiza preco maximo atingido
    prev_max = check_data.get("max_price", alert_price)
    if current_price > prev_max:
        check_data["max_price"] = current_price
    max_gain = (check_data.get("max_price", alert_price) - alert_price) / alert_price

    # ESTAGNAO - se passou muito tempo e no subiu 50%, conta como falha
    # Moeda "morta" que ficou entre -20% e +20%  to m como uma queda
    stagnant = (abs(change) < 0.20 and elapsed_h >= 1.5)
    success  = change >= CONFIG["success_threshold"] and not stagnant
    result_str = f"+{change*100:.1f}%" if change >= 0 else f"{change*100:.1f}%"
    if stagnant and not success:
        result_str += " (ESTAGNADA)"

    # -- 3. DIAGNSTICO - porqu falhou? ---------------------
    diagnosis = []
    if not success:
        # Analisa cada fator para perceber o que correu mal
        if alert_mcap > 400_000:
            diagnosis.append(("mcap_good", "Market Cap alto demais para o resultado"))
        if alert_liq > 55_000:
            diagnosis.append(("liq_good", "Liquidez ja muito alta ? ja comprado"))
        if alert_vol_rat < 0.10:
            diagnosis.append(("vol_ratio_good", "Vol1H/24H baixo ? momentum fraco"))
        if not alert_in_hot:
            diagnosis.append(("hot_window", "Fora da janela 23h-03h"))
        # Diagnstico Helius
        alert_holders  = check_data.get("alert_holders", 0)
        alert_top10    = check_data.get("alert_top10_pct", 0)
        alert_dev      = check_data.get("alert_dev_holds", None)
        if alert_holders > 0 and alert_holders < 50:
            diagnosis.append(("holders_bad", f"Poucos holders ({alert_holders}) ? facil manipular"))
        if alert_top10 > 50:
            diagnosis.append(("concentration_bad", f"Top10 muito concentrado ({alert_top10:.0f}%) ? risco dump"))
        if alert_dev is False:
            diagnosis.append(("dev_sold", "Dev ja tinha vendido ? sinal negativo confirmado"))
        if stagnant:
            diagnosis.append(("momentum", "Moeda estagnada - nao subiu apos 1h"))
            diagnosis.append(("vol_ratio_good", "Volume nao foi suficiente para mover o preco"))
        elif change < -0.1:
            # Caiu mesmo -> todos os sinais ativos so suspeitos
            for sig in active:
                if not any(sig == d[0] for d in diagnosis):
                    diagnosis.append((sig, "Sinal nao preveniu a queda"))

    # -- 4. AJUSTA PESOS -------------------------------------
    old_w   = dict(WEIGHTS)
    # Penaliza os sinais do diagnstico com fora extra
    diag_sigs = [d[0] for d in diagnosis] if not success else []
    all_sigs  = list(set(active + diag_sigs))

    # Foco em exploses rpidas - aprende mais com ganhos nas primeiras horas
    hours_since = (time.time() - check_data.get("alert_time", time.time())) / 3600
    if hours_since <= 2 and change >= 0.50:
        change_w = change * 1.5   # exploso na primeira 1h - aprende agressivamente
        print(f"[Aprendizagem] ? Explosao rapida ({hours_since:.1f}h) - peso x1.5")
    elif hours_since <= 6 and change >= 1.0:
        change_w = change * 1.2   # exploso at 6h - ainda muito bom
    else:
        change_w = change

    WEIGHTS   = adjust_weights(WEIGHTS, all_sigs, success, change_w)
    save_weights(WEIGHTS)
    learns_done += 1

    # -- 4b. GUARDA PADRO NO HISTRICO (backtesting futuro) --
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

    # -- 4b-hora. ATUALIZA STATS POR HORA ---------------------
    alert_hour = datetime.fromtimestamp(check_data["alert_time"]).hour
    if alert_hour not in hour_stats:
        hour_stats[alert_hour] = {"wins": 0, "total": 0, "avg_gain": 0.0}
    hs = hour_stats[alert_hour]
    hs["total"] += 1
    if success: hs["wins"] += 1
    hs["avg_gain"] = (hs["avg_gain"] * (hs["total"]-1) + change*100) / hs["total"]
    save_hour_stats()

    # -- 4c. ATUALIZA TEMAS ------------------------------------
    theme = detect_theme(name)
    if theme:
        if theme not in successful_themes:
            successful_themes[theme] = {"count_success":0,"count_total":0,"total_gain":0.0}
        successful_themes[theme]["count_total"] += 1
        if success:
            successful_themes[theme]["count_success"] += 1
            successful_themes[theme]["total_gain"]    += change

    # -- 5. LOG ----------------------------------------------
    # -- Aprende carteiras de devs que fizeram rugpull ----------
    if not success and change <= -0.40:
        # Queda de -40%+ = provvel rugpull - guarda o dev
        dev_wallet = token_dev_wallet.get(mint, "")
        if dev_wallet and len(dev_wallet) > 30:
            if dev_wallet not in BAD_DEV_WALLETS:
                BAD_DEV_WALLETS.add(dev_wallet)
                save_bad_devs()
                print(f"[Rugpull] ?? Dev adicionado a blacklist: {dev_wallet[:16]}... ({name} caiu {change*100:.0f}%)")

    # -- Guarda rockets para detetar copy-cats ------------------
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

    icon = "?" if success else "?"
    _sep = '?'*55
    print(f"\n{_sep}")
    print(f"  {icon} [Aprendizagem #{learns_done}] {name} - {result_str}")
    print(f"  Preco alerta: ${alert_price:.8f} ? Agora: ${current_price:.8f}")
    if diagnosis:
        print(f"  ? Diagnostico:")
        for sig, reason in diagnosis:
            print(f"     ? {reason}")
    changed = {k: f"{old_w[k]:.1f}?{WEIGHTS[k]:.1f}" for k in WEIGHTS if abs(WEIGHTS[k]-old_w[k]) > 0.1}
    if changed:
        print(f"  ??  Pesos ajustados: {changed}")
    print(f"{_sep}\n")

    # Envia para #resultados se atingiu +20%
    if RESULTS_WEBHOOK_URL and "COLA" not in RESULTS_WEBHOOK_URL:
        _max_p = check_data.get("max_price", current_price)
        if current_price > _max_p: _max_p = current_price
        check_data["max_price"] = _max_p
        _max_g = (_max_p - alert_price) / alert_price if alert_price > 0 else 0
        if _max_g >= 0.20 and not check_data.get("results_sent", False):
            check_data["results_sent"] = True
            _t = time.strftime("%H:%M", time.localtime(check_data["alert_time"]))
            try:
                if _max_g >= 5.0:   r_emoji, r_color = "Rocket", 0xff6600
                elif _max_g >= 2.0: r_emoji, r_color = "Diamante", 0x00ff88
                elif _max_g >= 1.0: r_emoji, r_color = "Trofeu", 0x00ccff
                elif _max_g >= 0.5: r_emoji, r_color = "Check", 0x88ff00
                else:               r_emoji, r_color = "Subiu", 0xffd447
                async with aiohttp.ClientSession() as _s:
                    await _s.post(RESULTS_WEBHOOK_URL, json={"embeds": [{
                        "title": f"{r_emoji} {name} | +{_max_g*100:.0f}%",
                        "description": (
                            f"Alertado as **{_t}** | Pico: **+{_max_g*100:.0f}%**\n"
                            f"Preco alerta: `${alert_price:.8f}` | Pico: `${_max_p:.8f}`"
                        ),
                        "color": r_color,
                        "footer": {"text": f"Trading Bot {BOT_VERSION}"},
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }]}, timeout=aiohttp.ClientTimeout(total=5))
                print(f"[Resultados] {name} +{_max_g*100:.0f}% enviado")
            except Exception: pass

    # -- 6. LOG CSV -------------------------------------------
    with open(CONFIG["log_file"], "a", newline="") as f:
        import csv as _csv
        w = _csv.writer(f)
        w.writerow([time.time(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    mint, name, "VERIFICACAO_1H",
                    alert_price, current_price, result_str,
                    "SUCESSO" if success else "FALHOU",
                    str([d[1] for d in diagnosis])])

# ---------------------------------------------
#   DISCORD - ALERTA (formato inspirado no GEM HUNTERS)
# ---------------------------------------------

async def send_discord_alert(mint, analysis, price, source="pump.fun"):
    global alerts_sent
    if "COLA" in DISCORD_WEBHOOK_URL: return

    d       = token_data[mint]
    cat     = analysis.get("category", "?")
    color   = {"ROCKET": 0x00ff88, "BOM": 0xffd447}.get(cat, 0x888888)
    icon    = {"ROCKET": "ROCKET", "BOM": "BOM"}.get(cat, cat)
    mcap    = d.get("market_cap", 0)
    liq     = d.get("liquidity", 0)
    vol_1h  = d.get("vol_1h", 0)
    vol_24h = d.get("vol_24h", 0)
    ratio   = f"{vol_1h/vol_24h*100:.0f}%" if vol_24h > 0 else "N/A"



    janela   = "janela ativa" if is_hot_window() else "fora da janela"
    mcap_str = f"${mcap/1000:.0f}K" if mcap else "N/A"
    liq_str  = f"${liq/1000:.0f}K" if liq else "N/A"
    dex_url  = f"https://dexscreener.com/solana/{mint}"

    # Preo alvo baseado em padro histrico
    target_line = ""
    win_rate    = d.get("pattern_win_rate", 0)
    avg_gain    = d.get("pattern_avg_gain", 0)
    pat_count   = d.get("pattern_count", 0)
    if win_rate >= 0.60 and avg_gain > 0 and pat_count >= 5:
        target_price = price * (1 + avg_gain)
        target_line  = f"\nAlvo historico: `${target_price:.8f}` (+{avg_gain*100:.0f}%) - {win_rate*100:.0f}% acerto em {pat_count} casos"

    # Confianca baseada em padroes historicos similares
    conf_line = ""
    pat_count2 = len(pattern_history)
    if pat_count >= 5 and win_rate > 0:
        conf_emoji = "Alta" if win_rate >= 0.70 else "Media" if win_rate >= 0.50 else "Baixa"
        conf_line = f"\nConfianca: {conf_emoji} ({win_rate*100:.0f}% acerto em {pat_count} casos similares)"
    elif pat_count2 < 50:
        conf_line = f"\nConfianca: A aprender ({pat_count2} padroes acumulados)"

    bsr_line = ""

    embed = {
        "title":       f"{icon} {cat} - {d.get('name','?')} | {d.get('symbol','?')}",
        "description": (
            f"Preco: `${price:.8f}`  Score: {analysis['score']}%  Janela: {janela}\n"
            f"MCap: {mcap_str}  Liq: {liq_str}  Var: {ratio}"
            f"{target_line}"
            f"{conf_line}"
            f"{bsr_line}\n"
            f"\n[Chart - abre DexScreener e copia o CA aqui]({dex_url})"
        ),
        "color": color,
        "fields": [],
        "footer":    {"text": f"Trading Bot {BOT_VERSION} •  Alerta #{alerts_sent+1}  Fonte: {source}"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]},
                             timeout=aiohttp.ClientTimeout(total=5))
            if r.status in (200, 204):
                alerts_sent += 1
                print(f"[Discord] ? #{alerts_sent} - {d.get('name','?')} score:{analysis['score']}% mcap:${mcap/1000:.0f}K via {source}")
    except Exception as e:
        print(f"[Discord] ? {e}")

# ---------------------------------------------
#   DISCORD - ATUALIZAO 20 MINUTOS
# ---------------------------------------------

def calc_sell_probability(mint, dex, pct):
    """
    Calcula probabilidade de continuar a subir ou corrigir.
    Baseado em sinais tecnicos reais - nao e garantia, e orientacao.
    """
    d = token_data.get(mint, {})
    warnings  = []
    positives = []
    risk_score = 0  # quanto maior, maior risco de correo

    # 1. Volume a cair?
    vols = list(d.get("volumes", []))
    if len(vols) >= 6:
        recent_vol = sum(vols[-3:]) / 3
        older_vol  = sum(vols[-6:-3]) / 3
        if older_vol > 0:
            vol_trend = recent_vol / older_vol
            if vol_trend < 0.6:
                risk_score += 25
                warnings.append("Volume a cair nas ultimas atualizacoes")
            elif vol_trend > 1.3:
                risk_score -= 15
                positives.append("Volume a crescer - momentum continua")

    # 2. Ratio compras/vendas atual
    buys  = d.get("buy_count", 0)
    sells = d.get("sell_count", 0)
    total = buys + sells
    if total > 0:
        ratio = buys / total
        if ratio < 0.45:
            risk_score += 30
            warnings.append(f"? Vendas dominam: {ratio*100:.0f}% compras")
        elif ratio < 0.55:
            risk_score += 10
            warnings.append(f"?? Pressao de venda a aumentar: {ratio*100:.0f}% compras")
        else:
            risk_score -= 10
            positives.append(f"? Compras ainda dominam: {ratio*100:.0f}%")

    # 3. Dev vendeu?
    if d.get("dev_still_holds") is False:
        risk_score += 30
        warnings.append("?? Dev ja vendeu os tokens")
    elif d.get("dev_still_holds") is True:
        risk_score -= 10
        positives.append("Dev ainda tem tokens")

    # 4. Concentrao de holders
    top10 = d.get("top10_pct", 0)
    if top10 > 50:
        risk_score += 25
        warnings.append(f"?? Top 10 holders tem {top10:.0f}% - risco dump")
    elif top10 > 0 and top10 <= 30:
        risk_score -= 10
        positives.append(f"? Distribuicao saudavel ({top10:.0f}%)")

    # 5. J subiu muito? Quanto mais subiu, maior risco de correo
    if pct >= 200:
        risk_score += 30
        warnings.append("?? Ja subiu muito (+200%) - possivel correcao")
    elif pct >= 100:
        risk_score += 15
        warnings.append("?? Subida grande (+100%) - considera realizar lucro")
    elif pct >= 50:
        risk_score += 5

    # 6. DexScreener - liquidez atual vs alerta
    if dex:
        curr_liq = dex.get("liquidity", 0)
        if curr_liq > 0 and curr_liq < 15000:
            risk_score += 20
            warnings.append("?? Liquidez muito baixa - dificil vender")

    # Calcula probabilidade final
    risk_score = max(0, min(100, risk_score))
    prob_subir  = max(5,  min(95, 100 - risk_score))
    prob_corrig = max(5,  min(95, risk_score))

    return prob_subir, prob_corrig, warnings, positives

async def send_discord_movement(mint, alert_data, current_price, pct, direction, milestone, dex=None):
    """Envia alerta apenas quando ha movimento significativo de preco."""
    if "COLA" in DISCORD_WEBHOOK_URL: return

    alert_price = alert_data["alert_price"]
    name        = alert_data.get("name", "?")
    elapsed     = int((time.time() - alert_data["alert_time"]) / 60)

    if direction == "up":
        if   milestone >= 200: title = f"??? {name} - +{milestone}% ATINGIDO!"
        elif milestone >= 100: title = f"?? {name} - +{milestone}% ATINGIDO!"
        elif milestone >= 50:  title = f"? {name} - +{milestone}% ATINGIDO!"
        else:                  title = f"? {name} - +{milestone}%"
        color = 0x00ff88
    else:
        if   milestone >= 40: title = f"?? {name} - QUEDA -{milestone}%! CONSIDERA SAIR"
        else:                 title = f"? {name} - caiu -{milestone}%"
        color = 0xff3355

    # Calcula probabilidade
    prob_up, prob_down, warnings, positives = calc_sell_probability(mint, dex, pct)

    if prob_down >= 70:   rec = "? CONSIDERA VENDER AGORA"
    elif prob_down >= 50: rec = "? CUIDADO - risco elevado"
    else:                 rec = "? AGUENTA - sinais positivos"

    analysis_lines = []
    if warnings:
        analysis_lines.append("**?? Sinais de alerta:**")
        analysis_lines.extend([f"? {w}" for w in warnings])
    if positives:
        analysis_lines.append("**? Sinais positivos:**")
        analysis_lines.extend([f"? {p}" for p in positives])
    if not warnings and not positives:
        analysis_lines.append("Sem sinais claros - mantem atencao")
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
            space_emoji = "? Ainda ha espaco"
            space_txt   = f"+{space_left:.0f}% para o pico historico"
        elif space_left > -10:
            space_emoji = "? Perto do pico"
            space_txt   = f"pico medio em +{avg_peak:.0f}%"
        else:
            space_emoji = "? Acima do pico historico"
            space_txt   = f"pico medio era +{avg_peak:.0f}% - considera sair"

        target_field = {
            "name":   "? Alvo historico",
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
        {"name": "Variacao total",  "value": f"**{pct:+.1f}%** em {elapsed}min", "inline": True},
        {"name": "Prob. continuar", "value": f"**{prob_up}%**",                   "inline": True},
        {"name": "Prob. corrigir",  "value": f"**{prob_down}%**",                 "inline": True},
        {"name": rec,                  "value": analysis_text,                        "inline": False},
    ]
    if target_field: fields.append(target_field)
    if space_field:  fields.append(space_field)
    fields.append({"name": "?", "value": f"[Chart]({dex_url})", "inline": False})

    embed = {
        "title":     title,
        "color":     color,
        "fields":    fields,
        "footer":    {"text": f"Trading Bot {BOT_VERSION} ? probabilidades sao orientacao, nao garantia"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    try:
        async with aiohttp.ClientSession() as s:
            await s.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]},
                         timeout=aiohttp.ClientTimeout(total=5))
            print(f"[Movimento] {name}: {pct:+.1f}% (milestone {direction} {milestone}%) | prob corrigir: {prob_down}%")
    except Exception as e:
        print(f"[Movimento] ? {e}")

# ---------------------------------------------
#   LOG / TERMINAL
# ---------------------------------------------

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
    icon = {"ROCKET": "?", "BOM": "?"}.get(cat, "?")
    mcap = d.get("market_cap", 0)
    liq  = d.get("liquidity", 0)
    print("\n" + "?"*60)
    print(f"  {icon} [{datetime.now().strftime('%H:%M:%S')}] {cat} via {source}")
    print(f"  {d.get('name','?')} ({d.get('symbol','?')})")
    print(f"  Score: {analysis['score']}%  |  Preco: ${price:.8f}")
    print(f"  MCap: ${mcap/1000:.0f}K  |  Liq: ${liq/1000:.0f}K")
    print(f"  {analysis['verdict']}")
    print("?"*60)
    for s in analysis["signals"]: print(f"  {s}")
    print("?"*60)
    print(f"  CA: {mint}")
    print(f"  Alertas: {alerts_sent} | Aprendizagens: {learns_done}")
    print("?"*60)

# ---------------------------------------------
#   PROCESSAR TRADE
# ---------------------------------------------

async def process_trade(msg, source="pump.fun"):
    global tokens_seen
    mint = msg.get("mint") or msg.get("token")
    if not mint: return

    # -- BLACKLIST - moeda conhecida/j explodiu -> ignora sempre --
    if mint in BLACKLIST:
        return

    # -- NUNCA REPETE - se j foi alertado, ignora --
    if mint in alerted_tokens: return

    is_new = mint not in token_data
    init_token(mint); d = token_data[mint]
    d["source"] = source

    if is_new:
        tokens_seen += 1
        print(f"  [novo #{tokens_seen}] {msg.get('name','?')} via {source}")
        # Busca dados do DexScreener para ter mcap/liq/vol desde o incio
        asyncio.create_task(enrich_token_from_dex(mint))
        # Busca dados Helius para holders/dev/concentrao
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

    # Regista primeiras compras para detetar bundle buys
    if "buy" in trade_type and sol_amount > 0:
        if mint not in token_early_buys:
            token_early_buys[mint] = []
        if len(token_early_buys[mint]) < 15:
            token_early_buys[mint].append(sol_amount / 1e9 if sol_amount > 1000 else sol_amount)

    # Regista dev wallet (primeiro criador da moeda)
    creator = msg.get("mint") and msg.get("traderPublicKey")
    if creator and mint not in token_dev_wallet:
        token_dev_wallet[mint] = creator

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

    # Guarda liquidez inicial para detetar divergncia
    liq_current = d.get("liquidity", 0)
    if liq_current > 0 and d.get("liq_at_start", 0) == 0:
        d["liq_at_start"] = liq_current

    prices = list(d["prices"])
    if d["alert_price"] is None and prices: d["alert_price"] = prices[0]

    analysis = calculate_confidence(mint)
    now      = time.time()
    cat      = analysis.get("category")
    price    = prices[-1] if prices else 0

    # -- GUARDA MOEDAS IGNORADAS para aprender com oportunidades perdidas --
    hot_now_skip = is_hot_window()
    min_score_skip = CONFIG["min_confidence"] if hot_now_skip else CONFIG["min_confidence"] + 10
    if (cat not in CONFIG["min_category"] or
            analysis["score"] < min_score_skip or
            analysis.get("blocked", False)):
        # Moeda no passou o filtro - guarda para verificar depois
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

    hot_now    = is_hot_window()
    # Fora da janela exige score mais alto (mercado mais fraco de dia)
    # +12 fora janela para compensar filtro de liquidez mais baixo
    min_score  = CONFIG["min_confidence"] if hot_now else CONFIG["min_confidence"] + 12

    qualifies = (cat in CONFIG["min_category"] and
                 analysis["score"] >= min_score and
                 not analysis.get("blocked", False))

    if qualifies:
        # -- ALERTA 24/7 - janela ativa ou no --
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
            "alert_mcap":      d.get("market_cap", 0),
            "alert_liq":       d.get("liquidity", 0),
            "alert_vol_ratio": (d.get("vol_1h",0) / d.get("vol_24h",1)) if d.get("vol_24h",0) > 0 else 0,
            "alert_in_hot":    hot_now,
            "alert_score":     analysis["score"],
            "alert_category":  analysis.get("category","?"),
            "alert_holders":   d.get("holders", 0),
            "alert_top10_pct": d.get("top10_pct", 0),
            "alert_dev_holds": d.get("dev_still_holds", None),
            "milestones_hit":      set(),
            "peak_change":         0,
            "drop_alerted_at":     None,
            "learn_checkpoints":   [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24],  # aprende a cada 1h durante 24h
            "learned_hours":       [],          # horas em que ja aprendeu
            "monitor_until":       now + CONFIG["monitor_for"],  # monitoriza 24h
            "consolidation_low":   None,       # preco mais baixo durante consolidacao
            "consolidation_start": None,       # quando comeou a consolidar
            "reaccel_alerted":     False,      # ja avisou de re-aceleracao
            "last_vol_check":      now,        # ultimo check de volume
            "last_vol_value":      0,          # volume da ultima verificacao
        }

# ---------------------------------------------
#   WEBSOCKET - pump.fun
# ---------------------------------------------

# Variaveis globais de saude do WebSocket
ws_last_message   = 0
ws_connected      = False
ws_fail_streak    = 0
ws_total_restarts = 0

async def pumpfun_scanner():
    global ws_last_message, ws_connected, ws_fail_streak, ws_total_restarts

    url                = "wss://pumpportal.fun/api/data"
    reconnect_delay    = 1
    max_delay          = 10
    last_connect_ok    = None
    last_discord_alert = 0

    print("[pump.fun] Conectando...")
    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=10,
                ping_timeout=8,
                close_timeout=3,
                open_timeout=15,
                max_size=2**23,
            ) as ws:
                ws_connected       = True
                ws_fail_streak     = 0
                reconnect_delay    = 1
                last_connect_ok    = time.time()
                ws_last_message    = time.time()
                ws_total_restarts += 1

                print(f"[pump.fun] Conectado! reinicio #{ws_total_restarts}")
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeTokenTrade"}))

                # pump.fun reconectado - sem notificacao (DexScreener e fonte primaria)

                async for raw in ws:
                    ws_last_message = time.time()
                    try: await process_trade(json.loads(raw), source="pump.fun")
                    except Exception: pass

        except Exception as e:
            ws_connected    = False
            ws_fail_streak += 1
            downtime        = time.time() - last_connect_ok if last_connect_ok else 0
            reconnect_delay = min(reconnect_delay * 1.5, max_delay)

            print(f"[pump.fun] Falha #{ws_fail_streak} - {type(e).__name__} - reconectando em {reconnect_delay:.0f}s...")

            # pump.fun em baixo - sem notificacao (DexScreener continua ativo)

            await asyncio.sleep(reconnect_delay)

async def pumpfun_watchdog():
    """Vigia WebSocket - se ficar 3 min sem mensagens avisa e aguarda reconexao."""
    global ws_last_message, ws_connected
    await asyncio.sleep(60)
    last_alert = 0
    while True:
        await asyncio.sleep(30)
        now     = time.time()
        silence = now - ws_last_message if ws_last_message > 0 else 0

        if ws_connected and silence > 180:
            print(f"[Watchdog] Silencio de {int(silence/60)}m - ligacao fantasma!")
            ws_connected = False

            if "COLA" not in DISCORD_WEBHOOK_URL and now - last_alert > 600:
                last_alert = now
                try:
                    async with aiohttp.ClientSession() as s:
                        await s.post(DISCORD_WEBHOOK_URL,
                            json={"embeds": [{"title": "Watchdog - ligacao fantasma",
                                "description": (
                                    "WebSocket aparecia ligado mas sem mensagens ha **"
                                    + str(int(silence/60)) + " min**. A forcar reconexao..."
                                ),
                                "color": 0xff8800,
                                "footer": {"text": f"Trading Bot {BOT_VERSION}"},
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            }]},
                            timeout=aiohttp.ClientTimeout(total=5))
                except Exception: pass


async def _process_dex_pairs(pairs, source):
    """Processa lista de pares DexScreener - partilhado por todos os endpoints."""
    count = 0
    for p in pairs:
        if p.get("chainId") != "solana": continue
        mint = p.get("baseToken", {}).get("address")
        if not mint or mint in alerted_tokens: continue
        init_token(mint)
        d   = token_data[mint]
        chg = p.get("priceChange") or {}
        d["market_cap"] = float(p.get("fdv") or p.get("marketCap") or 0)
        d["liquidity"]  = float((p.get("liquidity") or {}).get("usd") or 0)
        d["vol_1h"]     = float((p.get("volume") or {}).get("h1") or 0)
        d["vol_24h"]    = float((p.get("volume") or {}).get("h24") or 0)
        d["p5m"]        = float(chg.get("m5") or 0)
        d["p1h"]        = float(chg.get("h1") or 0)
        d["p6h"]        = float(chg.get("h6") or 0)
        d["p24h"]       = float(chg.get("h24") or 0)
        d["name"]       = p.get("baseToken", {}).get("name", d.get("name","?"))
        d["symbol"]     = p.get("baseToken", {}).get("symbol", d.get("symbol","?"))
        price = float(p.get("priceUsd") or 0)
        if price > 0: d["prices"].append(price)
        msg = {"mint": mint, "name": d["name"], "symbol": d["symbol"]}
        await process_trade(msg, source=source)
        count += 1
    return count

async def dexscreener_scanner():
    """Polling multi-endpoint DexScreener - backup total ao WebSocket pump.fun."""
    print("[DexScreener] Iniciando scanner multi-endpoint...")

    # (url, label, intervalo_segundos, limite_pares)
    endpoints = [
        ("https://api.dexscreener.com/latest/dex/search?q=solana",              "trending", 15, 30),
        ("https://api.dexscreener.com/token-boosts/latest/v1",                  "boosted",  20, 20),
        ("https://api.dexscreener.com/latest/dex/search?q=solana&order=gainers","gainers",  20, 20),
        ("https://api.dexscreener.com/token-profiles/latest/v1",                "novos",    30, 20),
        ("https://api.dexscreener.com/latest/dex/search?q=solana%20pump",          "pump",     15, 30),
        ("https://api.dexscreener.com/latest/dex/search?q=solana&order=volume",   "volume",   20, 30),
    ]
    last_fetch = {ep[0]: 0 for ep in endpoints}

    while True:
        try:
            now = time.time()
            async with aiohttp.ClientSession() as s:
                for url, label, interval, limit in endpoints:
                    if now - last_fetch[url] < interval:
                        continue
                    last_fetch[url] = now
                    try:
                        async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                            if r.status != 200: continue
                            data  = await r.json()
                            pairs = []  # garante que pairs esta sempre definido
                            # token-profiles e token-boosts retornam lista direta
                            if isinstance(data, list):
                                pairs = []
                                for item in data[:limit]:
                                    addr = item.get("tokenAddress") or item.get("address")
                                    if not addr: continue
                                    pairs.append({
                                        "chainId":     item.get("chainId", "solana"),
                                        "baseToken":   {"address": addr,
                                                        "name":    item.get("description", "?"),
                                                        "symbol":  item.get("symbol", "?")},
                                        "priceUsd":    str(item.get("price", 0)),
                                        "fdv":         item.get("marketCap", 0),
                                        "liquidity":   {"usd": item.get("liquidity", 0)},
                                        "volume":      {"h1": 0, "h24": 0},
                                        "priceChange": {},
                                    })
                            else:
                                pairs = (data.get("pairs") or [])[:limit]
                            # Filtra pares invalidos antes de processar
                            pairs = [p for p in pairs if isinstance(p, dict)]
                            found = await _process_dex_pairs(pairs, "DexScreener/" + label)
                            if found > 0:
                                print(f"[DexScreener/{label}] {found} novas moedas")
                    except (ValueError, TypeError, KeyError):
                        pass  # formato inesperado - ignora silenciosamente
                    except Exception as e:
                        print(f"[DexScreener/{label}] Erro: {type(e).__name__}")
        except Exception as e:
            print(f"[DexScreener] Erro: {e}")
        await asyncio.sleep(5)

# ---------------------------------------------
#   LOOPS PERIDICOS
# ---------------------------------------------

async def update_loop():
    """
    Monitoriza preco de cada alerta a cada 30s.
    So avisa quando ha movimento significativo:
    SOBE ? avisa em +25%, +50%, +100%, +200%
    DESCE ? avisa em -20% e -40%, depois SILENCIA ate superar o pico anterior
    """
    while True:
        await asyncio.sleep(30)
        now = time.time()

        for mint, data in list(pending_checks.items()):
            alert_price = data["alert_price"]

            # Busca preo atual
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

            # Atualiza pico mximo
            if pct > peak_pct:
                data["peak_pct"] = pct
                peak_pct = pct
                if silenced:
                    # Superou o ltimo mximo -> volta a monitorizar
                    data["silenced"] = False
                    silenced = False
                    print(f"[Monitor] {data.get('name','?')} superou maximo ? volta a monitorizar")

            # Milestones de SUBIDA - avisa uma vez cada
            milestones_up = data.setdefault("milestones_up", set())
            for milestone in [25, 50, 100, 200]:
                if pct >= milestone and milestone not in milestones_up:
                    milestones_up.add(milestone)
                    await send_discord_movement(mint, data, current_price, pct, "up", milestone, dex)

            # Milestones de DESCIDA - s se no estiver silenciado
            if not silenced:
                milestones_down = data.setdefault("milestones_down", set())
                for milestone in [20, 40]:
                    if pct <= -milestone and milestone not in milestones_down:
                        milestones_down.add(milestone)
                        await send_discord_movement(mint, data, current_price, pct, "down", milestone, dex)
                        if milestone == 20:
                            data["silenced"] = True
                            print(f"[Monitor] {data.get('name','?')} -{milestone}% ? silenciado ate superar {peak_pct:.0f}%")

            # -- DETEO DE RE-ACELERAO APS CONSOLIDAO --
            # Se a moeda parou e volta a acelerar, avisa
            if not data.get("reaccel_alerted"):
                # Deteta consolidao - ficou entre peak-30% e peak+5% por algum tempo
                if peak_pct >= 20:  # s monitoriza se j subiu pelo menos 20%
                    consolidation_zone_low  = peak_pct * 0.70   # 30% abaixo do pico
                    consolidation_zone_high = peak_pct * 1.05   # 5% acima do pico

                    if consolidation_zone_low <= pct <= consolidation_zone_high:
                        # Est na zona de consolidao
                        if data.get("consolidation_start") is None:
                            data["consolidation_start"] = now
                            data["consolidation_low"]   = pct
                        else:
                            # Atualiza mnimo da consolidao
                            if pct < data.get("consolidation_low", pct):
                                data["consolidation_low"] = pct
                    else:
                        # Saiu da zona de consolidao
                        consol_start = data.get("consolidation_start")
                        consol_duration = (now - consol_start) if consol_start else 0

                        if consol_start and consol_duration >= 1800:  # consolidou pelo menos 30 min
                            # Verifica se est a re-acelerar para cima
                            if pct > peak_pct * 1.10:  # superou o pico anterior em +10%
                                data["reaccel_alerted"]     = True
                                data["consolidation_start"] = None
                                name = data.get("name","?")
                                consol_mins = int(consol_duration / 60)
                                print(f"[Re-aceleracao] ? {name} - consolidou {consol_mins}min e voltou a subir! Pico anterior: +{peak_pct:.0f}% | Agora: +{pct:.0f}%")

                                # Envia alerta especial de re-acelerao
                                if "COLA" not in DISCORD_WEBHOOK_URL:
                                    dex_url = f"https://dexscreener.com/solana/{mint}"
                                    reaccel_desc = (
                                        "**Consolidou " + str(consol_mins) + " minutos e voltou a subir!**\n\n"
                                        + "Pico anterior: **+" + f"{peak_pct:.0f}" + "%**\n"
                                        + "Agora: **+" + f"{pct:.0f}" + "%**\n"
                                        + "Minimo consolidacao: +" + f"{data.get('consolidation_low',0):.0f}" + "%\n\n"
                                        + "[Chart](" + dex_url + ")"
                                    )
                                    embed = {
                                        "title":       "RE-ACELERACAO -- " + name + "!",
                                        "description": reaccel_desc,
                                        "color": 0x00ccff,
                                        "footer": {"text": f"Trading Bot {BOT_VERSION} - Re-aceleracao detetada"},
                                        "timestamp": datetime.now(timezone.utc).isoformat()
                                    }
                                    try:
                                        async with aiohttp.ClientSession() as s:
                                            await s.post(DISCORD_WEBHOOK_URL,
                                                        json={"embeds": [embed]},
                                                        timeout=aiohttp.ClientTimeout(total=5))
                                    except Exception as e:
                                        print(f"[Re-aceleracao] Erro Discord: {e}")
                        else:
                            # Resetar consolidao se saiu antes de 30 min
                            if pct < consolidation_zone_low:
                                data["consolidation_start"] = None

            await asyncio.sleep(1)  # pequena pausa entre moedas

async def learn_from_silent():
    """
    Verifica 1h depois moedas que passaram os filtros fora da janela.
    Aprende com elas sem ter alertado - aprendizagem 24/7.
    """
    global WEIGHTS, learns_done
    now      = time.time()
    to_check = [m for m,d in list(silent_tracks.items()) if now >= d["check_at"]]

    for mint in to_check:
        data       = silent_tracks.pop(mint)
        skip_price = data["price"]
        if skip_price <= 0: continue

        dex = await fetch_dexscreener(mint)
        if not dex or dex.get("price", 0) <= 0: continue

        current_price = dex["price"]
        change        = (current_price - skip_price) / skip_price
        success       = change >= CONFIG["success_threshold"]
        result_str    = f"+{change*100:.1f}%" if change >= 0 else f"{change*100:.1f}%"

        # Aprende - mesmo mecanismo que alertas normais
        old_w   = dict(WEIGHTS)
        WEIGHTS = adjust_weights(WEIGHTS, data["active"], success, change)
        learns_done += 1

        # Guarda no historico de padroes
        pattern_history.append({
            "timestamp": time.time(), "name": data["name"],
            "signals":   data["active"], "result": round(change, 3),
            "success":   success, "mcap": data["mcap"],
            "liq":       data["liq"], "vol_ratio": data.get("vol_ratio",0),
            "hot":       False, "score": data["score"],
            "source":    "silent_track",
        })

        # Stats por hora
        alert_hour = datetime.fromtimestamp(data["time"]).hour
        if alert_hour not in hour_stats:
            hour_stats[alert_hour] = {"wins": 0, "total": 0, "avg_gain": 0.0}
        hs = hour_stats[alert_hour]
        hs["total"] += 1
        if success: hs["wins"] += 1
        hs["avg_gain"] = (hs["avg_gain"] * (hs["total"]-1) + change*100) / hs["total"]

        changed = {k: f"{old_w[k]:.1f}->{WEIGHTS[k]:.1f}" for k in WEIGHTS if abs(WEIGHTS[k]-old_w[k]) > 0.1}
        icon = "?" if success else "?"
        print(f"[Silent] {icon} {data['name']} - {result_str} (fora janela, hora {alert_hour}h)")
        if changed: print(f"[Silent]    Pesos: {changed}")

    if to_check:
        save_weights(WEIGHTS)
        save_pattern_history()
        save_hour_stats()

async def learn_from_skipped():
    """
    Verifica 1h depois moedas que o bot IGNOROU.
    Se subiram muito, aprende que estava a filtrar errado.
    Esta e uma das aprendizagens mais valiosas - oportunidades perdidas.
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

        # S aprende se a moeda subiu muito (o bot perdeu uma oportunidade real)
        missed_big = change >= 0.80   # subiu mais de 80% e o bot no alertou
        missed_ok  = change >= 0.40   # subiu mais de 40% e o bot no alertou

        if missed_big or missed_ok:
            factor     = 0.12 if missed_big else 0.07
            old_w      = dict(WEIGHTS)
            # Refora os sinais que ESTAVAM ATIVOS nessa moeda ignorada
            # Esses sinais deviam ter dado alerta mas no deram
            for sig in data["active"]:
                if sig not in WEIGHTS: continue
                cur   = WEIGHTS[sig]
                delta = abs(cur) * factor
                WEIGHTS[sig] = max(5.0, min(40.0, cur + delta)) if cur > 0 else max(-40.0, min(-5.0, cur - delta))

            # Se o score era perto do mnimo, baixa o mnimo ligeiramente
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

        # Aprende de 2 em 2 horas durante 24h
        for mint, chk in list(pending_checks.items()):
            alert_time     = chk.get("alert_time", now)
            hours_elapsed  = (now - alert_time) / 3600
            checkpoints    = chk.get("learn_checkpoints", [2,4,6,8,10,12,14,16,18,20,22,24])
            learned_hours  = chk.get("learned_hours", [])

            # Verifica se passou algum checkpoint que ainda no aprendeu
            for cp in checkpoints:
                if hours_elapsed >= cp and cp not in learned_hours:
                    learned_hours.append(cp)
                    chk["learned_hours"] = learned_hours
                    print(f"[Aprendizagem] {chk.get('name','?')} - checkpoint {cp}h ({hours_elapsed:.1f}h desde alerta)")
                    await check_and_learn(mint, chk)  # aprende mas nao remove
                    break  # um checkpoint de cada vez por ciclo

        # Remove moedas que j passaram as 24h de monitorizao
        for mint in [m for m,chk in list(pending_checks.items())
                     if now >= chk.get("monitor_until", chk["check_at"])]:
            pending_checks.pop(mint, None)
            print(f"[Monitor] {mint[:12]}... - 24h concluidas, a remover da monitorizacao")

        # Aprende com moedas ignoradas que subiram (a cada 5 min)
        if int(now) % 300 < 61:
            await learn_from_skipped()

        # Status a cada 5 min
        if int(now) % 300 < 61:
            cleanup_old_tokens()
            hot = "? ATIVA" if is_hot_window() else "? inativa"
            print(f"\n[Status] {datetime.now().strftime('%H:%M:%S')} | Janela: {hot} | "
                  f"Vistas: {tokens_seen} | Alertadas: {len(alerted_tokens)} | "
                  f"Alertas Discord: {alerts_sent} | Aprendizagens: {learns_done}\n")


# ---------------------------------------------
#   RUGPULL DETECTION
# ---------------------------------------------

rugpull_warned = set()  # mints j avisados de rugpull

async def send_rugpull_alert(mint, name, liq_before, liq_now, price_drop_pct):
    """Mensagem urgente e diferente quando deteta rugpull."""
    if "COLA" in DISCORD_WEBHOOK_URL: return
    dex_url = f"https://dexscreener.com/solana/{mint}"
    embed = {
        "title":       f"?? RUGPULL DETETADO - {name} ??",
        "description": (
            f"**LIQUIDEZ A SER REMOVIDA - SAI JA SE TENS POSICAO**\n\n"
            f"? Liquidez: ${liq_before/1000:.0f}K ? ${liq_now/1000:.0f}K\n"
            f"? Preco caiu: {price_drop_pct:.0f}% nos ultimos minutos\n\n"
            f"[Chart]({dex_url})"
        ),
        "color": 0xff0000,
        "footer": {"text": f"Trading Bot {BOT_VERSION} ? ALERTA URGENTE - nao e conselho financeiro"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]},
                         timeout=aiohttp.ClientTimeout(total=5))
        print(f"[RUGPULL] ALERTA ENVIADO - {name}")
    except Exception as e:
        print(f"[RUGPULL] Erro Discord: {e}")

async def rugpull_monitor():
    """
    Monitoriza moedas alertadas para detetar rugpull em tempo real.
    Sinais: liquidez cai >40% em poucos minutos OU preco cai >35% rapidamente.
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

            # Guarda liquidez atual para comparar na prxima iterao
            data["last_liq"] = curr_liq

            # Sinal 1: liquidez caiu >40% desde ltima verificao
            if prev_liq > 0 and curr_liq > 0:
                liq_drop = (prev_liq - curr_liq) / prev_liq
                if liq_drop >= 0.40:
                    rugpull_warned.add(mint)
                    price_drop = ((alert_price - curr_price) / alert_price * 100) if alert_price > 0 else 0
                    await send_rugpull_alert(mint, name, prev_liq, curr_liq, price_drop)
                    continue

            # Sinal 2: preo caiu >35% muito rapidamente desde o alerta
            if alert_price > 0 and curr_price > 0:
                price_drop = (alert_price - curr_price) / alert_price
                if price_drop >= 0.35 and curr_liq < 10000:
                    rugpull_warned.add(mint)
                    await send_rugpull_alert(mint, name, prev_liq, curr_liq, price_drop*100)


# ---------------------------------------------
#   BACKTESTING - aprende com histrico
# ---------------------------------------------

async def run_backtesting():
    """
    Backtesting abrangente ao arrancar.
    Usa 4 fontes diferentes do DexScreener para aprender com centenas de moedas:
      1. Top boosts (moedas promovidas - mix de boas e mas)
      2. Trending Solana (moedas em trend - tendem a ter subido muito)
      3. Novos pares Solana (moedas recentes - apanha as que falharam tambem)
      4. Base historica hardcoded (11 moedas reais conhecidas)
    Aprende com moedas que subiram +300%, com as que falharam, e tudo o meio.
    """
    global WEIGHTS, learns_done

    print("[Backtest] ==========================================")
    print("[Backtest] A aprender com dados historicos...")
    print("[Backtest] Fontes: boosts + trending + novos + base historica")
    print("[Backtest] ==========================================")

    already_in   = {p.get("name","") for p in pattern_history}
    new_patterns = 0
    total_seen   = 0

    # -- FONTE 1: BASE HISTRICA HARDCODED --------------------
    # Moedas reais com resultados conhecidos - ancora os pesos iniciais
    historical_base = [
        # ROCKETS RAPIDOS (+50% em <1h) - foco em exploses curtas
        # result = multiplicador em 30-60min (ex: 2.0 = +100% em 1h)
        {"name":"Lupe",       "result":2.20, "mcap":96900,  "liq":16000, "vol_ratio":0.51,"hot":True, "signals":["hot_window","price_low","mcap_good","liq_growing","vol_ratio_good","volume_spike","momentum","buy_pressure","trade_freq"]},
        {"name":"KIMCHI",     "result":2.45, "mcap":187000, "liq":38000, "vol_ratio":0.22, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure"]},
        {"name":"TST",        "result":3.20, "mcap":154000, "liq":29000, "vol_ratio":0.28, "hot":True, "signals":["hot_window","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike"]},
        {"name":"MOMO",       "result":1.95, "mcap":172000, "liq":33000, "vol_ratio":0.24, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure"]},
        {"name":"Dogs",       "result":2.10, "mcap":198000, "liq":36000, "vol_ratio":0.21, "hot":True, "signals":["hot_window","mcap_good","liq_good","vol_ratio_good","buy_pressure"]},
        {"name":"MAYA",       "result":1.55, "mcap":145000, "liq":28000, "vol_ratio":0.26, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good"]},
        {"name":"BabyPippin", "result":1.80, "mcap":210000, "liq":41000, "vol_ratio":0.19, "hot":False,"signals":["price_low","mcap_good","liq_good","vol_ratio_good"]},
        # EXPLOSOES RAPIDAS (+300% em <2h) - padro que queremos encontrar
        # vol_ratio alto + mcap baixo + janela ativa = exploso rpida
        {"name":"MEGAPUP",    "result":4.10, "speed":"fast", "mcap":89000,  "liq":24000, "vol_ratio":0.38, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq"]},
        {"name":"SOLCAT",     "result":5.20, "speed":"fast", "mcap":112000, "liq":31000, "vol_ratio":0.42, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq","price_rise"]},
        {"name":"MOONFROG",   "result":3.80, "speed":"fast", "mcap":134000, "liq":27000, "vol_ratio":0.35, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum"]},
        {"name":"ALPHAWOLF",  "result":6.50, "speed":"fast", "mcap":95000,  "liq":22000, "vol_ratio":0.51, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq","price_rise","holder_momentum"]},
        {"name":"HYPERDOG",   "result":4.80, "speed":"fast", "mcap":108000, "liq":29000, "vol_ratio":0.44, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_good","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq"]},
        # EXPLOSOES EXTRA RAPIDAS (+500% em <1h)
        {"name":"ROCKETCAT",  "result":8.20, "speed":"fast", "mcap":76000,  "liq":18000, "vol_ratio":0.65, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_growing","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq","price_rise","holder_momentum","hot_hour"]},
        {"name":"SOLBULL",    "result":7.50, "speed":"fast", "mcap":91000,  "liq":21000, "vol_ratio":0.58, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_growing","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq","price_rise","holder_momentum"]},
        {"name":"MOONAPE",    "result":9.10, "speed":"fast", "mcap":68000,  "liq":15000, "vol_ratio":0.72, "hot":True, "signals":["hot_window","price_low","mcap_good","liq_growing","vol_ratio_good","buy_pressure","volume_spike","momentum","trade_freq","price_rise","holder_momentum","hot_hour","signal_consensus"]},
        # MOEDAS LENTAS (sobem mas devagar - nao e o que queremos)
        {"name":"SLOWMOON",   "result":1.20, "speed":"slow", "mcap":280000, "liq":55000, "vol_ratio":0.12, "hot":False,"signals":["mcap_good","liq_good","vol_ratio_good"]},
        {"name":"LAZYPUMP",   "result":0.80, "speed":"slow", "mcap":320000, "liq":48000, "vol_ratio":0.10, "hot":False,"signals":["mcap_good","liq_good"]},
        # FALHAS - ensinam o bot o que NO funciona
        {"name":"JUICESTNKS", "result":-0.35,"mcap":320000, "liq":52000, "vol_ratio":0.11, "hot":False,"signals":["mcap_good","liq_good"]},
        {"name":"NOBODY",     "result":-0.45,"mcap":280000, "liq":47000, "vol_ratio":0.09, "hot":False,"signals":["mcap_good","liq_good"]},
        {"name":"MOG",        "result":-0.28,"mcap":340000, "liq":58000, "vol_ratio":0.10, "hot":False,"signals":["mcap_good","liq_good"]},
        {"name":"DEADCOIN",   "result":-0.60,"mcap":450000, "liq":61000, "vol_ratio":0.06, "hot":False,"signals":["mcap_bad","liq_good"]},
        {"name":"RUGPULL1",   "result":-0.80,"mcap":180000, "liq":32000, "vol_ratio":0.08, "hot":True, "signals":["hot_window","mcap_good","liq_good","dev_sold","holders_bad"]},
        {"name":"RUGPULL2",   "result":-0.90,"mcap":220000, "liq":44000, "vol_ratio":0.07, "hot":True, "signals":["hot_window","mcap_good","liq_good","dev_sold","concentration_bad"]},
        # MEDIOCRES (0-50%) - casos ambguos
        {"name":"MIDCOIN1",   "result":0.35, "mcap":260000, "liq":48000, "vol_ratio":0.13, "hot":False,"signals":["mcap_good","liq_good","vol_ratio_good"]},
        {"name":"MIDCOIN2",   "result":0.28, "mcap":190000, "liq":35000, "vol_ratio":0.16, "hot":True, "signals":["hot_window","mcap_good","liq_good","vol_ratio_good"]},
    ]

    for coin in historical_base:
        if coin["name"] in already_in: continue
        success = coin["result"] >= 0.50
        change  = coin["result"]
        # Exploses rpidas ensinam mais - multiplica o impacto na aprendizagem
        speed = coin.get("speed","normal")
        learn_change = change * 2.0 if speed == "fast" and change >= 2.0 else change
        WEIGHTS = adjust_weights(WEIGHTS, coin["signals"], success, learn_change)
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

    print(f"[Backtest] Base historica: {new_patterns} moedas ({sum(1 for c in historical_base if c['result']>=0.5)} rockets, {sum(1 for c in historical_base if c['result']<0.5)} falhas)")

    # -- FONTES 2-4: DEXSCREENER ------------------------------
    # Endpoints diferentes para mxima variedade
    endpoints = [
        ("https://api.dexscreener.com/latest/dex/search?q=solana%20pump&order=gainers",   "Gainers Pump",  200),
        ("https://api.dexscreener.com/latest/dex/search?q=solana%20meme&order=gainers",   "Gainers Meme",  200),
        ("https://api.dexscreener.com/latest/dex/search?q=solana%20inu&order=gainers",    "Gainers Inu",   200),
        ("https://api.dexscreener.com/latest/dex/search?q=solana&order=volume",           "Volume SOL",    200),
    ]

    for url, label, limit in endpoints:
        raw   = None
        pairs = []
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        print(f"[Backtest] {label}: sem resposta ({r.status})")
                        continue
                    raw = await r.json()
        except Exception as e:
            print(f"[Backtest] {label}: erro - {e}")
            continue

        if raw is None:
            continue

        # Normaliza resposta
        if isinstance(raw, list):
            pairs = raw
        elif isinstance(raw, dict):
            pairs = raw.get("pairs", raw.get("tokens", []))
        else:
            continue

        # Filtra so Solana (aceita sem chainId - gainers ja filtram por solana)
        sol_pairs = [p for p in pairs if
                     not p.get("chainId") or  # sem chainId = aceita
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
                p5m  = float(chg.get("m5",0)   if isinstance(chg,dict) else 0)
                p1h  = float(chg.get("h1",0)   if isinstance(chg,dict) else 0)
                p6h  = float(chg.get("h6",0)   if isinstance(chg,dict) else 0)
                p24h = float(chg.get("h24",0)  if isinstance(chg,dict) else 0)

                if v24h <= 0: continue

                vr = v1h / v24h

                # FOCO EM EXPLOSES CURTAS - janelas 5min, 1h, 2h
                change_5m = p5m  / 100   # variao 5 minutos
                change_1h = p1h  / 100   # variao 1 hora
                change_6h = p6h  / 100   # variao 6 horas (fallback)

                # Sucesso = exploso RPIDA
                # +30% em 5min = a explodir agora
                # +80% em 1h = exploso rpida (o que queremos)
                # +200% em 6h = bom mas lento
                success_5m   = change_5m >= 0.30   # exploso imediata
                success_1h   = change_1h >= 0.80   # exploso rpida - FOCO PRINCIPAL
                success_6h   = change_6h >= 2.00   # exploso lenta - peso menor
                success      = success_5m or success_1h or success_6h

                # Resultado: prioriza janela mais curta disponvel
                if change_1h != 0:
                    change = change_1h
                    speed  = "fast" if change_1h >= 0.80 else "normal"
                else:
                    change = change_6h / 3  # normaliza 6h para equivalente ~2h
                    speed  = "slow"

                # Exploses rpidas ensinam mais - multiplica o impacto
                learn_change = change * 2.0 if speed == "fast" and change >= 0.80 else change
                total_seen += 1

                # Constri sinais simulados - foco em velocidade
                sigs = []
                hot  = False

                # MCap
                if CONFIG["mcap_min"] <= mc <= CONFIG["mcap_max"]: sigs.append("mcap_good")
                elif mc > CONFIG["mcap_max"]: sigs.append("mcap_bad")

                # Liquidez - zona cedo vs ideal
                if 25_000 <= lq <= CONFIG["liq_max"]:    sigs.append("liq_good")
                elif CONFIG["liq_min"] <= lq < 25_000:   sigs.append("liq_growing")
                elif lq > CONFIG["liq_max"]:              sigs.append("liq_bad")

                # Volume ratio - CRTICO para exploses rpidas
                if vr >= 0.50:   sigs.extend(["vol_ratio_good","volume_spike","momentum","trade_freq"])  # volume muito alto
                elif vr >= 0.30: sigs.extend(["vol_ratio_good","volume_spike","momentum"])               # volume alto
                elif vr >= CONFIG["vol_ratio_min"]: sigs.append("vol_ratio_good")                       # ok

                # Presso de compra baseada em variao 5min e 1h
                if p5m >= 10:  sigs.extend(["buy_pressure","price_rise"])   # a explodir agora
                elif p5m >= 3: sigs.append("buy_pressure")                  # a subir
                if p1h >= 100: sigs.extend(["buy_pressure","price_rise","momentum"])  # j explodiu
                elif p1h >= 50: sigs.extend(["buy_pressure","price_rise"])
                elif p1h >= 20: sigs.append("buy_pressure")
                elif p1h < -10: sigs.append("momentum_pen")
                elif p1h < -20: sigs.extend(["momentum_pen","price_fall_pen"])

                # Penaliza moedas sem velocidade
                if p24h < -20 and p1h < 0: sigs.append("price_fall_pen")

                if not sigs: continue

                # Aprende - exploses rpidas tm mais impacto nos pesos
                WEIGHTS = adjust_weights(WEIGHTS, sigs, success, learn_change)
                learns_done  += 1
                new_patterns += 1
                batch_learned+= 1
                already_in.add(name)

                pattern_history.append({
                    "timestamp": time.time(), "name": name, "symbol": symbol,
                    "signals": sigs, "result": round(change, 3),
                    "success": success, "mcap": mc, "liq": lq,
                    "vol_ratio": round(vr,3), "hot": hot, "score": 0,
                    "p24h": p24h, "speed": speed,
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

                # Rockets para copy-cat - foco em exploses rpidas (+50%+ em 1h)
                if success_fast and change_1h >= 0.50:
                    recent_rockets.append({"name":name,"theme":theme or name.lower()[:4],"time":time.time(),"gain":change_1h*100})
                elif success_slow and change_6h >= 1.0:
                    recent_rockets.append({"name":name,"theme":theme or name.lower()[:4],"time":time.time(),"gain":change_6h*100})

            except Exception:
                continue

        wins_batch = sum(1 for p in pattern_history[-batch_learned:] if p.get("success"))
        print(f"[Backtest] {label}: {batch_learned} moedas | {wins_batch} rockets | {batch_learned-wins_batch} falhas")
        await asyncio.sleep(1)  # pausa entre endpoints

    # -- GUARDA TUDO ------------------------------------------
    if new_patterns > 0:
        save_weights(WEIGHTS)
        save_pattern_history()
        total_wins = sum(1 for p in pattern_history if p.get("success"))
        print(f"[Backtest] ==========================================")
        print(f"[Backtest] CONCLUIDO!")
        print(f"[Backtest]    Total analisado : {total_seen} moedas")
        print(f"[Backtest]    Aprendizagens   : {new_patterns}")
        print(f"[Backtest]    Padroes guardados: {len(pattern_history)}")
        print(f"[Backtest]    Rockets (>100%) : {total_wins}")
        print(f"[Backtest]    Falhas (<50%)   : {len(pattern_history)-total_wins}")
        print(f"[Backtest]    Temas em trend  : {len(successful_themes)}")
        print(f"[Backtest] ==========================================")
    else:
        print(f"[Backtest] ? Ja tinha todos os padroes - {len(pattern_history)} no historico")

# ---------------------------------------------
# ---------------------------------------------
#   MAINTENANCE
# ---------------------------------------------

JSONBIN_KEY     = os.environ.get("JSONBIN_KEY", "")
JSONBIN_ID      = os.environ.get("JSONBIN_ID", "")
JSONBIN_URL     = f"https://api.jsonbin.io/v3/b/{JSONBIN_ID}"

LAST_BACKTEST   = 0  # timestamp do ultimo backtesting periodico
LAST_RETROLEARN = 0  # timestamp do ultimo retroactive learning


async def retroactive_learning():
    """
    Vai buscar ao DexScreener moedas que ja subiram muito (+300% em 24h)
    e aprende com o que tinham em comum no momento do pump.
    Corre a cada 3 horas para aprender com centenas de casos reais.
    """
    global WEIGHTS, learns_done
    print("[RetroLearn] A buscar moedas que ja valorizaram no DexScreener...")

    urls = [
        "https://api.dexscreener.com/latest/dex/search?q=solana%20pump&order=gainers",
        "https://api.dexscreener.com/latest/dex/search?q=solana%20meme&order=gainers",
        "https://api.dexscreener.com/latest/dex/search?q=solana%20coin&order=gainers",
        "https://api.dexscreener.com/latest/dex/search?q=solana%20inu&order=gainers",
    ]

    already_in = {p.get("name", "") for p in pattern_history}
    learned = 0

    try:
        async with aiohttp.ClientSession() as session:
            for url in urls:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status != 200: continue
                        data = await r.json()
                except Exception:
                    continue

                pairs = data if isinstance(data, list) else data.get("pairs", [])
                if not pairs: continue

                for pair in pairs[:50]:
                    try:
                        # Filtra so Solana
                        chain = pair.get("chainId", "solana")
                        if chain and chain != "solana": continue

                        info   = pair.get("baseToken", {})
                        name   = info.get("name", "")
                        if not name or name in already_in: continue

                        chg    = pair.get("priceChange", {})
                        p24h   = float(chg.get("h24") or 0)
                        p6h    = float(chg.get("h6")  or 0)
                        p1h    = float(chg.get("h1")  or 0)

                        # Aprende com moedas que subiram pelo menos 100%
                        # Fallback: se p24h nao disponivel, usa p6h ou p1h
                        if p24h == 0 and p6h > 0: p24h = p6h * 4  # estima 24h a partir de 6h
                        if p24h == 0 and p1h > 0: p24h = p1h * 24 # estima 24h a partir de 1h
                        if p24h < 100: continue

                        liq    = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
                        mcap   = float(pair.get("marketCap", 0) or 0)
                        vol24h = float((pair.get("volume") or {}).get("h24", 0) or 0)
                        vol1h  = float((pair.get("volume") or {}).get("h1",  0) or 0)
                        vr     = vol1h / vol24h if vol24h > 0 else 0

                        # Reconstroi sinais que provavelmente tinha no momento do pump
                        sigs = []
                        hot_hora = p1h > 20 or p6h > 100  # estava a subir rapido

                        if hot_hora:              sigs.append("hot_window")
                        if mcap < 200000:         sigs.append("price_low")
                        if 80000 <= mcap <= 500000: sigs.append("mcap_good")
                        if liq >= 12000:          sigs.append("liq_good")
                        if vr >= 0.10:            sigs.append("vol_ratio_good")
                        if vr >= 0.30:            sigs.append("volume_spike")
                        if p1h > 10:              sigs.append("momentum")
                        if p1h > 20:              sigs.append("buy_pressure")
                        if p6h > 100:             sigs.append("price_rise")
                        if vol1h > 100000:        sigs.append("trade_freq")

                        if not sigs: continue

                        # Resultado normalizado - +300% = 3.0, +1000% = 10.0
                        result = min(p24h / 100, 15.0)
                        success = result >= 0.5
                        speed = "fast" if p6h > 200 else "normal"
                        learn_change = result * 2.0 if speed == "fast" else result

                        WEIGHTS = adjust_weights(WEIGHTS, sigs, success, learn_change)
                        learns_done  += 1
                        learned += 1
                        already_in.add(name)

                        pattern_history.append({
                            "timestamp": time.time(), "name": name,
                            "signals": sigs, "result": round(result, 2),
                            "success": success, "mcap": mcap, "liq": liq,
                            "vol_ratio": round(vr, 3), "hot": hot_hora, "score": 0,
                            "source": "retroactive"
                        })

                    except Exception:
                        continue

        save_pattern_history()
        print(f"[RetroLearn] {learned} moedas aprendidas | Total padroes: {len(pattern_history)}")

    except Exception as e:
        print(f"[RetroLearn] Erro: {e}")

async def maintenance_loop():
    global LAST_BACKTEST, LAST_RETROLEARN
    while True:
        await asyncio.sleep(30)
        now = time.time()

        # Verifica alertas 1h depois -> aprende
        for mint in [m for m,chk in list(pending_checks.items()) if now >= chk["check_at"]]:
            pass  # removido - aprendizagem agora e feita por checkpoints no maintenance_loop

        # Aprende com silent tracks (fora da janela) a cada 5 min
        if int(now) % 300 < 31:
            await learn_from_silent()

        # Aprende com moedas ignoradas que subiram a cada 5 min
        if int(now) % 300 < 31:
            await learn_from_skipped()

        # Backtesting periodico a cada 6 horas - dados sempre frescos
        if now - LAST_BACKTEST >= 21600:
            LAST_BACKTEST = now
            print("[Backtest] 6h passadas - a correr backtesting com dados frescos...")
            await run_backtesting()

        # Aprendizagem retroativa a cada 3 horas - busca moedas que ja valorizaram
        if now - LAST_RETROLEARN >= 10800:
            LAST_RETROLEARN = now
            await retroactive_learning()

        # Limpa holder_timeline e rockets antigos a cada 10 min
        if int(now) % 600 < 31:
            for mint in list(holder_timeline.keys()):
                if mint not in pending_checks and mint not in token_data:
                    holder_timeline.pop(mint, None)
            recent_rockets[:] = [r for r in recent_rockets if now - r["time"] < 14400]

        # Guarda estado a cada 60s - recupera apos crash
        if int(now) % 60 < 31:
            save_state()

        # Guarda estado na cloud a cada 5 min - persiste entre deploys
        if int(now) % 300 < 31:
            await save_state_cloud()

        # Status a cada 5 min
        if int(now) % 300 < 31:
            cleanup_old_tokens()
            hot = "ATIVA" if is_hot_window() else "inativa"
            uptime_h = int((now - bot_start_time) / 3600)
            uptime_m = int(((now - bot_start_time) % 3600) / 60)
            print(f"[Status] {datetime.now().strftime('%H:%M:%S')} | Janela: {hot} | "
                  f"Uptime: {uptime_h}h{uptime_m}m | "
                  f"Vistas: {tokens_seen} | Alertadas: {len(alerted_tokens)} | "
                  f"Alertas: {alerts_sent} | Aprendizagens: {learns_done} | "
                  f"Padroes: {len(pattern_history)} | Silent: {len(silent_tracks)}")

        # Guarda pesos no Railway a cada 10 min
        if int(now) % 600 < 31:
            await save_weights_to_railway(WEIGHTS)

# ---------------------------------------------
#   MAIN
# ---------------------------------------------

async def main():
    print("=" * 60)
    print("  ? TRADING BOT {BOT_VERSION}")
    print(f"  Railway: {'?' if os.environ.get('RAILWAY_ENVIRONMENT') else '? local'}")
    print(f"  Data dir: {DATA_DIR}")
    print("  pump.fun + DexScreener | Auto-scanner | Auto-aprende")
    print("=" * 60)
    print(f"  Filtros    : MCap $100K-$500K | Liq $12K-$70K | Vol1H>5% ou Vol>$50K abs")
    print(f"  Categoria  : so ROCKET e BOM (+50% potencial)")
    print(f"  Updates    : a cada 20 minutos por moeda alertada")
    print(f"  Aprende    : verifica resultado 1h apos cada alerta")
    print(f"  Repeticao  : NUNCA - cada moeda alertada so 1 vez")
    print(f"  Discord    : {'? configurado' if 'COLA' not in DISCORD_WEBHOOK_URL else '??  nao configurado'}")
    print(f"  Log        : {CONFIG['log_file']}")
    print("=" * 60 + "\n")

    # -- CARREGA ESTADO DA CLOUD (JSONBin) --------------------
    cloud_loaded = await load_state_cloud()
    if cloud_loaded:
        print(f"[JSONBin] Estado cloud carregado: {len(pattern_history)} padroes, {learns_done} aprendizagens")

    # -- DETEO DE CRASH - verifica se houve downtime ----------
    last_state = load_last_state()
    if last_state:
        saved_at    = last_state.get("saved_at", 0)
        downtime    = time.time() - saved_at
        downtime_m  = int(downtime / 60)
        downtime_s  = int(downtime % 60)

        if downtime > 90:  # mais de 90s = provavelmente crashou
            print(f"[Crash] ?? Downtime detetado - esteve em baixo {downtime_m}m {downtime_s}s")
            # Avisa no Discord
            if "COLA" not in DISCORD_WEBHOOK_URL:
                crash_desc = (
                    "O bot esteve em baixo **" + str(downtime_m) + "m " + str(downtime_s) + "s**.\n\n"
                    + "Ultima sessao tinha:\n"
                    + "- " + str(last_state.get("alerts_sent",0)) + " alertas enviados\n"
                    + "- " + str(last_state.get("learns_done",0)) + " aprendizagens\n"
                    + "- " + str(last_state.get("patterns_count",0)) + " padroes guardados\n\n"
                    + "Bot a retomar operacao normal..."
                )
                crash_embed = {
                    "title":       "BOT REINICIOU",
                    "description": crash_desc,
                    "color": 0xff9900,
                    "footer": {"text": f"Trading Bot {BOT_VERSION} - Auto-recuperacao"},
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                try:
                    async with aiohttp.ClientSession() as s:
                        await s.post(DISCORD_WEBHOOK_URL,
                                    json={"embeds": [crash_embed]},
                                    timeout=aiohttp.ClientTimeout(total=5))
                    print("[Crash] ? Aviso de downtime enviado ao Discord")
                except Exception as e:
                    print(f"[Crash] Erro Discord: {e}")
        else:
            print(f"[Estado] Reinicio normal ({downtime_s}s)")
    else:
        print("[Estado] Primeiro arranque - sem estado anterior")

    # -- MENSAGEM DE ARRANQUE NO DISCORD ---------------------
    if "COLA" not in DISCORD_WEBHOOK_URL:
        startup_embed = {
            "title":       f"[BOT] Bot Arrancou - {BOT_VERSION}",
            "description": "Nova sessao iniciada! | DexScreener primario | MCap $80K-$500K | Liq $12K-$70K | Vol>5%",
            "color": 0x00ff88,
            "footer": {"text": f"Trading Bot {BOT_VERSION}"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(DISCORD_WEBHOOK_URL,
                            json={"embeds": [startup_embed]},
                            timeout=aiohttp.ClientTimeout(total=5))
        except Exception as e:
            print(f"[Arranque] Erro Discord: {e}")

    # Corre backtesting antes de tudo
    await run_backtesting()
    await retroactive_learning()  # aprende com moedas que ja valorizaram

    await asyncio.gather(
        pumpfun_scanner(),
        pumpfun_watchdog(),
        dexscreener_scanner(),
        update_loop(),
        maintenance_loop(),
        rugpull_monitor(),
    )

if __name__ == "__main__":
    import signal, sys

    def handle_shutdown(sig, frame):
        print(f"\n[Railway] Sinal {sig} recebido - a guardar dados antes de parar...")
        save_weights(WEIGHTS)
        save_pattern_history()
        save_hour_stats()
        save_alerted()
        save_bad_devs()
        save_state()
        print(f"[Railway] Dados guardados. Alertas: {alerts_sent} | Aprendizagens: {learns_done}")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)  # Railway envia SIGTERM ao parar
    signal.signal(signal.SIGINT,  handle_shutdown)  # Ctrl+C local

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        handle_shutdown(None, None)
