#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
#  GNU GPL v3 (or later) — same license as nishi-utils.
#
"""
ngrid-log-analyzer — analisa os logs de replicação do NGrid (replication-ngrid*.log)
e mostra os eventos relevantes numa tabela de CLI, em uma timeline mesclada e colorida.

Foca nos eventos que importam para debugar a HA (issue tems#9 / D1-D11):
  eleição, step-down, convergência de epoch, promoção (drain), snapshot/sync,
  handback orquestrado (D11), dual-leader (D10c), counter-scale desync (offset de
  linhagem), join-quiesce, reclaim-quiesce, relay forward-gap, fencing, unclean restart,
  lease, e erros. O ruído FINE (Served RELAY_STREAM_FETCH ...) é filtrado.

MODOS DE USO
------------
Interativo (sem argumentos) — pergunta os hosts e, por host, usuário/auth/path:
    ./ngrid-log-analyzer.py

One-liner com chave/agent SSH (vai direto, sem senha):
    ./ngrid-log-analyzer.py \\
        --host root@10.200.20.218:'/app/*/log/replication-ngrid*.log' \\
        --host root@10.200.20.219:'/app/*/log/replication-ngrid*.log'

One-liner com senha (pergunta a senha 1x por host; usa paramiko ou sshpass):
    ./ngrid-log-analyzer.py --ask-pass --host root@10.200.20.218:/app/node-1/log/replication-ngrid.0.log

Arquivos locais (ex.: depois de um scp):
    ./ngrid-log-analyzer.py --file node1.log --file node2.log

Filtros / saída:
    --types handback,election,snapshot   # só estas categorias (ver --list-types)
    --since "2026-06-13 01:00"            # a partir de (>=)
    --until "2026-06-13 02:00"            # até (<=)
    --node node-1                          # só este nó/origem
    --tail 50                             # últimos N eventos
    --raw                                 # anexa a mensagem original
    --no-color                            # desliga ANSI (auto-desliga se não for TTY)
    --no-summary                          # esconde o rodapé com contagens

O alias do nó na coluna NODE é deduzido do path (`/app/node-1/...` -> node-1) ou do host.
Vários segmentos rotacionados (.0/.1/.2/...) podem ser passados via glob; os eventos são
sempre ordenados por timestamp, então a ordem dos arquivos não importa.
"""

import argparse
import getpass
import os
import re
import shutil
import subprocess
import sys

# --------------------------------------------------------------------------------------
# Catálogo de eventos: (categoria, cor, regex de classificação, formatter do detalhe).
# A primeira categoria que casar vence — ordem do mais específico para o mais genérico.
# --------------------------------------------------------------------------------------

# Cores ANSI
C = {
    "reset": "\033[0m", "bold": "\033[1m", "dim": "\033[2m",
    "red": "\033[31m", "green": "\033[32m", "yellow": "\033[33m",
    "blue": "\033[34m", "magenta": "\033[35m", "cyan": "\033[36m",
    "bred": "\033[91m", "bgreen": "\033[92m", "byellow": "\033[93m",
    "bcyan": "\033[96m", "bmagenta": "\033[95m",
}


def _epoch(m):
    return m.group(1)


# Cada entrada: (CAT, cor, compiled-regex, lambda match -> detalhe)
EVENT_PATTERNS = [
    # --- Handback orquestrado (D11) — o protagonista da volta ---
    ("HANDBACK", "bcyan", re.compile(r"Affinity handback: (.+?)(?: \(issue.*)?$"),
     lambda m: m.group(1)),
    # --- Dual-leader (D10c) ---
    ("DUAL-RESOLVE", "bmagenta", re.compile(r"Dual-leader resolved: yielding leadership to higher-affinity (\S+)"),
     lambda m: f"YIELD → {m.group(1)} (resync da linhagem do vencedor)"),
    ("DUAL-DETECT", "bred", re.compile(r"Dual-leader detected with (\S+)"),
     lambda m: f"detectado com {m.group(1)}"),
    # --- Counter-scale desync (offset de linhagem — o sintoma do bug da volta) ---
    ("DESYNC", "bred", re.compile(r"peer watermark above its own applied \((\d+) < (\d+)\)"),
     lambda m: f"offset de linhagem: applied={m.group(1)} < peer={m.group(2)} (Δ={int(m.group(2)) - int(m.group(1))})"),
    # --- Eleição / step-down / epoch ---
    ("ELECT", "bgreen", re.compile(r"Leader epoch changed: (\d+) \(elected\)"),
     lambda m: f"→ LÍDER (epoch {m.group(1)})"),
    ("STEP-DOWN", "byellow", re.compile(r"Leader epoch changed: (\d+) \(stepped down\)"),
     lambda m: f"step-down (epoch {m.group(1)})"),
    ("LEASE", "bred", re.compile(r"Leader stepped down\. New epoch: (\d+)"),
     lambda m: f"step-down por lease (epoch {m.group(1)})"),
    ("EPOCH", "blue", re.compile(r"Leader epoch converged to (\d+) \((.+?)\)"),
     lambda m: f"epoch {m.group(1)} ({m.group(2)})"),
    # --- Promoção / drain-gate ---
    ("PROMOTE", "bgreen", re.compile(r"Relay drained on promotion"),
     lambda m: "relay drenado na promoção → write gate liberado"),
    # --- Snapshot / sync ---
    ("SNAP-DONE", "bgreen", re.compile(r"Sync completed for (\S+?)\.? Final sequence: (\d+)"),
     lambda m: f"snapshot CONCLUÍDO {m.group(1)} @seq {m.group(2)}"),
    ("SNAP-START", "yellow", re.compile(r"Starting sync for (\S+) at sequence (\d+)"),
     lambda m: f"snapshot START {m.group(1)} @seq {m.group(2)}"),
    ("SNAP-REQ", "yellow", re.compile(r"Lag detected \((\d+)\)\. Requesting sync for (\S+)"),
     lambda m: f"pede snapshot {m.group(2)} (lag {m.group(1)})"),
    ("SNAP-IGN", "dim", re.compile(r"Ignoring (?:stale sync|snapshot chunk) for (\S+)"),
     lambda m: f"snapshot ignorado {m.group(1)} (stale/late)"),
    # --- Join-quiesce (#129) ---
    ("JOIN-PAUSE", "yellow", re.compile(r"Leader pausing production for joining follower\(s\) \[(.*?)\]"),
     lambda m: f"join-quiesce: pausa p/ {m.group(1)}"),
    ("JOIN-RELEASE", "green", re.compile(r"Join-quiesce released"),
     lambda m: "join-quiesce liberado"),
    ("JOIN-TIMEOUT", "bred", re.compile(r"Join-quiesce exceeded max duration"),
     lambda m: "join-quiesce TIMEOUT (backstop)"),
    # --- Reclaim-quiesce (D10b) ---
    ("RECLAIM", "yellow", re.compile(r"Reclaim-quiesce (engaged|released|exceeded.*)"),
     lambda m: f"reclaim-quiesce {m.group(1)}"),
    # --- Relay gaps / fencing / unclean ---
    ("FWD-GAP", "bred", re.compile(r"(forward-gap|Non-contiguous relay head)"),
     lambda m: "relay forward-gap"),
    ("RE-PULL", "yellow", re.compile(r"re-pulling from the leader binlog"),
     lambda m: "forward-gap curado por re-pull do binlog"),
    ("FENCE", "dim", re.compile(r"Refusing RELAY_STREAM_FETCH from (\S+)"),
     lambda m: f"recusou fetch de {m.group(1)} (não é líder)"),
    ("UNCLEAN", "bred", re.compile(r"Unclean relay restart"),
     lambda m: "unclean restart → bootstrap pendente"),
    ("BOOTSTRAP", "yellow", re.compile(r"Relay bootstrap pending for topic (\S+)"),
     lambda m: f"bootstrap pendente {m.group(1)}"),
]

# Pré-filtro: só linhas que contêm um destes tokens são transferidas/parseadas.
# (Reduz drasticamente o volume — os logs têm spam FINE de Served RELAY_STREAM_FETCH.)
PREFILTER_TOKENS = [
    "Leader epoch", "Relay drained on promotion", "counter-scale desync",
    "Dual-leader", "Affinity handback", "sync for", "Sync completed",
    "Requesting sync", "Lag detected", "Ignoring stale sync", "Ignoring snapshot chunk",
    "quiesce", "forward-gap", "Non-contiguous", "re-pulling from the leader binlog",
    "Refusing RELAY_STREAM_FETCH", "Unclean relay restart", "bootstrap pending",
    "Leader stepped down", "SEVERE", "Exception",
]
# Regex ERE para o grep remoto (escapando os parênteses do conteúdo literal).
PREFILTER_ERE = "|".join(re.escape(t) for t in PREFILTER_TOKENS)

CATEGORY_ORDER = [p[0] for p in EVENT_PATTERNS] + ["ERROR", "OTHER"]

# Linha de log: 2026-06-13 01:07:40.044 [INFO] dev.x.y.Logger mensagem...
LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[(?P<lvl>\w+)\] (?P<logger>[\w.$]+) (?P<msg>.*)$"
)
NODE_RE = re.compile(r"(node[-_]?\d+)")


class Event:
    __slots__ = ("ts", "node", "cat", "color", "detail", "level", "raw", "leader")

    def __init__(self, ts, node, cat, color, detail, level, raw, leader="?"):
        self.ts = ts
        self.node = node
        self.cat = cat
        self.color = color
        self.detail = detail
        self.level = level
        self.raw = raw
        self.leader = leader  # líder corrente derivado da timeline (preenchido por annotate_leader)


def classify(msg):
    """Retorna (categoria, cor, detalhe) para uma mensagem de log."""
    for cat, color, rx, fmt in EVENT_PATTERNS:
        m = rx.search(msg)
        if m:
            try:
                return cat, color, fmt(m)
            except Exception:
                return cat, color, msg
    return None


def parse_rows(rows):
    """rows: lista de (fallback_node, line). 'line' pode vir do grep -H como
    'caminho:logline' (extrai o nó do caminho) ou já ser a logline pura (usa o fallback)."""
    events = []
    for fallback_node, line in rows:
        line = line.rstrip("\n")
        if not line:
            continue
        node = fallback_node
        # grep -H prefixa 'caminho:linha'; o caminho não tem ':' e a linha começa com a data.
        if not line.startswith("20") and ":" in line:
            prefix, rest = line.split(":", 1)
            nm = NODE_RE.search(prefix)
            if nm:
                node = nm.group(1)
            line = rest
        m = LINE_RE.match(line)
        if not m:
            continue
        msg = m.group("msg")
        level = m.group("lvl")
        res = classify(msg)
        if res is None:
            if level in ("SEVERE", "ERROR") or "Exception" in msg:
                cat, color, detail = "ERROR", "bred", msg[:200]
            else:
                continue  # token de pré-filtro casou mas não é evento de interesse
        else:
            cat, color, detail = res
        events.append(Event(m.group("ts"), node, cat, color, detail, level, msg))
    return events


# --------------------------------------------------------------------------------------
# Coleta (local / SSH)
# --------------------------------------------------------------------------------------

def fetch_local(path):
    out = []
    for p in _expand_local(path):
        node = _node_from_path(p) or os.path.basename(p)
        try:
            with open(p, "r", errors="replace") as fh:
                for line in fh:
                    if any(tok in line for tok in PREFILTER_TOKENS):
                        out.append((node, line))
        except OSError as e:
            print(f"[aviso] não li {p}: {e}", file=sys.stderr)
    return out


def _expand_local(path):
    import glob
    matches = glob.glob(path)
    return sorted(matches) if matches else [path]


def _node_from_path(path):
    m = NODE_RE.search(path)
    return m.group(1) if m else None


def _build_remote_cmd(remote_path):
    # grep -H (com nome do arquivo, p/ deduzir o nó), -a (trata binário como texto),
    # -E (ERE). O glob é expandido pelo shell REMOTO. 2>/dev/null engole 'No such file'.
    return f"grep -HaE '{PREFILTER_ERE}' {remote_path} 2>/dev/null"


def fetch_ssh_key(user, host, remote_path, port):
    """Via OpenSSH (chave/agent). Retorna lista de (node_fallback, raw_line)."""
    target = f"{user}@{host}" if user else host
    cmd = [
        "ssh", "-p", str(port),
        "-o", "ConnectTimeout=10",
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",  # falha rápido se exigir senha (não trava esperando)
        target, _build_remote_cmd(remote_path),
    ]
    return _run_capture(cmd, host)


def fetch_ssh_sshpass(user, host, remote_path, port, password):
    target = f"{user}@{host}" if user else host
    cmd = [
        "sshpass", "-p", password, "ssh", "-p", str(port),
        "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=no",
        target, _build_remote_cmd(remote_path),
    ]
    return _run_capture(cmd, host)


def fetch_ssh_paramiko(user, host, remote_path, port, password):
    import paramiko  # import tardio: só quando há senha e sshpass ausente
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    out = []
    try:
        client.connect(host, port=port, username=user, password=password, timeout=10,
                       allow_agent=False, look_for_keys=False)
        _, stdout, stderr = client.exec_command(_build_remote_cmd(remote_path), timeout=120)
        data = stdout.read().decode("utf-8", errors="replace")
        for line in data.splitlines():
            out.append((host, line))
    finally:
        client.close()
    return out


def _run_capture(cmd, host_fallback):
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    except FileNotFoundError as e:
        raise RuntimeError(f"comando ausente: {e}")
    except subprocess.TimeoutExpired:
        raise RuntimeError("timeout no SSH")
    if proc.returncode not in (0, 1):  # grep retorna 1 quando não casa nada (ok)
        msg = (proc.stderr or "").strip().splitlines()
        raise RuntimeError(msg[-1] if msg else f"ssh falhou (rc={proc.returncode})")
    return [(host_fallback, line) for line in proc.stdout.splitlines()]


# --------------------------------------------------------------------------------------
# Renderização da tabela
# --------------------------------------------------------------------------------------

def colorize(text, color, enabled):
    if not enabled or not color:
        return text
    return f"{C.get(color, '')}{text}{C['reset']}"


def render_table(events, use_color, show_raw):
    if not events:
        print("Nenhum evento encontrado (com os filtros atuais).")
        return
    term_w = shutil.get_terminal_size((140, 40)).columns
    ts_w, node_w, lead_w, cat_w = 23, 9, 9, 12
    fixed = ts_w + node_w + lead_w + cat_w + 4 * 3  # 4 separadores " │ "
    detail_w = max(20, term_w - fixed)

    def cell(s, w):
        s = s if len(s) <= w else s[: w - 1] + "…"
        return s.ljust(w)

    header = (
        colorize(cell("TIME", ts_w), "bold", use_color) + " │ " +
        colorize(cell("NODE", node_w), "bold", use_color) + " │ " +
        colorize(cell("LEADER", lead_w), "bold", use_color) + " │ " +
        colorize(cell("EVENT", cat_w), "bold", use_color) + " │ " +
        colorize(cell("DETAIL", detail_w), "bold", use_color)
    )
    print(header)
    print("─" * min(term_w, ts_w + node_w + lead_w + cat_w + detail_w + 12))
    for ev in events:
        detail = ev.detail if not show_raw else ev.raw
        lead_color = "green" if ev.leader != "?" else "dim"
        line = (
            cell(ev.ts, ts_w) + " │ " +
            colorize(cell(ev.node, node_w), "cyan", use_color) + " │ " +
            colorize(cell(ev.leader, lead_w), lead_color, use_color) + " │ " +
            colorize(cell(ev.cat, cat_w), ev.color, use_color) + " │ " +
            colorize(cell(detail, detail_w), ev.color, use_color)
        )
        print(line)


def render_summary(events, use_color):
    if not events:
        return
    from collections import Counter
    by_cat = Counter(ev.cat for ev in events)
    by_node = Counter(ev.node for ev in events)
    print()
    print(colorize("Resumo por evento:", "bold", use_color))
    for cat in CATEGORY_ORDER:
        if cat in by_cat:
            color = next((p[1] for p in EVENT_PATTERNS if p[0] == cat), "red" if cat == "ERROR" else None)
            print(f"  {colorize(cat.ljust(13), color, use_color)} {by_cat[cat]:>5}")
    print(colorize("Resumo por nó:", "bold", use_color))
    for node, n in sorted(by_node.items()):
        print(f"  {colorize(node.ljust(13), 'cyan', use_color)} {n:>5}")
    span = f"{events[0].ts}  →  {events[-1].ts}" if events else "-"
    print(colorize("Janela:", "bold", use_color), span, f"({len(events)} eventos)")
    # Dica de diagnóstico do bug da volta (offset de linhagem):
    desync = by_cat.get("DESYNC", 0)
    if desync:
        print(colorize(f"\n⚠ {desync} evento(s) DESYNC (offset de linhagem) — sintoma da volta lossy "
                       "pré-D11. Com o handback (D11) isso deve ser 0.", "bred", use_color))


# --------------------------------------------------------------------------------------
# Interativo / args
# --------------------------------------------------------------------------------------

DEFAULT_REMOTE_GLOB = "/app/*/log/replication-ngrid*.log"


def interactive_sources():
    """Wizard: pergunta hosts e, por host, usuário/auth/path. Retorna lista de coletas."""
    print("== ngrid-log-analyzer (modo interativo) ==")
    raw = input("Hosts/IPs (separados por espaço ou vírgula; ENTER p/ arquivo local): ").strip()
    collected = []
    if not raw:
        files = input("Arquivo(s) de log local (espaço-separado, aceita glob): ").strip()
        for f in files.split():
            collected.append(("local", f, None, None, None))
        return collected
    hosts = [h for h in re.split(r"[\s,]+", raw) if h]
    default_user = "root"
    for host in hosts:
        print(f"\n--- {host} ---")
        user = input(f"  usuário [{default_user}]: ").strip() or default_user
        default_user = user
        auth = input("  auth — [k] chave/agent (default) ou [s] senha: ").strip().lower()
        password = None
        if auth in ("s", "senha", "p", "pass"):
            password = getpass.getpass("  senha: ")
        path = input(f"  path do log [{DEFAULT_REMOTE_GLOB}]: ").strip() or DEFAULT_REMOTE_GLOB
        port_s = input("  porta SSH [22]: ").strip() or "22"
        collected.append(("ssh", host, user, password, path, int(port_s)))
    return collected


def parse_host_arg(spec):
    """[user@]host[:port?][:path]  — separa user, host, path. Porta via --port global."""
    user = None
    host = spec
    path = DEFAULT_REMOTE_GLOB
    if "@" in host:
        user, host = host.split("@", 1)
    # path é tudo após o PRIMEIRO ':' que vier depois do host (paths começam com '/')
    if ":" in host:
        host, path = host.split(":", 1)
    return user, host, path


def collect_events(args):
    sources = []
    if args.file:
        for f in args.file:
            sources.append(("local", f))
    for spec in (args.host or []):
        user, host, path = parse_host_arg(spec)
        sources.append(("ssh", host, user, None, path, args.port))
    if not sources:
        # modo interativo
        for s in interactive_sources():
            sources.append(s)

    events = []
    for src in sources:
        kind = src[0]
        try:
            if kind == "local":
                rows = fetch_local(src[1])  # já devolve (node, raw_line)
                events.extend(parse_rows(rows))
                continue
            # ssh: (kind, host, user, password, path[, port])
            _, host, user, password, path = src[:5]
            port = src[5] if len(src) > 5 else args.port
            if password is None and args.ask_pass:
                password = getpass.getpass(f"senha p/ {user or ''}@{host}: ")
            print(f"[coletando] {user or ''}@{host}:{path} ...", file=sys.stderr)
            if password:
                if shutil.which("sshpass"):
                    rows = fetch_ssh_sshpass(user, host, path, port, password)
                else:
                    rows = fetch_ssh_paramiko(user, host, path, port, password)
            else:
                rows = fetch_ssh_key(user, host, path, port)
            events.extend(parse_rows(rows))  # grep -H → o nó sai do caminho
        except Exception as e:
            print(f"[erro] origem {src[1] if len(src) > 1 else src}: {e}", file=sys.stderr)
    return events


def annotate_leader(events):
    """Deriva o líder corrente percorrendo a timeline (eventos JÁ ordenados por ts) e anota
    cada evento com o líder vigente NAQUELE ponto. Roda sobre o conjunto COMPLETO, antes de
    qualquer filtro, para que --types não remova os eventos de liderança que alimentam a derivação.

    Regra: ELECT / PROMOTE / handback 'asserting leadership' definem o líder = nó do evento;
    STEP-DOWN / LEASE do líder corrente o zeram ('?') até a próxima eleição.

    Durante um handback (entre 'PREP/granting' e 'asserting'/step-down do incumbente) o incumbente
    permanece o líder e o churn transitório de eleição do candidato (auto-eleição de boot que logo
    deferiu) é ignorado — assim a coluna mostra o incumbente servindo o snapshot, não '?'.
    """
    cur = None
    handover_incumbent = None  # incumbente enquanto um handback está em curso
    for ev in events:
        is_hb = ev.cat == "HANDBACK"
        if is_hb and ("PREP for candidate" in ev.detail or "granting handover" in ev.detail):
            cur = ev.node               # incumbente cedendo; segue líder até o candidato assumir
            handover_incumbent = ev.node
        elif is_hb and "asserting leadership" in ev.detail:
            cur = ev.node               # candidato assumiu no cutover
            handover_incumbent = None
        elif handover_incumbent is not None:
            # handback em curso: ignora o churn de ELECT/STEP-DOWN do candidato; só o step-down do
            # próprio incumbente encerra a janela (cai para '?' até o 'asserting' do candidato).
            if ev.cat in ("STEP-DOWN", "LEASE") and ev.node == handover_incumbent:
                cur = None
                handover_incumbent = None
        elif ev.cat in ("ELECT", "PROMOTE"):
            cur = ev.node
        elif ev.cat in ("STEP-DOWN", "LEASE") and ev.node == cur:
            cur = None
        ev.leader = cur or "?"


def apply_filters(events, args):
    if args.types:
        wanted = {t.strip().upper() for t in args.types.split(",")}
        events = [e for e in events if e.cat.upper() in wanted]
    if args.node:
        events = [e for e in events if args.node.lower() in e.node.lower()]
    if args.since:
        events = [e for e in events if e.ts >= args.since]
    if args.until:
        events = [e for e in events if e.ts <= args.until]
    events.sort(key=lambda e: e.ts)
    # dedup (mesmo ts+node+detail) — útil quando o glob pega segmentos sobrepostos
    seen = set()
    deduped = []
    for e in events:
        key = (e.ts, e.node, e.detail)
        if key not in seen:
            seen.add(key)
            deduped.append(e)
    events = deduped
    if args.tail:
        events = events[-args.tail:]
    return events


def main():
    ap = argparse.ArgumentParser(
        description="Analisa logs de replicação do NGrid e mostra os eventos numa tabela CLI.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--host", action="append", metavar="[user@]host[:path]",
                    help="host SSH (chave/agent). Repetível. path default: " + DEFAULT_REMOTE_GLOB)
    ap.add_argument("--file", action="append", metavar="PATH", help="arquivo de log local (aceita glob). Repetível.")
    ap.add_argument("--ask-pass", action="store_true", help="pergunta a senha SSH por host (usa sshpass ou paramiko)")
    ap.add_argument("--port", type=int, default=22, help="porta SSH (default 22)")
    ap.add_argument("--types", help="filtra categorias (csv). Ex: handback,election,snapshot")
    ap.add_argument("--node", help="filtra por nó/origem (substring). Ex: node-1")
    ap.add_argument("--since", help='timestamp mínimo "YYYY-MM-DD HH:MM[:SS]"')
    ap.add_argument("--until", help='timestamp máximo "YYYY-MM-DD HH:MM[:SS]"')
    ap.add_argument("--tail", type=int, help="só os últimos N eventos")
    ap.add_argument("--raw", action="store_true", help="mostra a mensagem original em vez do detalhe resumido")
    ap.add_argument("--no-color", action="store_true", help="desliga cores ANSI")
    ap.add_argument("--no-summary", action="store_true", help="esconde o rodapé de resumo")
    ap.add_argument("--list-types", action="store_true", help="lista as categorias de evento e sai")
    args = ap.parse_args()

    if args.list_types:
        print("Categorias de evento:")
        for cat, color, _, _ in EVENT_PATTERNS:
            print(f"  {cat}")
        print("  ERROR\n  (use nomes em --types, csv, case-insensitive)")
        return 0

    use_color = sys.stdout.isatty() and not args.no_color and os.environ.get("NO_COLOR") is None

    events = collect_events(args)
    events.sort(key=lambda e: e.ts)
    annotate_leader(events)  # deriva o líder na timeline COMPLETA, antes dos filtros
    events = apply_filters(events, args)
    render_table(events, use_color, args.raw)
    if not args.no_summary:
        render_summary(events, use_color)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrompido.", file=sys.stderr)
        sys.exit(130)
