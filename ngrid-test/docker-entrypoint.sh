#!/bin/sh
# Entrypoint do container ngrid-test.
#
# Opcionalmente aplica latencia de rede (netem) antes de iniciar o no, para
# alargar a janela do simultaneous-open TCP e reproduzir, em rede bridge, o
# cenario de rede real da issue #117. Ativado apenas quando NETEM_DELAY esta
# definido (ex.: "80ms" ou "80ms 20ms distribution normal"); caso contrario o
# comportamento e identico ao entrypoint original (java -jar ...).
#
# Requer capability NET_ADMIN e o pacote iproute2 (ver Dockerfile).
set -e

if [ -n "$NETEM_DELAY" ]; then
    if tc qdisc add dev eth0 root netem delay $NETEM_DELAY 2>/dev/null; then
        echo "NETEM_APPLIED:$NETEM_DELAY on eth0"
    else
        echo "NETEM_FAILED (NET_ADMIN/iproute2 ausentes?)"
    fi
fi

exec java -jar /app/ngrid-test.jar "$@"
