#!/usr/bin/env python3
"""
Sincronizador do supervisório
Espelha as tabelas do banco do supervisório (SQL Server da RAPFUF10) no Postgres da VPS.

Como funciona, por tabela:
- Primeira vez (tabela vazia ou inexistente no Postgres): copia a tabela inteira.
- Depois, a cada ciclo: relê da origem os últimos N dias (pela coluna de data da tabela)
  e, numa transação só, troca esse mesmo período no Postgres pelo que veio.
  Assim entram as linhas novas e as alteradas (ex.: alarme reconhecido).
- Nunca apaga nada mais antigo que a janela: o histórico fica guardado no Postgres
  mesmo que o supervisório faça limpeza.

Só faz SELECT na origem. Configuração por variáveis de ambiente (ou .env na mesma pasta).
"""
import csv
import io
import logging
import os
import sys
import time

import pg8000.native
import pytds

# tabela de origem -> (coluna da janela, dias da janela)
TABELAS = {
    "PRODUCAO_FBOXII_DB": ("Time_Stamp_INICIO", 2),
    "PRODUCAO_VICK_DB": ("Time_Stamp_INICIO", 2),
    "PRODUCAO_MT1_DB": ("DATA_INICIO", 2),
    "PRODUCAO_MT2_DB": ("DATA_INICIO", 2),
    "ALARMHISTORY": ("Al_Start_Time", 7),
    "EVENTHISTORY": ("Ev_Time", 2),
}

LOTE = 5000

TIPOS = {
    "int": "integer", "bigint": "bigint", "smallint": "smallint", "tinyint": "smallint",
    "bit": "boolean", "real": "real", "float": "double precision",
    "money": "numeric(19,4)", "smallmoney": "numeric(10,4)",
    "date": "date", "time": "time",
    "datetime": "timestamp", "datetime2": "timestamp", "smalldatetime": "timestamp",
    "datetimeoffset": "timestamptz",
    "binary": "bytea", "varbinary": "bytea", "image": "bytea",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("supervisorio")
logging.getLogger("pytds").setLevel(logging.WARNING)


def carregar_env(caminho):
    if not os.path.exists(caminho):
        return
    with open(caminho, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, valor = linha.split("=", 1)
            os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


def cfg(nome, padrao=None):
    valor = os.environ.get(nome, padrao)
    if valor is None:
        log.error("Faltando a configuração %s", nome)
        sys.exit(2)
    return valor


def q(nome):
    """Nome de coluna/tabela no Postgres: minúsculo e entre aspas."""
    return '"' + nome.lower().replace('"', '""') + '"'


def tipo_pg(data_type, precisao, escala):
    t = data_type.lower()
    if t in ("decimal", "numeric"):
        return f"numeric({precisao},{escala})"
    return TIPOS.get(t, "text")  # textos, uniqueidentifier, xml etc.


def valor_csv(v):
    if isinstance(v, (bytes, bytearray)):
        return "\\x" + bytes(v).hex()
    if isinstance(v, str):
        return v.replace("\x00", "")
    return v


def copiar_para_pg(pg, tabela_pg, colunas, linhas):
    """Insere linhas via COPY (rápido). None vira NULL; texto vazio continua texto vazio."""
    if not linhas:
        return
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_NOTNULL, lineterminator="\n")
    for linha in linhas:
        w.writerow([valor_csv(v) for v in linha])
    buf.seek(0)
    cols = ", ".join(q(c) for c in colunas)
    pg.run(f"COPY {tabela_pg} ({cols}) FROM STDIN WITH (FORMAT csv)", stream=buf)


def colunas_origem(ms, tabela):
    with ms.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (tabela,),
        )
        return cur.fetchall()


def preparar_tabela_pg(pg, tabela, cols, coluna_janela):
    """Cria a tabela espelho se não existir e acrescenta colunas novas da origem."""
    tabela_pg = q(tabela)
    defs = ", ".join(f"{q(c)} {tipo_pg(t, p, e)}" for c, t, p, e in cols)
    pg.run(f"CREATE TABLE IF NOT EXISTS {tabela_pg} ({defs}, "
           "synced_at timestamptz NOT NULL DEFAULT now())")
    existentes = {r[0] for r in pg.run(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = :t", t=tabela.lower())}
    for c, t, p, e in cols:
        if c.lower() not in existentes:
            log.info("%s: coluna nova na origem, adicionando %s", tabela, c)
            pg.run(f"ALTER TABLE {tabela_pg} ADD COLUMN {q(c)} {tipo_pg(t, p, e)}")
    indice = q(f"idx_{tabela}_{coluna_janela}"[:63])
    pg.run(f"CREATE INDEX IF NOT EXISTS {indice} ON {tabela_pg} ({q(coluna_janela)})")
    return tabela_pg


def sincronizar_tabela(ms, pg, tabela, coluna_janela, dias):
    cols = colunas_origem(ms, tabela)
    if not cols:
        log.warning("%s: tabela não encontrada na origem, pulando", tabela)
        return
    nomes = [c[0] for c in cols]
    tipo_janela = next(t for c, t, _, _ in cols if c == coluna_janela).lower()
    tabela_pg = preparar_tabela_pg(pg, tabela, cols, coluna_janela)
    select_cols = ", ".join(f"[{c}]" for c in nomes)

    vazia = pg.run(f"SELECT NOT EXISTS (SELECT 1 FROM {tabela_pg})")[0][0]
    forcar = tabela in {t.strip() for t in os.environ.get("RECARGA_COMPLETA", "").split(",")}

    with ms.cursor() as cur:
        if vazia or forcar:
            # Carga completa
            cur.execute(f"SELECT {select_cols} FROM dbo.[{tabela}] WITH (NOLOCK)")
            pg.run("START TRANSACTION")
            try:
                pg.run(f"TRUNCATE {tabela_pg}")
                total = 0
                while True:
                    linhas = cur.fetchmany(LOTE)
                    if not linhas:
                        break
                    copiar_para_pg(pg, tabela_pg, nomes, linhas)
                    total += len(linhas)
                pg.run("COMMIT")
            except Exception:
                pg.run("ROLLBACK")
                raise
            log.info("%s: carga completa, %s linhas", tabela, total)
            return

        # Janela deslizante: o corte vem do relógio da RAPFUF10, o mesmo dos dados
        cur.execute(f"SELECT DATEADD(day, -{int(dias)}, GETDATE())")
        corte = cur.fetchone()[0]
        if tipo_janela == "date":
            corte = corte.date()
        cur.execute(
            f"SELECT {select_cols} FROM dbo.[{tabela}] WITH (NOLOCK) WHERE [{coluna_janela}] >= %s",
            (corte,),
        )
        linhas = cur.fetchall()

    pg.run("START TRANSACTION")
    try:
        apagadas = pg.run(
            f"WITH d AS (DELETE FROM {tabela_pg} WHERE {q(coluna_janela)} >= :corte RETURNING 1) "
            "SELECT count(*) FROM d", corte=corte)[0][0]
        copiar_para_pg(pg, tabela_pg, nomes, linhas)
        pg.run("COMMIT")
    except Exception:
        pg.run("ROLLBACK")
        raise
    log.info("%s: janela desde %s, %s linhas (antes %s)", tabela, corte, len(linhas), apagadas)


def ciclo():
    ms = pytds.connect(
        dsn=cfg("MSSQL_HOST"), port=int(cfg("MSSQL_PORT", "1433")),
        database=cfg("MSSQL_DATABASE"), user=cfg("MSSQL_USER"), password=cfg("MSSQL_PASSWORD"),
        login_timeout=15, timeout=300, autocommit=True,
    )
    pg = pg8000.native.Connection(
        host=cfg("PG_HOST"), port=int(cfg("PG_PORT", "5432")), database=cfg("PG_DATABASE"),
        user=cfg("PG_USER"), password=cfg("PG_PASSWORD"), timeout=300,
    )
    try:
        for tabela, (coluna, dias) in TABELAS.items():
            try:
                sincronizar_tabela(ms, pg, tabela, coluna, dias)
            except Exception as erro:
                log.error("%s: falha (tento de novo no próximo ciclo): %r", tabela, erro)
    finally:
        pg.close()
        ms.close()


def main():
    carregar_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    intervalo = int(cfg("INTERVALO_SEGUNDOS", "600"))
    log.info("Sincronizador do supervisório iniciado. Intervalo: %s segundos", intervalo)
    while True:
        inicio = time.monotonic()
        try:
            ciclo()
        except Exception as erro:
            log.error("Falha no ciclo (tento de novo no próximo): %r", erro)
        time.sleep(max(30, intervalo - (time.monotonic() - inicio)))


if __name__ == "__main__":
    main()
