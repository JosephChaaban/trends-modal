import os
import uuid
import datetime as dt
import re
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import modal

image = (
    modal.Image.debian_slim(python_version="3.11")
    .uv_pip_install(
        "psycopg[binary]>=3.2",
        "pydantic>=2.6",
        "pytrends==4.9.2",
        "pandas>=2.1",
        "urllib3==1.26.20",
        "duckdb>=1.0.0",
    )
)

app = modal.App("trends-test")
Secret = modal.Secret
Volume = modal.Volume

def dsn() -> str:
    url = os.environ["NEON_DATABASE_URL"]
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url


def dsn_for_duckdb() -> str:
    
    url = os.environ["NEON_DATABASE_URL"]
    u = urlparse(url)
    qs = dict(parse_qsl(u.query, keep_blank_values=True))
    if qs.get("channel_binding", "").lower() == "require":
        qs["channel_binding"] = "prefer"
    if "sslmode" not in qs:
        qs["sslmode"] = "require"
    return urlunparse(u._replace(query=urlencode(qs)))


def slug(text: str, limit: int = 80) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "-", text.strip().lower()) 
    s = re.sub(r"-{2,}", "-", s).strip("-")                  
    return s[:limit] or "na"

def unique_keywords(seq: list[str]) -> list[str]:
    seen = set()
    out = []
    for k in (str(x).strip() for x in seq or []):
        if not k:
            continue
        key = k.casefold()
        if key not in seen:
            seen.add(key)
            out.append(k)
    return out


def write_staging_to_parquet(run_id: str, out_path: str):
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;") 

    rel = con.sql(
        """
        SELECT
          keyword, geo, date, value, is_partial,
          run_id, loaded_at, mode, timeframe, geo_list, keyword_list
        FROM postgres_scan(?, 'public', 'staging_trends_interest')
        WHERE CAST(run_id AS VARCHAR) = ?
        """,
        params=[dsn_for_duckdb(), str(run_id)],
    )
    
    rel.write_parquet(out_path)
    con.close()

@app.function(image=image, secrets=[Secret.from_name("neon-dsn")])
def create_tables():
    import psycopg
    with psycopg.connect(dsn(), autocommit=True) as conn:
        conn.execute("""
        create table if not exists trends_interest (
            keyword     text not null,
            geo         text not null,
            date        date not null,
            value       int  not null,
            is_partial  boolean not null default false,
            primary key (keyword, geo, date)
        );
        """)
        conn.execute("""
        create table if not exists staging_trends_interest (
            keyword        text not null,
            geo            text not null,
            date           date not null,
            value          int  not null,
            is_partial     boolean not null,
            -- run metadata
            run_id         uuid not null,
            loaded_at      timestamptz not null default now(),
            mode           text not null,         
            timeframe      text not null,              
            geo_list       jsonb not null,            
            keyword_list   jsonb not null              
        );
        """)
        conn.execute("create index if not exists ix_staging_run_id on staging_trends_interest(run_id);")

def fetch_df(keywords: list[str], geo: str):
    from pytrends.request import TrendReq
    import pandas as pd

    pytrends = TrendReq(hl="en-US", tz=0, retries=2, backoff_factor=0.25)
    pytrends.build_payload(keywords, timeframe="today 3-m", geo=geo or "")

    df = pytrends.interest_over_time()
    if df is None or df.empty:
        return pd.DataFrame(columns=["keyword","geo","date","value","is_partial"])

    wide = df.reset_index(names="date")
    if "isPartial" in wide.columns:
        partial = wide[["date", "isPartial"]].rename(columns={"isPartial": "is_partial"})
    else:
        partial = pd.DataFrame({"date": wide["date"], "is_partial": False})

    long = wide.melt(id_vars=["date"], value_vars=keywords,
                     var_name="keyword", value_name="value")
    out = long.merge(partial, on="date", how="left")
    out["geo"] = geo or "GLOBAL"
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["value"] = out["value"].astype(int)
    out["is_partial"] = out["is_partial"].astype(bool)
    return out[["keyword","geo","date","value","is_partial"]]


def duckdb_filter(df_all, days: int):
    import duckdb
    import pandas as pd

    if df_all is None or len(df_all) == 0:
        return df_all

    latest = pd.to_datetime(df_all["date"]).max().date()
    cut_date = latest - dt.timedelta(days=days - 1)

    con = duckdb.connect()
    con.register("raw", df_all)
    df_clean = con.execute(
        """
        with typed as (
          select
            cast(keyword as varchar)  as keyword,
            cast(geo as varchar)      as geo,
            cast(date as date)        as date,
            cast(value as int)        as value,
            coalesce(cast(is_partial as boolean), false) as is_partial
          from raw
        )
        select keyword, geo, date, value, is_partial
        from typed
        where date >= ?
        order by keyword, geo, date
        """,
        [cut_date],
    ).df()
    con.close()
    return df_clean


def validate_with_pydantic(df):
    from pydantic import BaseModel, Field, TypeAdapter, ValidationError
    class TrendRow(BaseModel):
        keyword: str
        geo: str
        date: dt.date
        value: int = Field(ge=0, le=100)
        is_partial: bool = False
    if df is None or df.empty:
        return []
    records = df.to_dict(orient="records")
    try:
        adapter = TypeAdapter(list[TrendRow])
        validated = adapter.validate_python(records)
    except ValidationError as e:
        raise ValueError(f"Pydantic validation failed: {e}") from e
    return [(r.keyword, r.geo, r.date, r.value, r.is_partial) for r in validated]

@app.function(image=image, secrets=[Secret.from_name("neon-dsn")], timeout=600, volumes={"/data": Volume.from_name("trends_semi_raw", create_if_missing=True)})
def upsert_trends(
    keywords: list[str] | str = ["football"],
    geos: list[str] = ["US"],
    mode: str = "full",
    writeraw: str = "none",
):
    import psycopg
    import pandas as pd
    from psycopg.types.json import Jsonb

    days = 90 if mode == "full" else 7
    
    create_tables.remote()

    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split(",") if k.strip()]
    keywords = unique_keywords(keywords)

    if len(keywords) == 0:
        raise ValueError("Please provide at least one keyword.")
    if len(keywords) > 5:
        raise ValueError("Please pass at most 5 keywords. Remove extras and retry.")

    dfs = []
    
    geos = list(dict.fromkeys(geos))
    
    for g in geos:
        dfs.append(fetch_df(keywords, g))
    df_all = pd.concat(dfs, ignore_index=True) if dfs else None
    df_stage = duckdb_filter(df_all, days)


    run_id = uuid.uuid4()
    
    timeframe = "today 3-m"
    
    written_path = None
    
    staged = validate_with_pydantic(df_stage)
    
    if not staged:
        return {"upserted": 0, "keywords": keywords, "geos": geos, "mode": mode}

    with psycopg.connect(dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            with cur.copy("""
            COPY staging_trends_interest
            (keyword, geo, date, value, is_partial,
            run_id, mode, timeframe, geo_list, keyword_list)
            FROM STDIN
            """) as cp:
                for row in staged: 
                    cp.write_row((
                        *row,
                        str(run_id),
                        mode,
                        timeframe,
                        Jsonb(sorted(geos)),
                        Jsonb(keywords)
                    ))

            cur.execute("""
            merge into trends_interest as t
            using (
                select keyword, geo, date, value, is_partial
                from staging_trends_interest
                where run_id = %s
            ) as s
            on (t.keyword = s.keyword and t.geo = s.geo and t.date = s.date)
            when matched then
                update set value = s.value, is_partial = s.is_partial
            when not matched then
                insert (keyword, geo, date, value, is_partial)
                values (s.keyword, s.geo, s.date, s.value, s.is_partial);
            """, (str(run_id),))

    if df_stage is not None and not df_stage.empty and writeraw == "volume":
        date_dir = dt.datetime.utcnow().strftime("%Y%m%d")
        geo_tag = slug("-".join(geos))
        kw_tag  = slug(",".join(keywords), 80)
        default_target = f"/data/trends/{date_dir}/trends_{geo_tag}_{kw_tag}_{days}d_{run_id}.parquet"
        base_dir, fname = os.path.split(default_target)
        os.makedirs(base_dir, exist_ok=True)
        written_path = os.path.join(base_dir, fname)

        write_staging_to_parquet(str(run_id), written_path)

    return {"upserted": len(staged), "keywords": keywords, "geos": geos, "mode": mode, "run_id": str(run_id), "raw_file": written_path}

@app.local_entrypoint()
def main(
    keywords: str = "football",
    geos: str = "US",
    mode: str = "full",
    writeraw: str = "none"
):
    geos_list = [g.strip() for g in geos.split(",") if g.strip()]
    result = upsert_trends.remote(keywords, geos_list, mode, writeraw)
    print(result)
