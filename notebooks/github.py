import marimo

__generated_with = "0.14.0"
app = marimo.App(width="medium", app_title="GitHub Real-Time Analytics")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import sqlalchemy
    import altair as alt
    import timeplus_connect
    import pandas as pd

    DATABASE_URL = "timeplus://demo:demo123@34.82.135.191:8123"
    #DATABASE_URL = "timeplus://play.us-west1-a.c.tpdemo2025.internal:8123"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return engine, mo, timeplus_connect


@app.cell
def _(mo):
    mo.md(
        r"""
    # Live GitHub Events
    👋 This is a live notebook, built with [Timeplus](https://github.com/timeplus-io/proton) and [marimo](https://marimo.io), showing streaming data from GitHub via [a public facing Kafka topic](http://kafka.demo.timeplus.com:8080/topics/github_events).

    Source code at [GitHub](https://github.com/timeplus-io/marimo.demo.timeplus.com/blob/main/notebooks/github.py) | [More details of this demo]([https://demos.timeplus.com](https://demos.timeplus.com/#/id/github))

    ## Live Events (10% Sample)
    """
    )
    return


@app.cell
def _(get_events, mo):
    mo.ui.table(data=get_events(),selection=None)
    return


@app.cell
def _(mo):
    cntRefresh = mo.ui.refresh(options=["4s"],default_interval="4s")
    return (cntRefresh,)


@app.cell
def _(cntRefresh):
    cntRefresh.style({"display": None})
    return


@app.cell
def _(mo):
    get_mv_count, set_mv_count = mo.state(0)
    return get_mv_count, set_mv_count


@app.cell
def _(streaming_table):
    streaming_table("""
    select to_string(created_at) as "Created At (UTC)", case 
    when type='PushEvent' then actor||' pushed to'||repo
    when type='CreateEvent' then actor||' created '||repo
    when type='ForkEvent' then actor||' forked '||payload:forkee.full_name||' as '||repo
    when type='PullRequestEvent' then actor||' '||payload:action||'ed pr for '||repo
    when type='WatchEvent' then actor||' added 🌟 to '||repo
    when type='ReleaseEvent' then actor||' released '||repo
    else 'unknown'
    end as "10% Sample Events"
    from github_events where id like '%0' and not type in ('PublicEvent','IssueCommentEvent','IssuesEvent','PullRequestReviewEvent','PullRequestReviewCommentEvent','DeleteEvent') SETTINGS query_mode='streaming'""",limit=100
        )
    return


@app.cell
def _(df_cnt, df_mv_cnt, get_count, get_mv_count, mo, set_count, set_mv_count):
    _new_count = df_cnt["cnt"][0]
    _diff = _new_count - get_count()
    set_count(_new_count)
    _new_mv_count = df_mv_cnt["cnt"][0]
    _diff_mv = _new_mv_count - get_mv_count()
    set_mv_count(_new_mv_count)
    mo.hstack(
        [
            mo.stat(
                _new_count,
                label="The Kafka topic keeps recent 7 days data",
                caption=f"{_diff} events",
                direction="increase" if _diff >= 0 else "decrease",
            ),
            mo.stat(
                _new_mv_count,
                label="Unlimited data materiailzed in Timeplus",
                caption=f"{_diff_mv} events",
                direction="increase" if _diff >= 0 else "decrease",
            ),
        ]
    )
    return


@app.cell
def _(mo):
    days=mo.ui.slider(start=1, stop=10, step=1,show_value=True, value=2)
    return (days,)


@app.cell
def _(days, mo):
    mo.md(f"""## 🔥 Top 20 Projects with most new stars (for last {days} days)""")
    return


@app.cell
def _(days, engine, mo):
    _df = mo.sql(
        f"""
    WITH current_ranks AS
      (
        SELECT
          repo, count_distinct(actor) AS new_followers, rank() OVER (ORDER BY new_followers DESC) AS current_rank
        FROM table(mv_github_events)
        WHERE _tp_time > (now() - {days.value}d) AND type = 'WatchEvent'
        GROUP BY repo ORDER BY new_followers DESC LIMIT 20
      ), previous_ranks AS
      (
        SELECT
          repo, rank() OVER (ORDER BY count_distinct(actor) DESC) AS previous_rank
        FROM table(mv_github_events)
        WHERE ((_tp_time >= (now() - {2*days.value}d)) AND (_tp_time <= (now() - {days.value}d))) AND type = 'WatchEvent'
        GROUP BY repo
      ), top20 AS(
        SELECT
          current.repo as repo, current.new_followers as new_followers, multi_if(previous.previous_rank = 0, '🆕',
    current.current_rank < previous.previous_rank-50, '🚀🚀 (was '||to_string(previous.previous_rank)||')',
    current.current_rank < previous.previous_rank, '🚀 (was '||to_string(previous.previous_rank)||')',
    current.current_rank > previous.previous_rank+8, '🔻🔻 (was '||to_string(previous.previous_rank)||')',
    current.current_rank > previous.previous_rank, '🔻 (was '||to_string(previous.previous_rank)||')', '→') AS trend
        FROM current_ranks AS current
        LEFT JOIN previous_ranks AS previous ON current.repo = previous.repo
        WHERE current.current_rank <= 20
        ORDER BY current.current_rank ASC
      ), enriched AS(
        SELECT gh_api('https://api.github.com/repos/'||repo) as info,
        info:description AS description,to_int32_or_zero(info:stargazers_count) AS stars,if('null'=info:language,'',info:language) AS language,
        * FROM top20
      )
      SELECT repo,new_followers, description, stars, language,trend FROM enriched
        """,
        engine=engine
    )
    _ui = []
    for row in _df.iter_rows():
        repo_link = mo.Html(f'<a style="display: inline-flex; align-items: center; gap: 5px; white-space: nowrap; text-decoration: none; color: #0B66BC;" href="https://github.com/{row[0]}" target="_blank"><img src="https://github.com/{row[0].split('/')[0]}.png" width=20 height=20>{row[0]}</a>')
        _ui.append({
            "Repo":repo_link,"Trend":row[5],
            "Description":row[2],
            "Language":row[4],"Total Stars":f'{row[3]:,}',
            f"Stars for last {days.value} days": f'{row[1]:,}'
        })
    repo_table = mo.ui.table(
        _ui,selection=None,show_column_summaries=False
    )
    repo_table
    return


@app.cell
def _(mo):
    get_count, set_count = mo.state(0)
    return get_count, set_count


@app.cell
def _(cntRefresh, engine, mo):
    df_mv_cnt = mo.sql(
        f"""
        -- {cntRefresh.value}
        SELECT count() as cnt FROM mv_github_events
        """,
        output=False,
        engine=engine
    )
    return (df_mv_cnt,)


@app.cell(hide_code=True)
def cell_cnt(cntRefresh, engine, mo):
    df_cnt = mo.sql(
        f"""
        -- {cntRefresh.value}
        SELECT count() as cnt FROM github_events
        """,
        output=False,
        engine=engine
    )
    return (df_cnt,)


@app.cell
def _(timeplus_connect):
    def get_client():
        return timeplus_connect.get_client(
            host="34.82.135.191",
            port=8123,
            username="demo",
            password="demo123"
        )
    return (get_client,)


@app.cell
def _(get_client, mo, set_events):
    from collections import deque
    five = deque(maxlen=5)
    def _streaming_table(sql: str, limit: int = 5):
        with get_client().query_arrow_stream(
            f"SELECT * FROM ({sql}) LIMIT {limit}"
        ) as _stream:
            for _batch in _stream:
                for row in _batch.to_pylist():
                    five.append(row)
                set_events(list(five))


    def streaming_table(sql: str, limit: int = 5):
        thread = mo.Thread(target=_streaming_table, args=(sql, limit))
        thread.start()
    return (streaming_table,)


@app.cell
def _(mo):
    get_events, set_events = mo.state(value=[])
    return get_events, set_events


if __name__ == "__main__":
    app.run()
