import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium", app_title="GitHub Real-Time Analytics")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import sqlalchemy
    import altair as alt

    DATABASE_URL = "timeplus://demo:demo123@34.82.135.191:8123"
    #DATABASE_URL = "timeplus://play.us-west1-a.c.tpdemo2025.internal:8123"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return alt, engine, mo


@app.cell
def _(mo):
    mo.md(
        r"""
    # Live GitHub Events
    👋 This is a live notebook, built with [Timeplus](https://github.com/timeplus-io/proton) and [marimo](https://marimo.io), showing streaming data from GitHub via [a public facing Kafka topic](http://kafka.demo.timeplus.com:8080/topics/github_events).

    Source code at [GitHub](https://github.com/timeplus-io/marimo.demo.timeplus.com/blob/main/notebooks/github.py) | [More details of this demo]([https://demos.timeplus.com](https://demos.timeplus.com/#/id/github))
    """
    )
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
        WHERE (type = 'WatchEvent') AND (_tp_time > (now() - {days.value}d))
        GROUP BY repo ORDER BY new_followers DESC LIMIT 20
      ), previous_ranks AS
      (
        SELECT
          repo, rank() OVER (ORDER BY count_distinct(actor) DESC) AS previous_rank
        FROM table(mv_github_events)
        WHERE (type = 'WatchEvent') AND ((_tp_time >= (now() - {2*days.value}d)) AND (_tp_time <= (now() - {days.value}d)))
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
    mo.md(r"""# Explore repos by event type""")
    return


@app.cell
def _(chart_repos, chart_types, mo):
    mo.hstack([chart_types,chart_repos],widths=[0,1])
    return


@app.cell(hide_code=True)
def _(chart_types):
    _type=' '
    if chart_types.selections.get("select_point"):
        _array=chart_types.selections["select_point"].get("type",None)
        if _array:
            _type=f"WHERE type='{_array[0]}'"
    typeWhere=_type
    return (typeWhere,)


@app.cell
def _(alt, engine, mo):
    _df_type = mo.sql(
        f"""
        with cte as(SELECT top_k(type,10,true) as a FROM mv_github_events limit 1)
        select a.1 as type,a.2 as cnt from cte array join a
        """,
        engine=engine,output=False
    )
    chart_types = mo.ui.altair_chart(
        alt.Chart(_df_type, height=150, width=150)
        .mark_arc()
        .encode(theta="cnt", color="type"),
        legend_selection=False
    )
    return (chart_types,)


@app.cell
def _(alt, engine, mo, typeWhere):
    _df_hotrepo = mo.sql(
        f"""
        with cte as(SELECT top_k(repo,10,true) as a FROM mv_github_events {typeWhere} limit 1)
        select a.1 as repo,a.2 as cnt from cte array join a
        """,
        engine=engine,output=False
    )
    chart_repos = mo.ui.altair_chart(
        alt.Chart(_df_hotrepo, height=200)
        .mark_bar()
        .encode(x='cnt',
                y=alt.Y('repo',sort=alt.EncodingSortField(field='cnt',order='descending')),)
    )
    return (chart_repos,)


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


if __name__ == "__main__":
    app.run()
