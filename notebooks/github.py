import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


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


@app.cell(hide_code=True)
def _(mo):
    range=mo.ui.slider(start=1, stop=10, step=1,label="View data for X minutes:", show_value=True)
    return (range,)


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
    refresh = mo.ui.refresh(label="Refresh",options=["2s", "5s", "10s"])
    return (refresh,)


@app.cell
def _(mo):
    days=mo.ui.slider(start=1, stop=10, step=1,show_value=True, value=7)
    return (days,)


@app.cell
def _(days, mo):
    mo.md(f"""## 🔥 Top 20 Projects with most new stars (for last {days} days)""")
    return


@app.cell
def _(days, engine, mo):
    _df = mo.sql(
        f"""
        SELECT 'https://github.com/'||repo as HotRepo, new_followers as StarLast{days.value}d FROM(
        SELECT repo, count(distinct actor) AS new_followers
        FROM table(mv_github_events) WHERE type ='WatchEvent' and _tp_time>now()-{days.value}d
        GROUP BY repo ORDER BY new_followers desc
        limit 20)
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""# Explore data via slider and charts""")
    return


@app.cell
def _(chart_repos, chart_types, mo, range, refresh):
    mo.vstack([mo.hstack([range,refresh]),mo.hstack([chart_types,chart_repos],widths=[0,1])])
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
def _(alt, df_type, mo):
    chart_types = mo.ui.altair_chart(
        alt.Chart(df_type, height=150, width=150)
        .mark_arc()
        .encode(theta="cnt", color="type"),
        legend_selection=False
    )
    return (chart_types,)


@app.cell
def _(alt, df_hotrepo, mo):
    chart_repos = mo.ui.altair_chart(
        alt.Chart(df_hotrepo, height=200)
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


@app.cell
def _(engine, mo, range, refresh, typeWhere):
    df_hotrepo = mo.sql(
        f"""
        -- {refresh.value}
        with cte as(SELECT top_k(repo,10,true) as a FROM mv_github_events {typeWhere} limit 1 SETTINGS seek_to='-{range.value}m')
        select a.1 as repo,a.2 as cnt from cte array join a
        """,
        engine=engine
    )
    return (df_hotrepo,)


@app.cell(hide_code=True)
def _(engine, mo, range, refresh):
    df_type = mo.sql(
        f"""
        -- {refresh.value}
        with cte as(SELECT top_k(type,10,true) as a FROM mv_github_events limit 1 SETTINGS seek_to='-{range.value}m')
        select a.1 as type,a.2 as cnt from cte array join a
        """,
        engine=engine
    )
    return (df_type,)


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
