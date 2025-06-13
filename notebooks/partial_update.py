import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import sqlalchemy

    DATABASE_URL = "timeplus://demo:demo123@34.82.135.191:8123"
    #DATABASE_URL = "timeplus://play.us-west1-a.c.tpdemo2025.internal:8123"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return (engine,)


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        create mutable stream if not exists customers (
               customer_id string,
               email string, phone int,
               newsletter bool,
               last_login datetime,
               family cf1(email,phone),--kafka topic1
               family cf2(newsletter), --kafka topic2
               family cf4(last_login)  --kafka topic3
        ) primary key customer_id
        settings coalesced = true
        """,
        output=False,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ```sql
    create mutable stream if not exists customers (
           customer_id string,
           email string, phone int,
           newsletter bool,
           last_login datetime,
           family cf1(email,phone),--kafka topic1
           family cf2(newsletter), --kafka topic2
           family cf4(last_login)  --kafka topic3
    ) primary key customer_id
    settings coalesced = true;

    select * from customers;
    ```
    """
    )
    return


@app.cell
def _(engine, insert1, insert2, insert3, mo):
    _df = mo.sql(
        f"""
        -- {insert1}
        -- {insert2}
        -- {insert3}
        SELECT * except _tp_time FROM customers
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    # Create a form with multiple elements
    topic1 = (
        mo.md('''
        **Customer Info (kafka topic 1)**

        {customer_id} {email} {phone}
    ''')
        .batch(
            customer_id=mo.ui.text(label="Customer ID",value="c1"),
            email=mo.ui.text(label="Email",value="a@g.com"),
            phone=mo.ui.text(label="Phone", value="123"),
        )
        .form()
    )
    topic1
    return (topic1,)


@app.cell
def _(engine, mo, topic1):
    _j=topic1.value
    insert1=""
    if _j is not None:
        insert1=f"INSERT INTO customers(customer_id,email,phone) VALUES('{_j['customer_id']}','{_j['email']}',{_j['phone']})"
        mo.sql(insert1,engine=engine)
    mo.md(f'''
        ```sql
        {insert1}
        ```
        ''')
    return (insert1,)


@app.cell
def _(mo):
    topic2 = (
        mo.md('''
        **Newsletter (kafka topic 2)**

        {customer_id} {newsletter}
    ''')
        .batch(
            customer_id=mo.ui.text(label="Customer ID",value="c1"),
            newsletter=mo.ui.checkbox(label="Subscribe to newsletter"),
        )
        .form()
    )
    topic2
    return (topic2,)


@app.cell
def _(engine, mo, topic2):
    _j=topic2.value
    insert2=""
    if _j is not None:
        insert2=f"INSERT INTO customers(customer_id,newsletter) VALUES('{_j['customer_id']}',{_j['newsletter']})"
        mo.sql(insert2,engine=engine)
    mo.md(f'''
        ```sql
        {insert2}
        ```
        ''')
    return (insert2,)


@app.cell
def _(mo):
    mo.md('**Login (kafka topic 3)**')
    button = mo.ui.button(
        value=0, on_click=lambda value: value + 1, label="Login", kind="warn"
    )
    mo.hstack([mo.md('**Login (kafka topic 3)**'),button])
    return (button,)


@app.cell
def _(button, engine, mo):
    import datetime
    insert3=""
    if button.value >0:
        insert3=f"""INSERT INTO customers(customer_id,last_login) VALUES('c1','{datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}')
        """
        mo.sql(insert3,engine=engine)
    mo.md(f'''
        ```sql
        {insert3}
        ```
        ''')
    return (insert3,)


if __name__ == "__main__":
    app.run()
