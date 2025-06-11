import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
    # Timeplus Notebooks
    - [Coalesced Mutable Stream - Read data from multiple Kafka topics and create/update a wide table by ID](/partial)
    """
    )
    return


if __name__ == "__main__":
    app.run()
