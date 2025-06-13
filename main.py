from typing import Annotated, Callable, Coroutine
from fastapi.responses import HTMLResponse, RedirectResponse
import marimo
from fastapi import FastAPI, Form, Request, Response

server = (
    marimo.create_asgi_app()
    .with_app(path="/", root="./index.py")
    .with_app(path="/partial", root="./notebooks/partial_update.py")
)
app = FastAPI()

app.mount("/", server.build())

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8080)
