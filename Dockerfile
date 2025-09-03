FROM python:3.11-buster AS builder

RUN pip install poetry==2.1.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml .
RUN touch README.md

RUN --mount=type=cache,target=$POETRY_CACHE_DIR poetry install --without dev --no-root

FROM python:3.11-slim-buster AS runtime

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app


WORKDIR /app

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
# HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import socket; s=socket.socket(); s.connect(('127.0.0.1', 60666))" || exit 1
COPY mictlanxrouter ./mictlanxrouter


# # 
# FROM python:3.10

# # 
# WORKDIR /app

# # 
# COPY ./requirements.txt /app/requirements.txt
# RUN pip install --extra-index-url https://test.pypi.org/simple  -r /app/requirements.txt
# #
# COPY ./pyproject.toml /app
# COPY ./mictlanxrouter /app/mictlanxrouter
# #COPY ./mictlanx.tar.gz /app/x.tar.gz
# #RUN pip install /app/x.tar.gz


# ENV MICTLANX_ROUTER_PORT=60666
# ENV MICTLANX_ROUTER_HOST=0.0.0.0

# COPY ./mictlanxrouter/interfaces /app/interfaces 
# COPY ./mictlanxrouter/helpers /app/helpers

# 
# CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "60666"]
# CMD ["python3","./mictlanxrouter/server.py"]
# CMD ["sleep","infinity"]
# CMD ["uvicorn", "mictlanxrouter.server:app","--host",$MICTLANX_ROUTER_HOST,"--port",$MICTLANX_ROUTER_PORT]

