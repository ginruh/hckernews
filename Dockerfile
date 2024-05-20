FROM python:3.12.3-slim as base
RUN useradd --create-home py
USER py
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache
ENV PATH="${PATH}:/home/py/.local/bin"
RUN pip install --user poetry
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root
COPY . .
RUN poetry install


FROM base as prod
WORKDIR /app
COPY --from=base /app /app
CMD ["/app/.venv/bin/app"]