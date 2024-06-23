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

FROM base as build
RUN pip install --user poetry
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root
COPY . .
RUN poetry install

FROM base as prod
USER root
RUN apt update -y
RUN apt install cron -y
# run at 00:00, 06:00, 12:00, 18:00
RUN echo "0 0,6,12,18 * * * root /app/.venv/bin/top_stories > /proc/1/fd/1 2>&1\n" >> /etc/cron.d/topstories
RUN chmod 0644 /etc/cron.d/topstories

USER py
WORKDIR /app
COPY --from=build /app /app

USER root
CMD cron -f -l 2