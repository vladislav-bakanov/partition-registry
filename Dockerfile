FROM python:3.11.7-slim-bullseye

ENV POETRY_VERSION=1.7.1
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv
ENV POETRY_CACHE_DIR=/opt/.cache

ENV PYTHONPATH="/application"

WORKDIR /application
COPY ./poetry.lock /application
COPY ./pyproject.toml /application

RUN pip install poetry==1.7.1
RUN poetry config virtualenvs.create false
RUN poetry install

COPY . .
