FROM python:3.11.7-slim-bullseye

WORKDIR /application

COPY ./partition_registry /application
COPY ./postgres /application
COPY ./pyproject.toml /application/
COPY ./poetry.lock /application/
COPY ./pylintrc_tests /application/
COPY ./setup.cfg /application/
COPY ./test.sh /application/

ENV PYTHONPATH="/application"

RUN pip install poetry==1.0.0
RUN poetry check
RUN poetry install
