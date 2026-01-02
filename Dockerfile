FROM apache/airflow:2.9.1

# 1) OS toolchain for PyTensor / SciPy stack
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc g++ build-essential libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

# (optional but nice) verify g++
RUN g++ --version

# 2) Back to airflow user
USER airflow

# Install dependencies
WORKDIR /app


# Copy Poetry files
COPY pyproject.toml poetry.lock /app/

# Install Poetry
COPY pyproject.toml poetry.lock /app/
RUN pip install --no-cache-dir poetry \
 && poetry config virtualenvs.create false \
 && poetry install --no-root


RUN poetry config virtualenvs.create false && poetry install --no-root

ENV PYTENSOR_FLAGS="cxx=/usr/bin/g++"
