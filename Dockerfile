FROM python:3.13.3-slim-bullseye

# Set the working directory in the container to /usr/src/app
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# Install poetry
RUN pip3 install poetry --no-cache-dir && \
  poetry config virtualenvs.create false

# Copy the poetry setup into the container
COPY pyproject.toml poetry.lock ./

# Install all python dependacies
RUN poetry install --no-root

# Copy project directory contents into the container
COPY . .
