FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends     tmux asciinema gawk coreutils procps ca-certificates tzdata locales  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
