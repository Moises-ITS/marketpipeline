# One image, two roles. The API and the ingest worker share all their code and dependencies and
# differ only in the command they run, so building two images would duplicate a layer cache for
# no benefit. docker-compose.yml sets the command per service.

FROM python:3.11-slim

# Send logs straight out instead of buffering them - otherwise `docker compose logs` shows
# nothing until the buffer fills, which looks exactly like a hung container.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencies are copied and installed BEFORE the source. Docker caches each layer: this way
# editing a .py file rebuilds in seconds, because the pip install layer is unchanged and
# reused. Copying everything at once would reinstall every dependency on every code edit.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY marketdata/ ./marketdata/

# Run as a non-root user. A container process that does not need root should not have it - if
# the app is ever compromised, the blast radius stops at this account.
RUN useradd --create-home --uid 1000 app && chown -R app:app /app
USER app

# Overridden per service in docker-compose.yml.
CMD ["uvicorn", "marketdata.api:app", "--host", "0.0.0.0", "--port", "8000"]
