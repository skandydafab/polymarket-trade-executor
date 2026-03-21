FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

ARG INSTALL_SDK_SIGNER=true

# Install optional SDK dependency so live mode can use py-clob-client signing.
RUN python -m pip install --upgrade pip && \
    if [ "$INSTALL_SDK_SIGNER" = "true" ]; then \
        python -m pip install py-clob-client; \
    fi

# Create an unprivileged runtime user.
RUN addgroup --system app && adduser --system --ingroup app app

COPY . /app

RUN mkdir -p /app/data/research /app/data/journal && chown -R app:app /app

USER app

CMD ["python", "examples/run_executor_from_env.py"]
