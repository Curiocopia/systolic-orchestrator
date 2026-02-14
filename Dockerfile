# -----------------------------------------------------------------------------
# Base image
# -----------------------------------------------------------------------------
FROM python:3.11-slim

# Prevent python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# -----------------------------------------------------------------------------
# Create non-root user
# -----------------------------------------------------------------------------
RUN addgroup --system appgroup \
    && adduser --system --ingroup appgroup --home /app appuser

# -----------------------------------------------------------------------------
# Workdir
# -----------------------------------------------------------------------------
WORKDIR /app

# -----------------------------------------------------------------------------
# Install dependencies
# -----------------------------------------------------------------------------
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------------
# Copy application
# -----------------------------------------------------------------------------
COPY ./app/systolic_orchestrator.py .

COPY ./app/ui ./ui
ENV UI_DIR=/app/ui

RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# -----------------------------------------------------------------------------
# Runtime configuration
# -----------------------------------------------------------------------------
EXPOSE 8080

# Default port (can be overridden by env)
ENV UVICORN_PORT=8080

# -----------------------------------------------------------------------------
# Start service
# -----------------------------------------------------------------------------
CMD ["uvicorn", "systolic_orchestrator:app", "--host", "0.0.0.0", "--port", "8080"]

