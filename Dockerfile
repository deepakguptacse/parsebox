FROM python:3.12-slim

WORKDIR /app

# Install system deps + Node.js (needed for Claude CLI)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Claude CLI (required by claude-agent-sdk)
RUN npm install -g @anthropic-ai/claude-code

# Copy project
COPY pyproject.toml .
COPY parsebox/ parsebox/
COPY sample_data/ sample_data/

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Create non-root user (Claude CLI refuses --dangerously-skip-permissions as root)
RUN useradd -m -s /bin/bash parsebox && chown -R parsebox:parsebox /app
USER parsebox

# Tell the server where sample data lives
ENV PARSEBOX_SAMPLE_DATA=/app/sample_data
ENV PORT=8080

EXPOSE ${PORT}

CMD uvicorn parsebox.web.server:create_app --host 0.0.0.0 --port ${PORT} --factory
