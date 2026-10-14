FROM python:3.12-slim
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml uv.lock* README.md ./
COPY src ./src
RUN uv sync --no-dev --no-editable
EXPOSE 8000
CMD ["uv", "run", "uvicorn", "featurestore.api:app", "--host", "0.0.0.0", "--port", "8000"]
