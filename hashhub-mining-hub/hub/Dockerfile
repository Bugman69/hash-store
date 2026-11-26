FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY app.py ./
COPY static ./static
COPY coins ./coins
ENV HUB_DATA=/data PYTHONUNBUFFERED=1
EXPOSE 8420
HEALTHCHECK --interval=30s --timeout=5s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8420/api/state',timeout=4)"
CMD ["python", "/app/app.py"]
