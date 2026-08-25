FROM python:3.11-slim

# Für opencv-python-headless (Personenerkennung) werden ein paar
# System-Bibliotheken benötigt.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY smarthome_all_in_one.py .

VOLUME ["/app/instance"]
EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/api/health')" || exit 1

CMD ["python3", "smarthome_all_in_one.py"]
