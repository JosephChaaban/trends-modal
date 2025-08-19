FROM python:3.11-slim

RUN pip install --no-cache-dir modal

COPY app.py /app.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV KEYWORDS="football" \
    GEOS="US" \
    MODE="full" \
    WRITERAW="none" \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
