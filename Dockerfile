FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends libmagic1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY skeeter_deleter.py ./
COPY docker/run_scheduler.sh /usr/local/bin/run_scheduler.sh

ENTRYPOINT ["run_scheduler.sh"]
