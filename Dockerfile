FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /app/results

COPY rdap_worker.py .
COPY domains_chunk1.txt domains_chunk1.txt
COPY domains_chunk2.txt domains_chunk2.txt

# Default: chunk 1. Override com ENV CHUNK=2 no EasyPanel
ENV CHUNK=1

# Delay 4.0 = 15 req/min (dentro do limite de 20/min do registro.br)
CMD python3 -u rdap_worker.py domains_chunk${CHUNK}.txt /app/results/rdap_results.txt --delay 4.0
