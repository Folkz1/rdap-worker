"""Brasil API enrichment worker - get telefone, socios, endereco for CNPJs."""
import urllib.request
import json
import os
import sys
import time
import csv
import re
import threading
import http.server

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

START_IDX = int(os.environ.get('START_IDX', 0))
END_IDX = int(os.environ.get('END_IDX', 1000))
INPUT_FILE = os.environ.get('INPUT_FILE', '/app/data/cnpjs.csv')
OUT = f'/app/results/enriched_{START_IDX}_{END_IDX}.json'
STATUS_FILE = f'/app/results/status_{START_IDX}.txt'

os.makedirs('/app/results', exist_ok=True)

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'ok\n')
        elif self.path == '/status':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            if os.path.exists(STATUS_FILE):
                self.wfile.write(open(STATUS_FILE, 'rb').read())
        elif self.path == '/results':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            if os.path.exists(OUT):
                self.wfile.write(open(OUT, 'rb').read())
            else:
                self.wfile.write(b'[]\n')
        else:
            self.send_response(404)
            self.end_headers()
    def log_message(self, *a): pass

server = http.server.HTTPServer(('0.0.0.0', 3000), Handler)
print(f"HTTP :3000 | idx {START_IDX}-{END_IDX}", flush=True)
threading.Thread(target=server.serve_forever, daemon=True).start()

# Load rows
rows = []
with open(INPUT_FILE, encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        if START_IDX <= i < END_IDX:
            rows.append(row)

print(f"Loaded {len(rows)} rows to enrich", flush=True)

results = []
# Resume from existing
if os.path.exists(OUT):
    existing = json.load(open(OUT, encoding='utf-8'))
    done_cnpjs = {e['cnpj_raw'] for e in existing}
    results = existing
    print(f"Resume: {len(results)} already done", flush=True)
else:
    done_cnpjs = set()

errors = 0
for i, row in enumerate(rows):
    cnpj_raw = re.sub(r'\D', '', row.get('cnpj', ''))
    if not cnpj_raw or cnpj_raw in done_cnpjs:
        continue
    
    url = f'https://brasilapi.com.br/api/cnpj/v1/{cnpj_raw}'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=10)
        d = json.loads(resp.read().decode('utf-8'))
        
        socios = d.get('qsa', [])
        result = {
            'cnpj_raw': cnpj_raw,
            'domain': row.get('domain', ''),
            'telefone': d.get('ddd_telefone_1', '') or '',
            'telefone2': d.get('ddd_telefone_2', '') or '',
            'situacao': d.get('descricao_situacao_cadastral', ''),
            'logradouro': (d.get('descricao_tipo_de_logradouro','') + ' ' + d.get('logradouro','')).strip(),
            'numero': d.get('numero', ''),
            'bairro': d.get('bairro', ''),
            'municipio': d.get('municipio', ''),
            'uf': d.get('uf', ''),
            'cep': d.get('cep', ''),
            'data_inicio': d.get('data_inicio_atividade', ''),
            'capital_social': d.get('capital_social', 0),
            'natureza_juridica': d.get('natureza_juridica', ''),
            'cnae_descricao': d.get('cnae_fiscal_descricao', ''),
            'socio1_nome': socios[0].get('nome_socio', '') if socios else '',
            'socio1_faixa_etaria': socios[0].get('faixa_etaria', '') if socios else '',
            'socio2_nome': socios[1].get('nome_socio', '') if len(socios) > 1 else '',
        }
        results.append(result)
        done_cnpjs.add(cnpj_raw)
        errors = 0
    except Exception as e:
        errors += 1
        if errors >= 10:
            print(f'10 errors, sleeping 30s... last: {e}', flush=True)
            time.sleep(30)
            errors = 0
        time.sleep(1)
        continue
    
    if len(results) % 100 == 0:
        with open(OUT, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False)
        with open(STATUS_FILE, 'w') as f:
            f.write(f'done={len(results)}\ntotal={len(rows)}\npct={len(results)*100//len(rows)}\n')
        print(f'Progress {len(results)}/{len(rows)} ({len(results)*100//len(rows)}%)', flush=True)
    
    time.sleep(0.5)  # ~120 req/min

# Final save
with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False)
with open(STATUS_FILE, 'w') as f:
    f.write(f'DONE\ndone={len(results)}\ntotal={len(rows)}\n')
print(f'DONE: {len(results)} enriched', flush=True)

print('Keeping alive...', flush=True)
while True:
    time.sleep(3600)
