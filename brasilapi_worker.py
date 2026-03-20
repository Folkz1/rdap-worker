"""Brasil API enrichment worker - get telefone, socios, endereco for CNPJs.
Uploads results to GitHub release when done."""
import urllib.request
import json
import os
import sys
import time
import csv
import re
import threading
import http.server
import subprocess

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

START_IDX = int(os.environ.get('START_IDX', 0))
END_IDX = int(os.environ.get('END_IDX', 1000))
INPUT_FILE = os.environ.get('INPUT_FILE', '/app/data/cnpjs.csv')
SERVER_NAME = os.environ.get('SERVER_NAME', 'unknown')
OUT = f'/app/results/enriched_{SERVER_NAME}_{START_IDX}_{END_IDX}.json'
STATUS_FILE = f'/app/results/status.txt'

os.makedirs('/app/results', exist_ok=True)

# HTTP server for status
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200); self.send_header('Content-Type','text/plain'); self.end_headers()
            self.wfile.write(b'ok\n')
        elif self.path == '/status':
            self.send_response(200); self.send_header('Content-Type','text/plain; charset=utf-8'); self.end_headers()
            if os.path.exists(STATUS_FILE):
                self.wfile.write(open(STATUS_FILE,'rb').read())
        elif self.path == '/results':
            self.send_response(200); self.send_header('Content-Type','application/json; charset=utf-8'); self.end_headers()
            if os.path.exists(OUT):
                self.wfile.write(open(OUT,'rb').read())
            else:
                self.wfile.write(b'[]\n')
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *a): pass

server = http.server.HTTPServer(('0.0.0.0', 3000), Handler)
print(f"HTTP :3000 | {SERVER_NAME} idx {START_IDX}-{END_IDX}", flush=True)
threading.Thread(target=server.serve_forever, daemon=True).start()

# Load rows
rows = []
with open(INPUT_FILE, encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        if START_IDX <= i < END_IDX:
            rows.append(row)

print(f"Loaded {len(rows)} rows", flush=True)

# Resume
results = []
if os.path.exists(OUT):
    results = json.load(open(OUT, encoding='utf-8'))
    done_cnpjs = {e['cnpj_raw'] for e in results}
    print(f"Resume: {len(results)} done", flush=True)
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
        results.append({
            'cnpj_raw': cnpj_raw, 'domain': row.get('domain',''),
            'telefone': d.get('ddd_telefone_1','') or '',
            'telefone2': d.get('ddd_telefone_2','') or '',
            'situacao': d.get('descricao_situacao_cadastral',''),
            'logradouro': (d.get('descricao_tipo_de_logradouro','') + ' ' + d.get('logradouro','')).strip(),
            'numero': d.get('numero',''), 'bairro': d.get('bairro',''),
            'municipio': d.get('municipio',''), 'uf': d.get('uf',''),
            'cep': d.get('cep',''), 'data_inicio': d.get('data_inicio_atividade',''),
            'capital_social': d.get('capital_social', 0),
            'cnae_descricao': d.get('cnae_fiscal_descricao',''),
            'socio1_nome': socios[0].get('nome_socio','') if socios else '',
            'socio2_nome': socios[1].get('nome_socio','') if len(socios)>1 else '',
        })
        done_cnpjs.add(cnpj_raw)
        errors = 0
    except Exception as e:
        errors += 1
        err_str = str(e)
        err_code = getattr(e, 'code', 0)
        if errors <= 3 or errors % 50 == 0:
            print(f'Error #{errors} cnpj={cnpj_raw}: {type(e).__name__} code={err_code} {err_str[:120]}', flush=True)
        if err_code == 429 or '429' in err_str:
            print(f'Rate limit! Sleeping 60s...', flush=True)
            time.sleep(60)
            errors = 0
        elif errors >= 10:
            print(f'10 errors (last: {type(e).__name__} {err_code}), sleeping 30s...', flush=True)
            time.sleep(30)
            errors = 0
        else:
            time.sleep(2)
        continue

    if len(results) % 100 == 0:
        with open(OUT, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False)
        pct = len(results) * 100 // len(rows)
        with open(STATUS_FILE, 'w') as f:
            f.write(f'server={SERVER_NAME}\ndone={len(results)}\ntotal={len(rows)}\npct={pct}\n')
        print(f'Progress {len(results)}/{len(rows)} ({pct}%)', flush=True)

    time.sleep(0.5)

# Final save
with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False)
with open(STATUS_FILE, 'w') as f:
    f.write(f'DONE\nserver={SERVER_NAME}\ndone={len(results)}\ntotal={len(rows)}\n')
print(f'DONE: {len(results)} enriched', flush=True)

# Upload to GitHub release
print('Uploading to GitHub release...', flush=True)
try:
    upload_url = f'https://uploads.github.com/repos/Folkz1/rdap-worker/releases/assets?name=brasilapi_{SERVER_NAME}_{START_IDX}_{END_IDX}.json'
    data = open(OUT, 'rb').read()
    # Get release ID first
    gh_token = os.environ.get('GITHUB_TOKEN', '')
    rel_req = urllib.request.Request(
        'https://api.github.com/repos/Folkz1/rdap-worker/releases/tags/v1.0',
        headers={'Authorization': f'token {gh_token}', 'Accept': 'application/vnd.github.v3+json'}
    )
    rel_data = json.loads(urllib.request.urlopen(rel_req, timeout=10).read())
    asset_name = f'brasilapi_{SERVER_NAME}_{START_IDX}_{END_IDX}.json'
    upload_url = rel_data.get('upload_url', '').replace('{?name,label}', f'?name={asset_name}')
    if upload_url and gh_token:
        up_req = urllib.request.Request(
            upload_url, data=data,
            headers={'Authorization': f'token {gh_token}',
                     'Content-Type': 'application/json', 'Accept': 'application/vnd.github.v3+json'},
            method='POST'
        )
        urllib.request.urlopen(up_req, timeout=60)
        print('Upload OK!', flush=True)
    else:
        print('No GITHUB_TOKEN, skip upload', flush=True)
except Exception as e:
    print(f'Upload error: {e}', flush=True)

print('Keeping alive...', flush=True)
while True:
    time.sleep(3600)
