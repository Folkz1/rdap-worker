"""Common Crawl discovery worker. Busca dominios .com.br com WordPress."""
import urllib.request
import json
import os
import sys
import time
from urllib.parse import urlparse

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

CC_API = "https://index.commoncrawl.org/CC-MAIN-2026-08-index"
START = int(os.environ.get('START_PAGE', 0))
END = int(os.environ.get('END_PAGE', 100))
OUT = '/app/results/domains.txt'

os.makedirs('/app/results', exist_ok=True)

found = set()
# Resume
if os.path.exists(OUT):
    with open(OUT) as f:
        found = set(l.strip() for l in f if l.strip())
    print(f"Resume: {len(found)} domains already found")

for page in range(START, END):
    url = f'{CC_API}?url=*.com.br/wp-content/*&output=json&page={page}'
    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (CCWorker/1.0)')
        resp = urllib.request.urlopen(req, timeout=120)
        lines = resp.read().decode('utf-8').strip().split('\n')
        before = len(found)
        for line in lines:
            try:
                d = json.loads(line)
                host = urlparse(d.get('url', '')).hostname or ''
                parts = host.split('.')
                if host.endswith('.com.br') and len(parts) >= 3:
                    root = '.'.join(parts[-3:])
                    found.add(root)
            except:
                pass
        new = len(found) - before
    except Exception as e:
        print(f'ERR page {page}: {e}', flush=True)
        time.sleep(5)
        continue

    if (page - START + 1) % 10 == 0:
        print(f'Page {page}/{END} | Total {len(found)} | +{new} new', flush=True)
        with open(OUT, 'w') as f:
            f.write('\n'.join(sorted(found)))

    time.sleep(2)

with open(OUT, 'w') as f:
    f.write('\n'.join(sorted(found)))
print(f'DONE: {len(found)} domains from pages {START}-{END}')
