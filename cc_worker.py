"""Common Crawl discovery worker with HTTP server to keep alive and serve results."""
import urllib.request
import json
import os
import sys
import time
import threading
import http.server
from urllib.parse import urlparse

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

CC_API = "https://index.commoncrawl.org/CC-MAIN-2026-08-index"
START = int(os.environ.get('START_PAGE', 0))
END = int(os.environ.get('END_PAGE', 100))
OUT = '/app/results/domains.txt'
STATUS_FILE = '/app/results/status.txt'

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
            if os.path.exists(OUT):
                count = sum(1 for _ in open(OUT))
                self.wfile.write(f'\ndomains={count}\n'.encode())
        elif self.path == '/results':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            if os.path.exists(OUT):
                self.wfile.write(open(OUT, 'rb').read())
            else:
                self.wfile.write(b'No results yet\n')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


# Start HTTP server on port 3000
server = http.server.HTTPServer(('0.0.0.0', 3000), Handler)
print(f"HTTP :3000 | pages {START}-{END}", flush=True)
threading.Thread(target=server.serve_forever, daemon=True).start()

# Run discovery
found = set()
if os.path.exists(OUT):
    with open(OUT) as f:
        found = set(l.strip() for l in f if l.strip())
    print(f"Resume: {len(found)} domains", flush=True)

errors = 0
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
                    found.add('.'.join(parts[-3:]))
            except:
                pass
        errors = 0  # reset on success
    except Exception as e:
        errors += 1
        print(f'ERR page {page}: {e}', flush=True)
        if errors >= 5:
            print(f'5 errors in a row, waiting 30s...', flush=True)
            time.sleep(30)
            errors = 0
        else:
            time.sleep(5)
        continue

    if (page - START + 1) % 10 == 0:
        with open(OUT, 'w') as f:
            f.write('\n'.join(sorted(found)))
        with open(STATUS_FILE, 'w') as f:
            f.write(f'page={page}/{END}\ndomains={len(found)}\n')
        print(f'Page {page}/{END} | {len(found)} domains', flush=True)

    time.sleep(2)

# Final save
with open(OUT, 'w') as f:
    f.write('\n'.join(sorted(found)))
with open(STATUS_FILE, 'w') as f:
    f.write(f'DONE\npage={END}/{END}\ndomains={len(found)}\n')
print(f'DONE: {len(found)} domains (pages {START}-{END})', flush=True)

# Keep alive for results download
print('Keeping alive for results download...', flush=True)
while True:
    time.sleep(3600)
