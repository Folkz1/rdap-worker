"""Run RDAP worker + HTTP status server on port 3000"""
import http.server
import threading
import os
import sys
import subprocess

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

RESULTS_FILE = '/app/results/rdap_results.txt'
CHUNK = os.environ.get('CHUNK', '1')
DELAY = os.environ.get('DELAY', '8.0')


class StatusHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/status':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            if os.path.exists(RESULTS_FILE):
                with open(RESULTS_FILE, 'r') as f:
                    lines = f.readlines()
                self.wfile.write(f'chunk={CHUNK}\n'.encode())
                self.wfile.write(f'lines={len(lines)}\n'.encode())
                self.wfile.write(f'delay={DELAY}\n'.encode())
                self.wfile.write(b'\nLast 10:\n')
                for l in lines[-10:]:
                    self.wfile.write(l.encode())
            else:
                self.wfile.write(b'No results yet\n')
        elif self.path == '/results':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            if os.path.exists(RESULTS_FILE):
                with open(RESULTS_FILE, 'rb') as f:
                    self.wfile.write(f.read())
            else:
                self.wfile.write(b'No results yet\n')
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'ok\n')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Silenciar logs HTTP


# Start HTTP server
server = http.server.HTTPServer(('0.0.0.0', 3000), StatusHandler)
print(f"HTTP server on :3000 | chunk={CHUNK} delay={DELAY}")
threading.Thread(target=server.serve_forever, daemon=True).start()

# Run RDAP worker as subprocess (keep HTTP server alive)
domains_file = f'domains_chunk{CHUNK}.txt'
print(f"Starting RDAP worker: {domains_file} delay={DELAY}s")
proc = subprocess.Popen([
    sys.executable, '-u', 'rdap_worker.py',
    domains_file, RESULTS_FILE,
    '--delay', DELAY
])
proc.wait()
