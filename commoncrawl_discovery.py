"""
Common Crawl WordPress BR Discovery
Busca dominios brasileiros com WordPress no Common Crawl Index.
Extrai dominios unicos, dedup contra base do Erik, salva novos.
"""
import urllib.request
import json
import sys
import time
import os
from urllib.parse import urlparse

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

CC_API = "https://index.commoncrawl.org/CC-MAIN-2026-08-index"
ERIK_DOMAINS = "D:/tmp/cnpj-rfb/erik-data/all_domains.csv"
OUTPUT_DIR = "D:/tmp/cnpj-rfb/commoncrawl"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "new_wordpress_br.txt")
RESUME_FILE = os.path.join(OUTPUT_DIR, "resume_page.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_erik_domains():
    """Carrega dominios do Erik pra dedup"""
    domains = set()
    with open(ERIK_DOMAINS, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) >= 2:
                d = parts[1].strip().lower()
                # Extrair root
                if d.endswith('.com.br'):
                    domains.add(d)
                elif '.' in d:
                    # subdominio -> root
                    p = d.split('.')
                    if d.endswith('.com.br') and len(p) >= 3:
                        domains.add('.'.join(p[-3:]))
                    else:
                        domains.add(d)
    return domains


def extract_root_domain(url_str):
    """Extrai dominio raiz de uma URL"""
    try:
        parsed = urlparse(url_str if '://' in url_str else f'https://{url_str}')
        host = (parsed.hostname or '').lower()
        parts = host.split('.')

        # .com.br, .net.br, .org.br etc
        if len(parts) >= 3 and parts[-1] == 'br':
            # ex: empresa.com.br ou sub.empresa.com.br
            tld = f'.{parts[-2]}.br'
            if parts[-2] in ('com', 'net', 'org', 'ind', 'srv', 'eti', 'art', 'mus'):
                return '.'.join(parts[-3:])
            else:
                return '.'.join(parts[-2:])
        elif len(parts) >= 2 and parts[-1] == 'br':
            return host
        elif len(parts) >= 2:
            return '.'.join(parts[-2:])
        return host
    except:
        return ''


def fetch_cc_page(query_url, page, max_retries=3):
    """Busca uma pagina do CC Index"""
    url = f'{query_url}&page={page}'
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'Mozilla/5.0 (DomainEnrich/1.0)')
            resp = urllib.request.urlopen(req, timeout=120)
            content = resp.read().decode('utf-8')
            lines = [l for l in content.strip().split('\n') if l.strip()]
            return lines
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                print(f"  [ERR] Page {page}: {e}")
                return []


def main():
    start = time.time()

    # Carregar base Erik pra dedup
    print("Carregando base Erik...", flush=True)
    erik_domains = load_erik_domains()
    print(f"  Erik domains: {len(erik_domains)}")

    # Carregar dominios ja encontrados (resume)
    found_domains = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r') as f:
            found_domains = set(l.strip() for l in f if l.strip())
    print(f"  Ja encontrados (resume): {len(found_domains)}")

    # Resume page
    start_page = 0
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, 'r') as f:
            start_page = int(f.read().strip())
    print(f"  Resume from page: {start_page}")

    # Queries: .com.br, .net.br, .org.br + wp-content
    queries = [
        ('*.com.br/wp-content/*', 2313),
        ('*.com.br/wp-json/*', 2313),
        ('*.org.br/wp-content/*', 15),
        ('*.net.br/wp-content/*', 2),
    ]

    total_new = 0

    for pattern, num_pages in queries:
        query_url = f'{CC_API}?url={pattern}&output=json'
        print(f"\n=== {pattern} ({num_pages} pages) ===", flush=True)

        for page in range(start_page, num_pages):
            lines = fetch_cc_page(query_url, page)
            if not lines:
                continue

            page_domains = set()
            for line in lines:
                try:
                    data = json.loads(line)
                    url_str = data.get('url', '')
                    root = extract_root_domain(url_str)
                    if root and root not in erik_domains and root not in found_domains:
                        page_domains.add(root)
                except:
                    pass

            if page_domains:
                found_domains.update(page_domains)
                total_new += len(page_domains)
                with open(OUTPUT_FILE, 'a') as f:
                    for d in page_domains:
                        f.write(d + '\n')

            # Save resume
            with open(RESUME_FILE, 'w') as f:
                f.write(str(page + 1))

            if (page + 1) % 100 == 0:
                elapsed = time.time() - start
                print(f"  Page {page+1}/{num_pages} | New: {total_new} | Total unique: {len(found_domains)} | {elapsed/60:.0f}min", flush=True)

            time.sleep(2.0)  # Slower to avoid CC rate limit

        # Reset start_page pra proxima query
        start_page = 0

    elapsed = time.time() - start
    print(f"\n{'='*50}")
    print(f"DISCOVERY COMPLETO em {elapsed/60:.0f}min")
    print(f"Novos dominios WordPress BR: {len(found_domains)}")
    print(f"(excluindo {len(erik_domains)} do Erik)")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
