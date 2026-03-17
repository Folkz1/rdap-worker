#!/usr/bin/env python3
"""
RDAP Worker - Consulta registro.br RDAP para extrair CNPJ de domínios .com.br
Uso: python3 rdap_worker.py <domains_file> <output_file> [--offset N] [--limit N] [--delay 0.4]

RDAP rate limits (registro.br):
- 100 queries / 5 min (20/min)
- 1000 queries / 60 min (~16.7/min)
- Conservador: 15/min = 0.25 req/s = 4s delay (seguro)
- Agressivo: 2 req/s testado sem bloqueio (usar com cuidado)

Salva incrementalmente. Resume automático.
"""
import sys
import time
import json
import os
import argparse
import urllib.request
import urllib.error

def rdap_lookup(domain, timeout=10):
    """Consulta RDAP do Registro.br"""
    url = f'https://rdap.registro.br/domain/{domain}'
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (compatible; DomainEnrich/1.0)')
    req.add_header('Accept', 'application/rdap+json')

    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        data = json.loads(resp.read().decode('utf-8'))

        cnpj = ''
        owner = ''
        id_type = ''

        for ent in data.get('entities', []):
            roles = ent.get('roles', [])
            if 'registrant' not in roles:
                continue

            for pid in ent.get('publicIds', []):
                id_type = pid.get('type', '')
                if id_type in ('cnpj', 'cpf'):
                    cnpj = pid.get('identifier', '')

            vcard = ent.get('vcardArray', [])
            if vcard and len(vcard) > 1:
                for item in vcard[1]:
                    if item[0] == 'fn':
                        owner = item[3]
                        break

        if cnpj:
            return cnpj, owner, id_type.upper()
        return None, None, 'NO_ID'

    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, None, 'NOT_FOUND'
        elif e.code == 429 or e.code == 403:
            return None, None, 'RATE_LIMITED'
        return None, None, f'HTTP_{e.code}'
    except Exception as e:
        return None, None, f'ERROR:{type(e).__name__}:{str(e)[:50]}'


def load_done(output_file):
    """Carrega domínios já processados"""
    done = set()
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if parts:
                    done.add(parts[0])
    return done


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('domains_file')
    parser.add_argument('output_file')
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--delay', type=float, default=0.5, help='Delay entre requests (default 0.5s = 2/s)')
    args = parser.parse_args()

    # Carregar domínios
    domains = []
    with open(args.domains_file, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            d = line.strip()
            if not d:
                continue
            # Suporta tanto formato pipe-separated quanto plain
            if '|' in d:
                parts = d.split('|')
                d = parts[1] if len(parts) >= 2 else parts[0]
            d = d.strip().lower()
            if d.endswith('.com.br'):
                domains.append(d)

    if args.offset:
        domains = domains[args.offset:]
    if args.limit:
        domains = domains[:args.limit]

    # Dedup
    domains = list(dict.fromkeys(domains))

    # Resume
    done = load_done(args.output_file)
    remaining = [d for d in domains if d not in done]

    print(f"Domains: {len(domains)} | Done: {len(done & set(domains))} | Remaining: {len(remaining)}")
    print(f"Delay: {args.delay}s | Rate: ~{1/args.delay:.1f} req/s | ETA: {len(remaining)*args.delay/3600:.1f}h")
    print(f"Output: {args.output_file}")
    print()

    stats = {'cnpj': 0, 'cpf': 0, 'not_found': 0, 'rate_limited': 0, 'errors': 0}
    batch = []
    consecutive_429 = 0
    start = time.time()

    for i, domain in enumerate(remaining):
        cnpj, owner, status = rdap_lookup(domain)

        if status == 'RATE_LIMITED':
            stats['rate_limited'] += 1
            consecutive_429 += 1
            if consecutive_429 >= 3:
                wait = min(60, consecutive_429 * 10)
                print(f"  [{i+1}] Rate limited {consecutive_429}x, backoff {wait}s...")
                time.sleep(wait)
            else:
                time.sleep(5)
            # Retry
            cnpj, owner, status = rdap_lookup(domain)
            if status == 'RATE_LIMITED':
                time.sleep(30)
                continue
        else:
            consecutive_429 = 0

        if 'ERROR' in status:
            print(f"  [{i+1}] {domain}: {status}", flush=True)

        if status == 'CNPJ':
            stats['cnpj'] += 1
            owner_clean = owner.replace('|', ' ').replace('\n', ' ')
            batch.append(f"{domain}|{cnpj}|{owner_clean}|CNPJ")
        elif status == 'CPF':
            stats['cpf'] += 1
            batch.append(f"{domain}|CPF|{owner.replace('|',' ')}|CPF")
        elif status == 'NOT_FOUND':
            stats['not_found'] += 1
        else:
            stats['errors'] += 1

        # Salvar cada 200
        if len(batch) >= 200:
            with open(args.output_file, 'a', encoding='utf-8') as f:
                for line in batch:
                    f.write(line + '\n')
            batch = []

        if (i + 1) % 1000 == 0:
            elapsed = time.time() - start
            rps = (i + 1) / elapsed
            eta_h = (len(remaining) - i - 1) / rps / 3600 if rps > 0 else 999
            found = stats['cnpj'] + stats['cpf']
            print(f"[{i+1}/{len(remaining)}] CNPJ:{stats['cnpj']} CPF:{stats['cpf']} "
                  f"404:{stats['not_found']} RL:{stats['rate_limited']} | "
                  f"{rps:.2f} req/s | ETA:{eta_h:.1f}h | Found:{found}")

        time.sleep(args.delay)

    # Flush
    if batch:
        with open(args.output_file, 'a', encoding='utf-8') as f:
            for line in batch:
                f.write(line + '\n')

    elapsed = time.time() - start
    found = stats['cnpj'] + stats['cpf']
    print(f"\n{'='*50}")
    print(f"DONE in {elapsed/3600:.1f}h")
    print(f"CNPJ: {stats['cnpj']} | CPF: {stats['cpf']} | Total found: {found}")
    print(f"404: {stats['not_found']} | Rate limited: {stats['rate_limited']} | Errors: {stats['errors']}")
    print(f"Output: {args.output_file}")


if __name__ == '__main__':
    main()
