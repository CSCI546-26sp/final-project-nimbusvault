#!/usr/bin/env python3
import sys
import re
import zlib
from pathlib import Path

MIN_PRINTABLE_SEQ = 20

def extract_printable(b):
    # find ASCII-printable sequences of length >= MIN_PRINTABLE_SEQ
    seqs = re.findall(rb'[\x20-\x7E]{%d,}' % MIN_PRINTABLE_SEQ, b)
    return [s.decode('utf-8', 'ignore') for s in seqs]


def extract_from_file(path):
    data = path.read_bytes()
    results = []
    pos = 0
    while True:
        idx = data.find(b'stream', pos)
        if idx == -1:
            break
        # locate start of stream content
        nl = data.find(b'\n', idx)
        if nl == -1:
            pos = idx + 6
            continue
        start = nl + 1
        end = data.find(b'endstream', start)
        if end == -1:
            break
        stream_bytes = data[start:end]
        header_start = max(0, idx-300)
        header = data[header_start:idx]
        tried = False
        if b'/FlateDecode' in header:
            tried = True
            try:
                dec = zlib.decompress(stream_bytes)
                printable = extract_printable(dec)
                if printable:
                    results.append(('flate', printable))
            except Exception:
                # sometimes streams have extra bytes; try forgiving decompression
                try:
                    # strip potential leading \n or spaces
                    dec = zlib.decompress(stream_bytes.strip())
                    printable = extract_printable(dec)
                    if printable:
                        results.append(('flate', printable))
                except Exception:
                    pass
        if not tried:
            # try to extract printable ASCII from raw stream
            printable = extract_printable(stream_bytes)
            if printable:
                results.append(('raw', printable))
        pos = end + len(b'endstream')
    # also try to extract printable sequences from entire file as fallback
    if not results:
        all_prints = extract_printable(data)
        if all_prints:
            results.append(('all', all_prints))
    return results


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: extract_pdfs_fallback.py <pdf-file> [more...]')
        sys.exit(1)
    for p in sys.argv[1:]:
        fp = Path(p)
        print('\n=== Begin:', p, '===\n')
        if not fp.exists():
            print('(file not found)')
            continue
        try:
            res = extract_from_file(fp)
            if not res:
                print('(no printable text extracted)')
            else:
                for kind, seqs in res:
                    print(f'-- Extract kind: {kind} --\n')
                    for s in seqs:
                        print(s)
                        print('\n----\n')
        except Exception as e:
            print('Error processing', p, e)
        print('\n=== End:', p, '===\n')
