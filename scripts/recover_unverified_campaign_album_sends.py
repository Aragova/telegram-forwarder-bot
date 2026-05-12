#!/usr/bin/env python3
from __future__ import annotations
import argparse


def main() -> int:
    parser = argparse.ArgumentParser(description='Recovery helper for unverified campaign album sends')
    parser.add_argument('--run-id', type=int, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    mode = 'APPLY' if args.apply else 'DRY-RUN'
    print(f'[{mode}] run_id={args.run_id}. Скрипт-заготовка: восстановление выполняется через runtime после внедрения в окружение.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
