#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
echo "Type-checking..."
npx tsc --noEmit
echo "Bundling..."
npx esbuild src/main.ts --bundle --outfile=dist/main.js --format=esm --target=es2020
echo "Done → dist/main.js"
