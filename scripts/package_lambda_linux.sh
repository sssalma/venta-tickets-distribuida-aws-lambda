#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR=".lambda_build"
ZIP_FILE="lambda_worker.zip"

rm -rf "$BUILD_DIR" "$ZIP_FILE"
mkdir -p "$BUILD_DIR"

python3 -m pip install -r lambda_requirements.txt -t "$BUILD_DIR"

cp lambda_worker.py "$BUILD_DIR/"
cp -r base "$BUILD_DIR/"

(cd "$BUILD_DIR" && zip -r "../$ZIP_FILE" .)

echo "ZIP generado: $ZIP_FILE"
