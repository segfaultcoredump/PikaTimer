#!/bin/sh
# Downloads the select timer web app's OCR runtime and models into
# src/main/resources/web/select/lib and checks them against the hashes in
# src/main/resources/web/select/lib/THIRD_PARTY.md.
#
# The files are checked in, so this is only needed to restore or update them.
# To update a version, change it below, run this, then update the hashes
# (and EXPECTED_BYTES in ocr-worker.js).
set -eu

ORT_VERSION=1.31.1
MODELS="https://www.modelscope.cn/models/RapidAI/RapidOCR/resolve/master"

cd "$(dirname "$0")/.."
LIB=src/main/resources/web/select/lib
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$LIB/ort" "$LIB/models"

echo "onnxruntime-web $ORT_VERSION"
curl -fsSL -o "$TMP/ort.tgz" "https://registry.npmjs.org/onnxruntime-web/-/onnxruntime-web-$ORT_VERSION.tgz"
mkdir "$TMP/ort"
tar xzf "$TMP/ort.tgz" -C "$TMP/ort"
for f in ort.wasm.bundle.min.mjs ort-wasm-simd-threaded.mjs ort-wasm-simd-threaded.wasm; do
    cp "$TMP/ort/package/dist/$f" "$LIB/ort/$f"
done

echo "PP-OCRv4 models"
curl -fsSL -o "$LIB/models/ppocrv4_det_mobile.onnx" "$MODELS/onnx/PP-OCRv4/det/ch_PP-OCRv4_det_mobile.onnx"
curl -fsSL -o "$LIB/models/en_ppocrv4_rec_mobile.onnx" "$MODELS/onnx/PP-OCRv4/rec/en_PP-OCRv4_rec_mobile.onnx"
curl -fsSL -o "$LIB/models/en_dict.txt" "$MODELS/paddle/PP-OCRv4/rec/en_PP-OCRv4_rec_mobile/en_dict.txt"

echo "Checking"
cd "$LIB"
sed -n '/^```$/,/^```$/p' THIRD_PARTY.md | grep -E '^[0-9a-f]{64}  ' | shasum -a 256 -c -
