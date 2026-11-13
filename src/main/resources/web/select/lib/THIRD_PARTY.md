# Third party files in the select timer web app

These files are served by PikaTimer to the select timer devices. They are
bundled (not loaded from the internet) because race sites often have no
internet connection. `tools/fetch-select-timer-assets.sh` downloads them
again and checks them against the hashes below.

## onnxruntime-web 1.31.1 (`ort/`)

- License: MIT, Copyright (c) Microsoft Corporation
- Source: https://www.npmjs.com/package/onnxruntime-web (`dist/` of the npm package)
- Files:
  - `ort.wasm.bundle.min.mjs`
  - `ort-wasm-simd-threaded.mjs`
  - `ort-wasm-simd-threaded.wasm`

## PaddleOCR PP-OCRv4 mobile models, ONNX conversions by RapidOCR (`models/`)

- License: Apache License 2.0 (PaddleOCR, Copyright (c) PaddlePaddle Authors; RapidOCR, Copyright (c) RapidAI)
- Source: https://www.modelscope.cn/models/RapidAI/RapidOCR
- Files (saved under shorter names):
  - `ppocrv4_det_mobile.onnx` from `onnx/PP-OCRv4/det/ch_PP-OCRv4_det_mobile.onnx` (text detection)
  - `en_ppocrv4_rec_mobile.onnx` from `onnx/PP-OCRv4/rec/en_PP-OCRv4_rec_mobile.onnx` (English recognition)
  - `en_dict.txt` from `paddle/PP-OCRv4/rec/en_PP-OCRv4_rec_mobile/en_dict.txt` (the recognition model's characters)

## SHA-256

```
4b884c203b0fa48d057be3332c65513ff85aa3c5bddd6946acc0fab7ecda001d  ort/ort-wasm-simd-threaded.wasm
30ed0b5cd1182ede931fea448afb567d4dc705ec957af09f108a2be5f770dac3  ort/ort-wasm-simd-threaded.mjs
51bd7168175767324ab0acfcfc4ca94a096a3826e3ebac9ccc4f195eb865f8dd  ort/ort.wasm.bundle.min.mjs
d2a7720d45a54257208b1e13e36a8479894cb74155a5efe29462512d42f49da9  models/ppocrv4_det_mobile.onnx
e8770c967605983d1570cdf5352041dfb68fa0c21664f49f47b155abd3e0e318  models/en_ppocrv4_rec_mobile.onnx
5662df9d2d03f0e8ca0d3b0649d6acbab904b6a14b3d3521463c71c37c668ce3  models/en_dict.txt
```
