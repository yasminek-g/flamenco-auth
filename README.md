# flamenco-auth

`scrape_candil.py` downloads every PDF linked by `Descargar` from the Candil results listing at:

`https://www.dipujaen.es/revistacandil/results.vm?q=parent:0000393913&t=%2Balpha&lang=es&view=cdl`

The script follows the pagination automatically, extracts the `download-*` links on each results page, and saves the files into a directory you choose.
Candil issues are now stored as:

`candil/YYYY-MM/YYYY-MM.pdf`

Resume mode is enabled by default: if a PDF already exists in the output directory, the script skips it and continues from the next missing file.

## Usage

```bash
python3 scrape_candil.py --output-dir /absolute/path/to/downloads
```

Useful flags:

- `--dry-run`: list the files it would download without saving them.
- `--resume` / `--no-resume`: enable or disable "start where I left off" behavior. Resume is on by default.
- `--overwrite`: redownload files even if they already exist.
- `--retries 5`: retry transient timeouts or 504 responses.
- `--delay 0.5`: wait longer between requests if you want to be gentler with the site.
- `--start-url URL`: override the initial results page.
- `--limit 5`: only process the first 5 unique files.
- `--organize-only`: move previously downloaded numeric or old-format Candil PDFs into the new `candil/YYYY-MM/YYYY-MM.pdf` layout without downloading missing files.

Example dry run:

```bash
python3 scrape_candil.py --output-dir ./downloads --dry-run
```

Run it in a separate terminal with unbuffered output so progress appears immediately:

```bash
python3 -u scrape_candil.py \
  --output-dir /Users/yasminekroknes-gomez/Downloads/history-dataset \
  --timeout 120 \
  --retries 5 \
  --delay 0
```

## La Nueva Alborea

`scrape_alborea.py` downloads the full set of "La Nueva Alborea" issue PDFs from:

`https://www.juntadeandalucia.es/cultura/flamenco/content/la-nueva-albore%C3%A1`

It first collects the issue links from the index page, then opens each issue page and extracts the PDF URL from the embedded viewer iframe.

```bash
python3 -u scrape_alborea.py \
  --output-dir /absolute/path/to/alborea-downloads \
  --timeout 120 \
  --retries 5 \
  --delay 0
```

The same operational flags are supported here too:

- `--resume` / `--no-resume`
- `--overwrite`
- `--dry-run`
- `--limit`
- `--retries`

## OCR

`run_paddleocr_vl.py` runs the official `PaddleOCRVL` pipeline on local PDFs/images and writes:

- one merged JSON file per document
- one merged Markdown file per document
- extracted Markdown-linked images under `images/`

By default it writes outputs next to each input file, so a Candil issue like:

`candil/1978-04/1978-04.pdf`

will produce:

- `candil/1978-04/1978-04.json`
- `candil/1978-04/1978-04.md`
- `candil/1978-04/images/...`

Example GPU run:

```bash
python3 -u run_paddleocr_vl.py \
  /Users/yasminekroknes-gomez/Downloads/history-dataset/candil \
  --device gpu:0 \
  --precision fp16 \
  --enable-hpi \
  --use-layout-detection \
  --merge-tables
```

For cluster usage with limited scratch space, process in batches and sync each finished document out immediately with a post-success hook.

Example batch run on scratch:

```bash
python3 -u run_paddleocr_vl.py \
  /scratch/$USER/history-dataset/candil \
  --device gpu:0 \
  --precision fp16 \
  --batch-size 25 \
  --batch-index 0 \
  --post-success-command "rsync -av {output_dir}/ your-local-target/" \
  --cleanup-output-on-success
```

Useful cluster-oriented flags:

- `--batch-size N`: only process one batch of `N` documents.
- `--batch-index K`: choose which batch to run.
- `--skip-existing`: skip docs whose `.json` and `.md` already exist.
- `--post-success-command`: run a shell command after each successful document.
- `--cleanup-output-on-success`: delete generated OCR outputs after the post-success command succeeds.
- `--cleanup-input-on-success`: also delete the source input file after success.
