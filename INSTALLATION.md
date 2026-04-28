# Installation Guide

This project uses a standard Python virtual environment with `pip`.

The project dependencies are declared in `pyproject.toml`. The recommended Python version is **Python 3.11**, because some ML/retrieval dependencies may be less stable on the newest Python versions.

## 1. Check available Python versions

On macOS with Homebrew, check installed Python versions with:

```bash
brew list --versions | grep '^python'
```

You can also check which Python executables are available:

```bash
which -a python python3 python3.9 python3.10 python3.11 python3.12 python3.13
```

For this project, use:

```bash
python3.11 --version
```

Expected output should be something like:

```text
Python 3.11.x
```

## 2. Create the virtual environment

From the repository root:

```bash
python3.11 -m venv flamenco-env
```

This creates a local virtual environment folder called:

```text
flamenco-env/
```

Do **not** commit this folder to Git.

## 3. Activate the environment

```bash
source flamenco-env/bin/activate
```

After activation, your shell prompt may show something like:

```text
(flamenco-env)
```

Check that the environment is using the correct Python:

```bash
python --version
which python
```

Expected output:

```text
Python 3.11.x
/path/to/flamenco-auth/flamenco-env/bin/python
```

## 4. Upgrade pip

```bash
python -m pip install --upgrade pip
```

## 5. Install the project dependencies

Install the package in editable mode with development and retrieval dependencies:

```bash
pip install -e ".[dev,retrieval]"
```

This installs:

- core project dependencies
- development tools such as `pytest`, `ruff`, and `mypy`
- retrieval dependencies such as `rank-bm25`, `sentence-transformers`, and `faiss-cpu`

## 6. Verify the installation

Run:

```bash
python -c "import pydantic, typer, rich, orjson, yaml, tqdm; print('base deps ok')"
```

Then run:

```bash
python -c "import rank_bm25, sentence_transformers, faiss; print('retrieval deps ok')"
```

If both commands print `ok`, the environment is ready.

## 7. Reactivating the environment later

Whenever you return to the project, run:

```bash
source flamenco-env/bin/activate
```

## 8. Deactivating the environment

To leave the environment:

```bash
deactivate
```

## 9. Rebuilding the environment from scratch

If the environment becomes broken or stale, remove it and rebuild it:

```bash
rm -rf flamenco-env
python3.11 -m venv flamenco-env
source flamenco-env/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev,retrieval]"
```

## 10. Common issue: `python3.11: command not found`

If this happens, check whether Homebrew has Python 3.11 installed:

```bash
brew list --versions | grep '^python'
```

If `python@3.11` is installed but `python3.11` is not found, try:

```bash
brew --prefix python@3.11
```

On Apple Silicon Macs, the executable is usually:

```bash
/opt/homebrew/bin/python3.11
```

On Intel Macs, it is usually:

```bash
/usr/local/bin/python3.11
```

You can then create the environment using the full path:

```bash
/opt/homebrew/bin/python3.11 -m venv flamenco-env
```

or:

```bash
/usr/local/bin/python3.11 -m venv flamenco-env
```