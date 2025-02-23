# System detection
UNAME := "$(shell uname)"
PYTHON_VENV_NAME := ".venv"

# Python virtual environment settings
VENV_NAME := .venv
PYTHON := python

# Check if we're on Windows
ifeq ($(OS),Windows_NT)
    PYTHON_CMD := $(CURDIR)/$(VENV_NAME)/Scripts/python
    # Fix UV path for Windows - use proper path escaping
    UV_CMD := "$(subst \,/,$(USERPROFILE))/.local/bin/uv.exe"
    # Add these Windows-specific commands
    ACTIVATE := source $(CURDIR)/$(VENV_NAME)/Scripts/activate
    DEACTIVATE := source $(CURDIR)/$(VENV_NAME)/Scripts/deactivate
else
    PYTHON_CMD := $(CURDIR)/$(VENV_NAME)/bin/python
    UV_CMD := uv
    ACTIVATE := source $(CURDIR)/$(VENV_NAME)/bin/activate
    DEACTIVATE := deactivate
endif

# Python setup commands
install-python:
ifeq ($(UNAME),"Darwin")
	brew install python@3.12
else
	@echo "Please install Python 3.12 manually for your operating system"
	@echo "Visit: https://www.python.org/downloads/"
	@exit 1
endif

check-uv:
	@if [ "$(shell uname)" = "Darwin" ]; then \
		which uv > /dev/null || (echo "Installing uv via Homebrew..." && brew install uv); \
	else \
		which uv > /dev/null || (echo "Installing uv via curl..." && curl -LsSf https://astral.sh/uv/install.sh | sh); \
	fi

init-python:
	@if [ ! -d "$(PYTHON_VENV_NAME)" ]; then \
		echo "Creating virtual environment with Python 3.12..."; \
		uv venv --python 3.12 $(PYTHON_VENV_NAME); \
	fi

install-python-deps:
	uv pip install -e ".[dev]"

upgrade-python-deps:
	uv lock --upgrade
	make install-python-deps

# Base commands
init: check-uv init-python install-python-deps

clean:
	find . \( -type d -name "__pycache__" -o -type f -name "*.pyc" -o -type d -name ".pytest_cache" -o -type d -name "*.egg-info" \) -print0 | xargs -0 rm -rf

# Testing commands
test:
	$(PYTHON_CMD) -m pytest -vv --log-cli-level=INFO $(filter-out $@,$(MAKECMDGOALS))

mypy:
	$(PYTHON_CMD) -m mypy src/

# Data pipeline commands
dlt:
	$(PYTHON_CMD) src/loader/main.py
	@echo "\nVerifying data in DuckDB:"
	@$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/shell_corp.duckdb'); print(conn.sql('SELECT * FROM raw.transfer_listings').df())"

sqlmesh-plan:
	cd src/sqlmesh && ../../$(VENV_NAME)/Scripts/sqlmesh plan
	@echo "\nShowing transfer analysis results:"
	-@$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/shell_corp.duckdb'); print(conn.sql('SELECT * FROM staging.transfer_analysis').df())"

sqlmesh-restate:
	cd src/sqlmesh && ../../$(VENV_NAME)/Scripts/sqlmesh plan --restate-model staging.transfer_analysis

sqlmesh-test:
	cd src/sqlmesh && ../../$(VENV_NAME)/Scripts/sqlmesh test

sqlmesh-audit:
	cd src/sqlmesh && ../../$(VENV_NAME)/Scripts/sqlmesh audit

sqlmesh-run:
	cd src/sqlmesh && ../../$(VENV_NAME)/Scripts/sqlmesh run

.PHONY: init init-python install-python check-uv install-python-deps upgrade-python-deps clean test mypy dlt sqlmesh-plan sqlmesh-restate sqlmesh-test sqlmesh-audit sqlmesh-run