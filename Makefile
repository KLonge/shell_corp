# System detection
UNAME := "$(shell uname)"
PYTHON_VENV_NAME := ".venv"

# Python virtual environment settings
VENV_NAME := .venv
PYTHON := python

# Check if we're on Windows
ifeq ($(OS),Windows_NT)
    PYTHON_CMD := $(CURDIR)/$(VENV_NAME)/Scripts/python
    SQLMESH_CMD := $(CURDIR)/$(VENV_NAME)/Scripts/sqlmesh
    UV_CMD := "$(subst \,/,$(USERPROFILE))/.local/bin/uv.exe"
    ACTIVATE := source $(CURDIR)/$(VENV_NAME)/Scripts/activate
    DEACTIVATE := source $(CURDIR)/$(VENV_NAME)/Scripts/deactivate
else
    PYTHON_CMD := $(CURDIR)/$(VENV_NAME)/bin/python
    SQLMESH_CMD := $(CURDIR)/$(VENV_NAME)/bin/sqlmesh
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

display-a:
	@echo "\nShowing derived table A results:"
	-$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/new/transferroom.duckdb'); print(conn.sql('SELECT * FROM prod.derived_a LIMIT 5').df())"

display-b:
	@echo "\nShowing derived table B results:"
	-$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/new/transferroom.duckdb'); print(conn.sql('SELECT * FROM prod.derived_b LIMIT 5').df())"

display-c:
	@echo "\nShowing derived table C results:"
	-$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/new/transferroom.duckdb'); print(conn.sql('SELECT * FROM prod.derived_c LIMIT 5').df())"

# Use the separate display commands
sqlmesh-plan:
	cd src/sqlmesh && $(SQLMESH_CMD) plan
	$(DISPLAY_RESULTS)

sqlmesh-restate:
	cd src/sqlmesh && $(SQLMESH_CMD) plan --restate-model prod.derived_a --restate-model prod.derived_b --restate-model prod.derived_c
	@$(MAKE) display-a
	@$(MAKE) display-b
	@$(MAKE) display-c

sqlmesh-test:
	cd src/sqlmesh && $(SQLMESH_CMD) test

sqlmesh-audit:
	cd src/sqlmesh && $(SQLMESH_CMD) audit

sqlmesh-run:
	cd src/sqlmesh && $(SQLMESH_CMD) run
	$(DISPLAY_RESULTS)

.PHONY: init init-python install-python check-uv install-python-deps upgrade-python-deps clean test mypy dlt sqlmesh-plan sqlmesh-restate sqlmesh-test sqlmesh-audit sqlmesh-run display-a display-b display-c