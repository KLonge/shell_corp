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
		which uv > /dev/null || (\
			echo "Installing uv..." && \
			(powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex" && \
			export PATH="$$PATH:$$USERPROFILE/.local/bin" && \
			echo 'export PATH="$$PATH:$$USERPROFILE/.local/bin"' >> ~/.bashrc && \
			source ~/.bashrc) \
		); \
	fi

init-python:
	@if [ ! -d "$(VENV_NAME)" ]; then \
		echo "Creating virtual environment with Python 3.12..."; \
		cd $(CURDIR) && $(UV_CMD) venv --python 3.12 $(VENV_NAME); \
	fi
	make install-python-deps

install-python-deps:
	cd $(CURDIR) && $(UV_CMD) pip install -e .

upgrade-python-deps:
	$(UV_CMD) lock --upgrade
	make install-python-deps

# Base commands
init: check-uv init-python install-python-deps

clean:
	find . \( -type d -name "__pycache__" -o -type f -name "*.pyc" -o -type d -name ".pytest_cache" -o -type d -name "*.egg-info" \) -print0 | xargs -0 rm -rf

test:
	$(PYTHON_VENV_NAME)/bin/pytest --cov=shell_corp shell_corp_tests

mypy:
	$(PYTHON_VENV_NAME)/bin/mypy shell_corp

# Add these new commands
dlt:
	$(PYTHON_CMD) src/loader/main.py
	@echo "\nVerifying data in DuckDB:"
	@$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/shell_corp.duckdb'); print(conn.sql('SELECT * FROM raw.transfer_listings').df())"

show-analysis:
	@echo "Showing transfer analysis results:"
	@$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/shell_corp.duckdb'); print(conn.sql('SELECT * FROM staging.transfer_analysis').df())"

sqlmesh-plan:
	sqlmesh -p src/sqlmesh plan
	@echo "\nShowing transfer analysis results:"
	-@$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/shell_corp.duckdb'); print(conn.sql('SELECT * FROM staging.transfer_analysis').df())"

sqlmesh-restate:
	sqlmesh -p src/sqlmesh plan --restate-model staging.transfer_analysis
	@echo "\nShowing transfer analysis results:"
	-@$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('database/shell_corp.duckdb'); print(conn.sql('SELECT * FROM staging.transfer_analysis').df())"

# Clean command to remove virtual environment
clean-venv:
	$(DEACTIVATE) || true
	rm -rf $(VENV_NAME)
	rm -rf database/*.duckdb*

# Create virtual environment
create-venv:
	$(UV_CMD) venv $(VENV_NAME)
	@echo "Virtual environment created. To activate, run: source .venv/Scripts/activate"

# Install dependencies
install-deps:
	$(UV_CMD) pip install -e .

# Full reinstall command
reinstall: clean-venv create-venv install-deps

# Add these new commands
activate:
	@echo "To activate the virtual environment, run:"
	@echo "source .venv/Scripts/activate"

deactivate:
	@echo "To deactivate the virtual environment, run:"
	@echo "source .venv/Scripts/deactivate"

# Package inspection commands
list-packages:
	$(PYTHON_CMD) -m uv pip list

show-tree:
	$(PYTHON_CMD) -m uv tree

check-env:
	$(PYTHON_CMD) -m uv pip check

.PHONY: test test-debug mypy dlt sqlmesh-plan clean-venv create-venv install-deps reinstall activate deactivate list-packages show-tree check-env