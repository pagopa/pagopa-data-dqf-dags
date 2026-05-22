-include .env
export

.PHONY: install lint test pre-commit-install render render-all smoke smoke-all

install:
	pip install -e .[dev]

lint:
	ruff check .

test:
	pytest

pre-commit-install:
	pre-commit install

# Render del DAG per una coppia (system, env).
# Uso: make render SYSTEM=gpd ENV=test
render:
	@test -n "$(SYSTEM)" || (echo "ERROR: set SYSTEM=<system>" >&2; exit 1)
	@test -n "$(ENV)" || (echo "ERROR: set ENV=<env>" >&2; exit 1)
	@python3 scripts/render.py "$(SYSTEM)" "$(ENV)" \
		dag/pagopa_dq.py \
		dags_config/$(SYSTEM)/$(ENV).json \
		dist/dag_dq_$(SYSTEM)_quality_$(ENV).py

# Render di tutti gli env per uno specifico system.
# Uso: make render-all SYSTEM=gpd
render-all:
	@test -n "$(SYSTEM)" || (echo "ERROR: set SYSTEM=<system>" >&2; exit 1)
	@for cfg in dags_config/$(SYSTEM)/*.json; do \
		env=$$(basename $$cfg .json); \
		$(MAKE) render SYSTEM=$(SYSTEM) ENV=$$env; \
	done

# Smoke: render-all per un system + ast.parse di ogni file in dist/.
# Uso: make smoke SYSTEM=gpd
smoke: render-all
	@for f in dist/dag_dq_$(SYSTEM)_quality_*.py; do \
		python3 -c "import ast; ast.parse(open('$$f').read()); print('OK', '$$f')" || exit 1; \
	done

# Smoke su TUTTI i system trovati in dags_config/.
smoke-all:
	@for sys_dir in dags_config/*/; do \
		sys=$$(basename $$sys_dir); \
		$(MAKE) smoke SYSTEM=$$sys; \
	done
