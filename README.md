# pagopa-data-dqf-dags

DAG Airflow del pagoPA Data Quality Framework: un template unico, una config per ambiente, deploy su Cloudera Data Engineering (CDE).

## Overview

Questo repo è il livello di **orchestrazione** dei controlli di data quality: genera e deploya i DAG Airflow che, su CDE, lanciano i controlli. Non contiene il motore DQ né i data contract.

Il DAG deployato non esegue i controlli: per ogni entità triggera un job Spark su CDE (`dq-quality-<env>`) — il vero motore, che vive fuori da qui — passandogli il percorso del data contract e i parametri del controllo. I data contract YAML stanno in un repo separato (`carlomanco-qty/qty-data-contracts`).

Il problema che risolve è la proliferazione di DAG quasi identici. Ogni combinazione di *system* (es. `gpd`) e *env* (`dev`, `test`, `prod`) ha bisogno del suo DAG, ma differiscono solo per configurazione. Invece di duplicare file o affidarsi a un template engine, qui c'è **un solo template Python** e **un file JSON di config per combinazione**; uno script li fonde e produce il DAG concreto da caricare su CDE.

```
dag/pagopa_dq.py              template unico (Python valido, default gpd/dev)
dags_config/<system>/<env>.json   config per combinazione
scripts/render.py             fonde template + config -> DAG concreto
scripts/deploy.sh             render + upload su CDE (+ create job)
tests/unit/                   parse-check dei DAG renderizzati
```

## Installazione

Serve Python ≥ 3.10 (la CI gira su 3.11). Per il deploy serve inoltre la CLI `cde` di Cloudera già configurata.

```bash
make install          # = pip install -e .[dev]
```

Le dipendenze sono tutte di sviluppo (Airflow, `cloudera-airflow-provider`, `pendulum`, `pytest`, `ruff`, `pre-commit`): il progetto stesso non installa codice, vedi sotto. In alternativa è disponibile un Dev Container in `.devcontainer/`.

Il pre-commit fa scan dei segreti con ggshield e richiede `GITGUARDIAN_API_KEY` nell'ambiente:

```bash
make pre-commit-install
```

## Utilizzo

Il ciclo è: modifichi il template o una config → renderizzi → verifichi → deployi.

Renderizza il DAG per una combinazione (l'output va in `dist/`, ignorato da git):

```bash
make render SYSTEM=gpd ENV=test
# -> dist/dag_dq_gpd_quality_test.py
```

Verifica che tutti i DAG di un system siano Python valido, e lancia i test (che renderizzano ogni config e la parsano con l'Airflow DagBag):

```bash
make smoke SYSTEM=gpd
make test
```

Deploya su CDE — render, poi upload del file nella resource `pagopa-dqf-dags`; con `--create` crea anche il job Airflow, senza aggiorna solo il file già caricato:

```bash
./scripts/deploy.sh gpd test --create
```

Per aggiungere un ambiente basta un nuovo `dags_config/<system>/<env>.json`; per un nuovo system, una nuova cartella `dags_config/<system>/`. Il template non si tocca.

## Perché è fatto così

**Un template solo, renderizzato patchando l'AST.** `dag/pagopa_dq.py` è un DAG completo e valido con valori di default (system `gpd`, env `dev`): non è un template Jinja, non ha placeholder, gira così com'è. Al deploy `scripts/render.py` lo legge con `ast.parse`, sostituisce il valore delle costanti top-level (`ENV`, `SYSTEM`, `CONTRACTS`, `SCHEDULE`, …) con quelli del JSON e riscrive il file con `ast.unparse`. Il vantaggio rispetto a un template engine è che il sorgente resta sempre importabile, lintabile con ruff e parsabile da Airflow: lo apri, lo esegui e lo testi senza renderizzare nulla. Nota che `system` ed `env` non sono nel JSON — vengono dedotti dal percorso `dags_config/<system>/<env>.json`, così la config non può contraddire il nome del file. (`ast.unparse` scarta i commenti: il file in `dist/` è più scarno del template ed è un artefatto di build, non si edita a mano.)

**Il repo non è un package Python, di proposito.** In `pyproject.toml`, `packages = []` e `py-modules = []`, con il commento che spiega il perché: il repo contiene template, config e script, non moduli importabili, e senza questa dichiarazione l'editable install fallirebbe perché setuptools vedrebbe `dag/` e `dags_config/` come due package top-level in conflitto. `pip install -e .[dev]` serve solo a tirare dentro le dipendenze di sviluppo.

**Il DAG orchestra, non esegue.** Ogni task è un `CdeRunJobOperator` che triggera il job Spark CDE `dq-quality-<env>` già esistente — il motore DQ vero, esterno a questo repo. Il DAG gli passa solo argomenti: quale data contract usare (`--contract-path`), da quale repo e ref clonarlo (`--repository`, `--ref`), su quale layer scrivere gli esiti (`--dl-layer`), l'identità del run (`--dag-id`, `--airflow-run-id`). Così questo repo resta leggero e il motore evolve in modo indipendente. Ne discende la topologia del deploy: un job Airflow per ogni combinazione (system, env), tutti nella stessa resource `pagopa-dqf-dags`, che a runtime chiamano l'unico job Spark del motore.

**ENV e SYSTEM iniettati come variabili d'ambiente Spark a runtime.** Non viaggiano come argomenti CLI ma come `spark.kubernetes.driverEnv` / `spark.executorEnv` nella `conf` dell'override. Il commit che l'ha introdotto lo motiva nel commento: il motore legge `ENV`/`SYSTEM` via `os.getenv`, e quei valori restano "cotti" nel job Spark al momento del `cde job create`; iniettarli dal DAG a runtime li sovrascrive, così lo stesso job Spark si riusa per ambienti diversi decisi dal DAG che lo chiama. (`SYSTEM` viaggia anche come argomento `--domain`: le due cose servono a punti diversi del motore a valle.)

**`dl_layer` è obbligatorio a valle e ha un default qui.** Il layer del Data Lake su cui il motore scrive gli esiti prefissa le tabelle di output — `<dl_layer>_dqf_<system>_results` e `_failed_records` — e con esse il lookup del watermark incrementale, quindi due layer diversi hanno storici e watermark indipendenti. A differenza degli altri parametri non è opzionale per il motore, quindi `DL_LAYER` è una costante del template con default `"silver"` (tutti i contratti attuali sono silver) e viene passata **sempre**, non "solo se valorizzata". La config la dichiara comunque esplicitamente in ogni `dags_config/<system>/<env>.json`: dato che il valore decide su quale tabella finiscono gli esiti, lasciarlo implicito renderebbe invisibile in config un cambio di destinazione.

**Un task per contratto, in parallelo, con parametri opzionali per entità.** Il DAG cicla su `CONTRACTS` e crea un task per ogni entità, tutti figli dell'unico `log_env` e quindi eseguiti in parallelo. Oltre al contratto, ogni entità può ricevere parametri presenti nella config e passati al job Spark **solo se valorizzati**: `xref_datasets`, `dataset_pk_map` (chiavi primarie), `watermark_column` / `watermark_from` (lettura incrementale). Sono stati aggiunti uno alla volta man mano che il motore li supportava, ed è il motivo della forma "aggiungi l'argomento se la mappa contiene l'entità": una config minimale resta valida, una config ricca attiva controlli in più. Il significato preciso di questi parametri lo definisce il motore a valle, non questo repo; i nomi ne indicano l'intento — rispettivamente dataset correlati per l'integrità referenziale, unicità sulle chiavi, incrementalità.
