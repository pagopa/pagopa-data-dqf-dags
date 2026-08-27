# pagopa-data-dqf-dags

DAG Airflow del pagoPA Data Quality Framework: un template unico, una config per ambiente, deploy su Cloudera Data Engineering (CDE).

## Overview

Questo repo è il livello di **orchestrazione** dei controlli di data quality: genera e deploya i DAG Airflow che, su CDE, lanciano i controlli. Non contiene il motore DQ né i data contract.

Il DAG deployato non esegue i controlli: per ogni entità triggera un job Spark su CDE (`dq-quality-<env>`) — il vero motore, che vive fuori da qui — passandogli il percorso del data contract e i parametri necessari all'esecuzione. I data contract YAML stanno in un repo separato impostato tramite configurazione del singolo DAG.

Il problema che risolve è la proliferazione di DAG quasi identici. Ogni combinazione di *system* (es. `gpd`) e *env* (`dev`, `test`, `prod`) ha bisogno del suo DAG, ma differiscono solo per configurazione. Invece di duplicare file o affidarsi a un template engine, qui c'è **un solo template Python** e **un file JSON di config per combinazione**; uno script li fonde e produce il DAG concreto da caricare su CDE.

```
dag/pagopa_dq.py              template unico (Python valido, default gpd/dev)
dags_config/<system>/<env>.json   config per combinazione
scripts/render.py             fonde template + config -> DAG concreto
scripts/deploy.sh             render + upload su CDE (+ create job)
tests/unit/                   parse-check dei DAG renderizzati
```

## Installazione

Si lavora sempre dentro il **Dev Container** in `.devcontainer/` (Python 3.11-slim + `make`/`git`): apri il repo in VS Code e scegli "Reopen in Container". Al primo avvio il `postCreateCommand` esegue già `pip install -e .[dev]`, quindi l'ambiente è pronto senza altri passi. Per il deploy serve inoltre la CLI `cde` di Cloudera già configurata (fuori dal container, sulla propria macchina).

Se per qualche motivo non usi il container (es. CI, o esecuzione locale senza Docker), Python ≥ 3.10 basta (la CI gira su 3.11):

```bash
make install          # = pip install -e .[dev]
```

Le dipendenze sono tutte di sviluppo (Airflow, `cloudera-airflow-provider`, `pendulum`, `pytest`, `ruff`, `pre-commit`): il progetto stesso non installa codice, vedi sotto.

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

**Checklist prima di deployare.** Prima di lanciare `deploy.sh` verifica:

- `make lint` e `make test` passano.
- `make smoke SYSTEM=<system>` passa per il system che stai toccando (render di ogni env + parse Python).
- Il rendered in `dist/` è quello atteso: apri il file e controlla a occhio i valori delle costanti (`ENV`, `SYSTEM`, `CONTRACTS`, `DL_LAYER`, `SCHEDULE`, `OWNER`, `EMAIL`) — un errore lì passa i controlli automatici ma sbaglia ambiente o destinazione a runtime.
- I percorsi in `CONTRACTS` esistono davvero nel repo dei data contract, sulla `REF` indicata.
- Se hai toccato `dl_layer`, `watermark_column`/`watermark_from`/`watermark_bootstrap_from`, `xref_datasets` o `dataset_pk_map`: il motore a valle supporta già quel parametro per quell'entità (il template non lo valida, lo passa e basta).
- `watermark_bootstrap_from` è opzionale: entra in gioco *solo* quando un check incrementale non ha ancora storico (tabella results assente, o presente ma senza righe per quel check). Se manca in quel momento, il motore fallisce solo per quel check — non a priori per l'intero run. Se lasci il parametro assente in config e prima o poi un check nuovo ne avrà bisogno, il job fallirà finché non lo valorizzi. È idempotente e non richiede mai un secondo deploy per essere tolto: resta inerte una volta che ogni check ha il suo storico in tabella. Diverso da `watermark_from`: quello forza *tutti* i check ad ogni run schedulata finché non lo rimuovi da config — per un backfill una tantum va passato via `dag_run.conf`/`cde job run --arg`, non messo qui.
- Stai deployando l'env giusto: `--create` su un job già esistente lo ricrea, e su `prod` un errore di config diventa visibile solo alla prossima esecuzione schedulata.

Deploya su CDE — render, poi upload del file nella resource `pagopa-dqf-dags`; con `--create` crea anche il job Airflow, senza aggiorna solo il file già caricato:

```bash
./scripts/deploy.sh gpd test --create
```

> **Deploy: oggi manuale.** Il comando sopra va lanciato a mano da chi ha la CLI `cde` configurata in locale. La CI (`.github/workflows/ci.yml`) al momento fa solo lint + test su ogni push/PR, non deploya nulla. È prevista l'automazione del deploy via CI/CD (render + upload + create job su push/tag).

## Perché è fatto così

**Il DAG orchestra, non esegue.** Ogni task è un `CdeRunJobOperator` che triggera il job Spark CDE `dq-quality-<env>` già esistente — il motore DQ vero, esterno a questo repo. Il DAG gli passa solo argomenti: quale data contract usare (`--contract-path`), da quale repo e ref clonarlo (`--repository`, `--ref`), su quale layer del Data Lake scrivere gli esiti (`--dl-layer`), l'identità del run (`--dag-id`, `--airflow-run-id`). Così questo repo resta leggero e il motore evolve in modo indipendente.

**Un task per contratto, in parallelo, con parametri opzionali per entità.** Il DAG cicla su `CONTRACTS` e crea un task per ogni entità, tutti figli dell'unico `log_env` e quindi eseguiti in parallelo. Oltre al contratto, ogni entità può ricevere parametri presenti nella config e passati al job Spark **solo se valorizzati**: `xref_datasets`, `dataset_pk_map` (chiavi primarie), `watermark_column` / `watermark_from` / `watermark_bootstrap_from` (lettura incrementale — quest'ultimo obbligatorio lato motore se il contract usa il placeholder incrementale).
