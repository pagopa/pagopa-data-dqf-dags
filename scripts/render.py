"""Render del DAG sorgente sostituendo le costanti top-level con i valori del config JSON.

Il source resta un .py valido (default dev/gpd) e viene patchato via AST.
L'output è anch'esso .py valido.

Costanti patchate (mappa JSON-key -> Python-constant):
    env              -> ENV
    system           -> SYSTEM
    repository       -> REPOSITORY
    ref              -> REF
    dl_layer         -> DL_LAYER
    schedule         -> SCHEDULE
    contracts        -> CONTRACTS
    email            -> EMAIL
    owner            -> OWNER
    watermark_column -> WATERMARK_COLUMN
    watermark_from   -> WATERMARK_FROM
    watermark_bootstrap_from -> WATERMARK_BOOTSTRAP_FROM
    xref_datasets    -> XREF_DATASETS
    dataset_pk_map   -> DATASET_PK_MAP

Uso:
    python scripts/render.py <system> <env> <source.py> <config.json> <output.py>
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

CONFIG_TO_PY = {
    "env": "ENV",
    "system": "SYSTEM",
    "repository": "REPOSITORY",
    "ref": "REF",
    "dl_layer": "DL_LAYER",
    "schedule": "SCHEDULE",
    "contracts": "CONTRACTS",
    "email": "EMAIL",
    "owner": "OWNER",
    "watermark_column": "WATERMARK_COLUMN",
    "watermark_from": "WATERMARK_FROM",
    "watermark_bootstrap_from": "WATERMARK_BOOTSTRAP_FROM",
    "xref_datasets": "XREF_DATASETS",
    "dataset_pk_map": "DATASET_PK_MAP",
}


def _value_to_ast(value: object) -> ast.expr:
    return ast.parse(repr(value), mode="eval").body


def render(system: str, env_name: str, source_path: Path, config_path: Path, output_path: Path) -> None:
    if not config_path.exists():
        raise FileNotFoundError(f"Config non trovata: {config_path}")

    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["env"] = env_name
    config["system"] = system

    py_overrides = {CONFIG_TO_PY[k]: v for k, v in config.items() if k in CONFIG_TO_PY}

    tree = ast.parse(source_path.read_text(encoding="utf-8"))

    replaced: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and target.id in py_overrides:
            node.value = _value_to_ast(py_overrides[target.id])
            replaced.add(target.id)

    missing = set(py_overrides) - replaced
    if missing:
        raise RuntimeError(
            f"Le costanti {sorted(missing)} non sono state trovate in {source_path}. "
            "Devono essere assegnazioni top-level (es. `ENV = \"dev\"`)."
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(ast.unparse(tree) + "\n", encoding="utf-8")
    print(f"Rendered {output_path} from {source_path} with config {config_path}")


def main() -> None:
    if len(sys.argv) != 6:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    render(sys.argv[1], sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]), Path(sys.argv[5]))


if __name__ == "__main__":
    main()
