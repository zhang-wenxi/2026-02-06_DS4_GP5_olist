import os
import subprocess
import json
from pathlib import Path

from dagster import (
    AssetExecutionContext, 
    Definitions, 
    define_asset_job, 
    asset, 
    AssetSelection,
    TableSchema,
    TableColumn,
    MetadataValue,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets, DbtProject

# 1. Setup Project Paths - FIXED THE PATH ERROR HERE
# .parents[1] goes up from dagster/definition.py to the root folder
ROOT_DIR = Path(__file__).resolve().parents[1] 
DBT_PROJECT_DIR = ROOT_DIR / "dbt_olist"

# 2. Environment Configuration Loader
def _load_dotenv():
    env_path = ROOT_DIR / ".env"
    if env_path.exists():
        with open(env_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip().strip('"').strip("'")

_load_dotenv()

# 3. Catalog Loading Helpers
def load_catalog_index(target_path: Path):
    catalog_path = target_path / "catalog.json"
    if not catalog_path.exists():
        return {}
    with open(catalog_path) as f:
        catalog = json.load(f)
    return {**catalog.get("nodes", {}), **catalog.get("sources", {})}

# 4. Custom Translator (The UI "Type" Fix)
class CustomTranslator(DagsterDbtTranslator):
    def __init__(self, catalog_index):
        super().__init__()
        self.catalog_index = catalog_index

    def get_metadata(self, dbt_resource_props):
        # 1. Get standard metadata
        metadata = super().get_metadata(dbt_resource_props)
        
        # 2. Extract column info
        unique_id = dbt_resource_props.get("unique_id")
        catalog_node = self.catalog_index.get(unique_id, {})
        catalog_columns = catalog_node.get("columns", {})
        manifest_columns = dbt_resource_props.get("columns", {})

        column_defs = []
        for name, info in manifest_columns.items():
            # Priority: Catalog -> YAML -> Fallback
            col_type = (
                catalog_columns.get(name, {}).get("type") 
                or info.get("data_type") 
                or "unknown"
            )
            column_defs.append(
                TableColumn(name=name, type=str(col_type), description=info.get("description"))
            )

        # 3. Manually inject the schema so it appears in the UI
        if column_defs:
            metadata["dagster/column_schema"] = MetadataValue.table_schema(
                TableSchema(columns=column_defs)
            )
        return metadata

# 5. Initialize dbt Project & Metadata
olist_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    state_path=DBT_PROJECT_DIR / "target",
)

# Auto-generate manifest if missing (helps new contributors)
manifest_path = olist_project.manifest_path
if not manifest_path.exists():
    print("manifest.json not found — running `dbt parse` to generate it...")
    subprocess.run(
        ["dbt", "parse", "--no-partial-parse"],
        check=True,
        cwd=str(DBT_PROJECT_DIR)
    )
    
# Load the catalog index once at startup
catalog_index = load_catalog_index(DBT_PROJECT_DIR / "target")

# 6. dbt Assets Definition
@dbt_assets(
    manifest=olist_project.manifest_path,
    dagster_dbt_translator=CustomTranslator(catalog_index)
)
def olist_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run", "--exclude", "package:dbt_expectations"], context=context).stream()
    yield from dbt.cli(["test", "--select", "package:dbt_expectations"], context=context).stream()

# 7. Quality & Metadata Assets
@asset(deps=[olist_dbt_assets], group_name="quality")
def quality_gate():
    return "Tests Completed"

@asset(deps=[quality_gate], group_name="metadata")
def dbt_docs_asset():
    env = {**os.environ}
    subprocess.run(["dbt", "docs", "generate"], check=True, cwd=str(DBT_PROJECT_DIR), env=env)

# 8. Definitions Registry
defs = Definitions(
    assets=[olist_dbt_assets, quality_gate, dbt_docs_asset], 
    jobs=[define_asset_job(name="run_full_pipeline", selection=AssetSelection.all())],
    resources={
        "dbt": DbtCliResource(
            project_dir=os.fspath(DBT_PROJECT_DIR),
            target_path="target"
        ),
    },
)
