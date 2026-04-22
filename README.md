# bydelsfakta-databricks

Databricks replacement for the
[bydelsfakta-data-processing](https://github.com/oslokommune/bydelsfakta-data-processing)
Lambda functions. Transforms Excel input data into per-district JSON files for
[bydelsfakta.oslo.kommune.no](https://bydelsfakta.oslo.kommune.no).

## Architecture

**Data flow:** Excel files land in a Unity Catalog (UC) Volume called
`bronze_default`, are read and transformed by Python jobs on Databricks using
pandas, and written out as per-district JSON files to another UC Volume
(`gold_default`). An intermediate CSV (`silver_default`) is also written for
each dataset.

Each dataset is a standalone Python entry point that reads Excel input, resolves
geography IDs via lookup tables (`geography.py`), aggregates data at three
levels (delbydel → bydel → Oslo i alt), and generates per-district JSONi using
the `Output`/`Template` classes.

Jobs are deployed as a Python wheel via Databricks Bundles (DAB) and run
on serverless compute (Python 3.12). File arrival triggers automatically run all
dependent jobs when new Excel files are uploaded.

## Project structure

```
src/bydelsfakta/
├── geography.py          # Bydel/delbydel lookup tables
├── io.py                 # Excel reading (local/Volume), JSON/CSV writing
├── aggregate.py          # Three-level aggregation (delbydel → bydel → Oslo)
├── output.py             # Per-district JSON output generation
├── population_utils.py   # Age-distributed population calculations
├── templates.py          # Value generation (TemplateA, B, C, E, G, H, I, J, K)
├── transform.py          # Status/historic data splitting
├── util.py               # Min/max calculations, district lookups
└── jobs/                 # One module per dataset (25 entry points)
    ├── utdanning.py
    ├── folkemengde.py
    └── ...

resources/                # Databricks Bundle job definitions
├── utdanning_job.yml     # One file per job module (status + historisk variants)
├── ...
└── trigger_jobs.yml      # File arrival triggers

scripts/
├── create_upload_dirs.sh # Create upload directories in the uploads Volume
└── upload_input.sh       # Upload Excel files from input/ to the Volume

tests/                    # pytest test suite
```

Each job module follows the same pattern:
- `read_*_excel(path)` — reads Excel, resolves geography, normalizes columns
- `transform_*(df, type_of_ds)` — transforms data, returns JSON output list
- `main()` — CLI entry point (argparse with `--input-dir`, `--output-dir`, etc.)

## Commands

| Task             | Command                                |
|------------------|----------------------------------------|
| Run tests        | `make test`                            |
| Lint             | `make lint`                            |
| Format           | `make format`                          |
| Build wheel      | `make build`                           |
| Bundle validate  | `make validate`                        |
| Deploy (dev)     | `make deploy-dev`                      |
| Deploy (prod)    | `make deploy-prod`                     |

## Dependencies and the isolated VPC

The Databricks workspace runs in an isolated VPC with no internet access. This
has two consequences:

1. **`dependencies = []` in pyproject.toml.** Runtime dependencies like pandas
   and numpy are pre-installed on Databricks Runtime and must not be listed as
   PyPI dependencies (pip can't reach PyPI).

2. **`dist/deps/` contains vendored wheels.** openpyxl (and its dependency
   et_xmlfile) are not part of Databricks Runtime, so they are pre-built as
   wheels and included alongside the project wheel. The job YAML references
   both: `../dist/*.whl` (project) and `../dist/deps/*.whl` (vendored deps).

For local development and testing, use `uv run --group test` which installs the
test dependency group (pandas, numpy, pytest, openpyxl).

## Resource configuration (DAB)

Job definitions, triggers, and UC Volumes are declared as Databricks Bundle
(DAB) resources in `databricks.yml` and `resources/*.yml`, rather than in a
separate IaC repository. This is intentional: the job definitions are tightly
coupled to the code — they reference specific entry points, CLI parameters, and
Volume paths that change together with the Python code. Keeping them in the same
repo ensures they stay in sync and can be reviewed in the same PR.

What lives *outside* this repo (in IaC): the Databricks workspace itself, the
Unity Catalog catalog, and network/VPC configuration. These live in `padda-iac`.

### Job structure

Each dataset has one or two Databricks jobs (typically a `status` and a
`historisk` variant) defined in `resources/<dataset>_job.yml`. Both variants run
the same Python entry point with different `--type-of-ds` arguments.

### File arrival triggers

`resources/trigger_jobs.yml` defines file arrival triggers on the uploads
Volume. When a new Excel file lands in a subdirectory (e.g.,
`uploads/utdanning/`), the trigger fires and runs all jobs that depend on that
input. Some inputs are shared: for instance, uploading
`befolkning-etter-kjonn-og-alder` triggers 20 different jobs across multiple
datasets.

### Volume warning

The `gold_output` volume's underlying S3 location is hardcoded in
`bydelsfakta-api/serverless.yaml`. If this volume is ever deleted and recreated,
the S3 path will change and the API config must be updated.
