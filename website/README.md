# Uni Documentation

Documentation site for Uni, the embedded multi-model graph database.

## Setup

```bash
# Install dependencies
poetry install

# Convert Jupyter notebooks to Markdown (required before build/serve)
poetry run python scripts/convert_notebooks.py

# Serve locally with hot reload
poetry run zensical serve

# Build static site
poetry run zensical build
```

## Structure

```
website/
├── docs/                    # Documentation source files
│   ├── index.md            # Home page
│   ├── getting-started/    # Installation, quickstart, CLI
│   ├── concepts/           # Architecture, data model, etc.
│   ├── guides/             # Cypher, vector search, ingestion
│   ├── internals/          # Implementation details
│   ├── reference/          # API, config, troubleshooting
│   └── assets/             # Custom CSS and JS
├── scripts/                # Build and notebook generation scripts
├── mkdocs.yml              # Zensical configuration (backward-compatible format)
└── pyproject.toml          # Python dependencies
```

## Development

The site uses [Zensical](https://zensical.org/) (from the creators of Material for MkDocs).

### Local Preview

```bash
# Convert notebooks first (only needed after notebook changes)
poetry run python scripts/convert_notebooks.py

poetry run zensical serve
```

Visit http://localhost:8000

### Building

```bash
poetry run python scripts/convert_notebooks.py
poetry run zensical build
```

Output goes to `site/` directory.

### Locy Notebook Generation

Locy example notebooks are generated and should not be edited manually.

```bash
# Regenerate Locy Python + Rust notebooks
uv run python website/scripts/generate_locy_notebooks.py

# Verify notebooks are in sync (used in PR checks)
uv run python website/scripts/generate_locy_notebooks.py --check

# Generate the flagship semiconductor Locy notebook
uv run python website/scripts/generate_semiconductor_flagship_notebook.py

# Execute + validate flagship notebook outputs
uv run --with ./bindings/uni-db python website/scripts/verify_semiconductor_flagship_notebook.py

# Prepare pharma flagship dataset bundle
uv run python website/scripts/prepare_pharma_batch_genealogy_data.py

# Generate flagship #2 pharma Locy notebook
uv run python website/scripts/generate_pharma_flagship_notebook.py

# Execute + validate flagship #2 notebook outputs
uv run --with ./bindings/uni-db python website/scripts/verify_pharma_flagship_notebook.py

# Prepare cyber flagship #3 dataset bundle
uv run python website/scripts/prepare_cyber_exposure_twin_data.py

# Generate flagship #3 cyber Locy notebook
uv run python website/scripts/generate_cyber_flagship_notebook.py

# Execute + validate flagship #3 notebook outputs
uv run --with ./bindings/uni-db python website/scripts/verify_cyber_flagship_notebook.py
```

## Deployment

The site is deployed to GitHub Pages via the `deploy-docs` GitHub Actions workflow. It runs automatically on releases and can be triggered manually via `workflow_dispatch`.
