# Uni Documentation

Documentation site for Uni, the embedded multi-model graph database.

## Setup

```bash
# Install dependencies
poetry install

# Serve locally with hot reload
poetry run mkdocs serve

# Build static site
poetry run mkdocs build
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
│   ├── stylesheets/        # Custom CSS
│   └── javascripts/        # Custom JS
├── mkdocs.yml              # MkDocs configuration
└── pyproject.toml          # Python dependencies
```

## Development

The site uses [MkDocs](https://www.mkdocs.org/) with the [Material](https://squidfunk.github.io/mkdocs-material/) theme.

### Local Preview

```bash
poetry run mkdocs serve
```

Visit http://localhost:8000

### Building

```bash
poetry run mkdocs build
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

The site can be deployed to GitHub Pages:

```bash
poetry run mkdocs gh-deploy
```
