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
```

## Deployment

The site can be deployed to GitHub Pages:

```bash
poetry run mkdocs gh-deploy
```
