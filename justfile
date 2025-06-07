# Serve documentation locally with hot reload
docs-serve:
    cd docs && mkdocs serve --dev-addr 127.0.0.1:8000

# Build documentation for deployment
docs-build:
    cd docs && mkdocs build

# Clean generated files
clean-docs:
    rm -rf docs/build/
    rm -f json_schema/aqueducts.schema.json
