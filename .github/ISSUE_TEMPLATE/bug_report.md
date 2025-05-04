---
name: Bug report
about: Create a report to help us improve Aqueducts
title: '[BUG] '
labels: bug
assignees: ''
---

## Bug Description
A clear and concise description of what the bug is.

## Reproduction Steps
Steps to reproduce the behavior:
1. Install '...'
2. Configure '...'
3. Run command '...'
4. See error

## Expected Behavior
A clear and concise description of what you expected to happen.

## Actual Behavior
What actually happened, including error messages, logs, or screenshots if applicable.

## Environment
- OS: [e.g. Ubuntu 22.04, Windows 11, macOS 14.0]
- Rust version: [e.g. 1.76.0]
- Aqueducts version: [e.g. 0.2.1]
- Feature flags used: [e.g. odbc, s3, yaml]
- Installation method: [e.g. cargo install, Docker]

## Configuration
Relevant configuration details (with sensitive information redacted):
```yaml
# Your pipeline configuration or other relevant configs
sources:
  - type: File
    name: example
    # ...
```

## Additional Context
Add any other context about the problem here. For example:
- Does the issue happen consistently or intermittently?
- Did this work in a previous version?
- Are you using any special environment variables?