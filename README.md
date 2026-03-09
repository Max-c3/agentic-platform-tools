# agentic-platform-tools

Monorepo containing the shared contract/runtime package plus the platform-specific recruiting tool packages.

## Packages

- `agentic-tools-core`
- `agentic-tools-ashby`
- `agentic-tools-gem`
- `agentic-tools-harmonic`
- `agentic-tools-metaview`

## Purpose

- `agentic-tools-core` defines the shared contract, registry, policy, checkpoint, and verification runtime.
- The platform packages define the actual tool implementations and their generated `tool_catalog.json` files.
- This repo is the source of truth for tool logic. The OpenCode runtime repo consumes these packages via editable installs during local development.

## Local testing

Run `pytest -q` inside any package directory to test that package.
