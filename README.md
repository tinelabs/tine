# Tine

[![PyPI version](https://img.shields.io/pypi/v/tine?label=PyPI)](https://pypi.org/project/tine/)
[![PyPI downloads](https://static.pepy.tech/badge/tine)](https://pepy.tech/project/tine)
[![CI](https://img.shields.io/github/actions/workflow/status/tinelabs/tine/ci.yml?branch=main&label=CI)](https://github.com/tinelabs/tine/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-97ca00.svg)](./LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/tine)](https://pypi.org/project/tine/)

A branching notebook runtime for AI and humans.

Tine is a local-first execution engine where notebooks branch like code. Run work in the browser, connect an agent through MCP, and keep both attached to the same fast local runtime.

<p align="center">
  <strong>Built for agent workflows</strong> · <strong>MCP-native</strong> · <strong>Written in Rust</strong>
</p>

<p align="center">
  <img src="docs/assets/tine-demo.gif" alt="Tine branching workflow demo" width="920" />
</p>

## Get started

```bash
pip install tine
tine serve --open
```

That installs the wrapper, starts a local server, and opens the UI at <http://127.0.0.1:9473>.

Tine needs Python 3.10+. If your default `python` is older, install with a newer one explicitly, e.g. `python3.11 -m pip install tine`.

To run inside a specific folder as your workspace:

```bash
tine serve --workspace ./my-project --open
```

To skip the browser auto-open, drop `--open`.

To sanity-check the install:

```bash
tine doctor
```

## How it works

Tine is built around a tree-native notebook model:

- an **experiment** is the main working unit
- an experiment contains **branches**
- branches contain **cells**

Each experiment owns its own kernel and environment, so branches let you explore multiple paths in parallel against the same code without collapsing everything into one linear notebook. The Web UI and any connected agent share that local runtime, so they always see the same state, logs, and outputs.

## Desktop app

If you'd rather not install through pip, signed desktop bundles are published on every release:

- **macOS** — `.dmg` for Apple Silicon and Intel
- **Windows** — `.msi` for x86_64

Each bundle ships with a private Python runtime inside, so it doesn't depend on a system Python install.

Downloads: <https://github.com/tinelabs/tine/releases>

The desktop app uses the same local backend as `pip install tine` — only the wrapper differs.

## Use with AI agents

Tine is MCP-native. The MCP adapter connects your agent host to a running Tine
API server.

If you use the desktop app from a `.dmg` or `.msi`, open Tine first. The app
starts the local API at `http://127.0.0.1:9473` unless that port is already in
use, then falls back to another local port. The title bar shows an `MCP :<port>`
button with the actual port; click it to copy the matching `tine-mcp --api-url
...` command.

The desktop bundle's private Python runtime is for notebooks and kernels; it
does not install a global MCP command for VS Code, Cursor, or Claude to launch.
Install the small Python wrapper once so your agent host can start the MCP
stdio process:

```bash
pip install tine
```

Then generate a config for your agent host of choice:

```bash
tine mcp print-config --host vscode --api-url http://127.0.0.1:9473
```

Supported hosts: `vscode`, `cursor`, `claude`, `generic`.

To write the generated config directly into your host's standard config location:

```bash
tine mcp register --host vscode --api-url http://127.0.0.1:9473
```

Default config targets per host:

| Host | macOS | Linux | Windows |
| --- | --- | --- | --- |
| VS Code | `~/Library/Application Support/Code/User/mcp.json` | `~/.config/Code/User/mcp.json` | `%APPDATA%/Code/User/mcp.json` |
| Cursor | `~/Library/Application Support/Cursor/User/mcp.json` | `~/.config/Cursor/User/mcp.json` | `%APPDATA%/Cursor/User/mcp.json` |
| Claude | `~/Library/Application Support/Claude/claude_desktop_config.json` | `~/.config/Claude/claude_desktop_config.json` | `%APPDATA%/Claude/claude_desktop_config.json` |

To manage the file yourself, use `print-config` and paste it in manually.

A typical agent setup:

```bash
pip install tine
tine serve --open  # or open the Tine desktop app
tine mcp register --host vscode --api-url http://127.0.0.1:9473
```

You now have the Web UI and an agent both talking to the same local runtime.

## Supported targets

`pip install tine` resolves a matching engine binary for your OS and architecture on first use.

| OS | Architectures |
| --- | --- |
| macOS | Apple Silicon, Intel |
| Linux | x86_64, arm64 |
| Windows | x86_64 |

## Develop locally

If you're working inside this repo, run the Rust CLI directly instead of the packaged wrapper:

```bash
cargo run -p tine-cli -- serve --workspace . --open
```

Source development expects Rust 1.75+ and Python 3.10+.

The most relevant top-level areas:

- `ui/` — browser UI
- `crates/tine-server/` — local HTTP and WebSocket server
- `crates/tine-cli/` — local launcher and operator commands
- `packaging/python/` — Python wrapper, packaging, and MCP entrypoints

## Contributing

Contributions from both humans and AI agents are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution rules, focus areas, and validation guidance.

## Acknowledgements

Tine is grateful to the `ipykernel` and Jupyter communities. Their work helped establish the notebook and kernel patterns that make interactive computing and tool-driven workflows possible.
