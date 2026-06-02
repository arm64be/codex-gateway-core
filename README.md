# codex-gateway-core

Shared Rust library for gateway applications that drive a local Codex app-server.

The core owns Codex thread/session state, turn queues, model and reasoning settings, goal
commands, session switching, interrupts, and app-server notification handling. Gateway
applications provide a channel key and an async output sink; they do not need to depend on or
interact with `codex-app-server` directly.

## Install

Add the library from the release tag that matches your gateway:

```toml
codex-gateway-core = { git = "https://github.com/arm64be/codex-gateway-core", tag = "v0.1.0" }
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license, at your option.
