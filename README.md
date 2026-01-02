![Group 14 (1)](https://github.com/user-attachments/assets/f3725551-c19e-44cd-a8d4-f268bce5ac2a)

![GitHub Release](https://img.shields.io/github/v/release/orc-analytics/orca)

[Orca](https://orc-a.io) is a analytics orchestration framework that makes it easy for
development and product teams to extract real-time insights from real-time data. It provides a
structured and scalable way to schedule, process, and analyse data, at scale, using a
time window based triggering mechanism. This means Orca is perfectly suited to tasks that involve
capturing regions of time, as they occur, and running analyses on them, whilst also allowing
dependencies between analyses and nested time windows (i.e. analysis can trigger other analyses).

## ✨ Features

- [ ] Orca Core:
  - [x] Build cross-language algorithms and scale horizontally. [DOCS](https://orc-a.io/docs)
  - [x] Automatically handles algorithm dependencies and execution order. [DOCS](https://orc-a.io/docs)
  - [ ] Telemetry cache, so downstream analytics don't hammer the database for the same data.
- [ ] Supported Storage Solutions
  - [x] PostgreSQL. [DOCS](https://orc-a.io/docs)
  - [ ] MongoDB
  - [ ] BigQuery
  - [ ] RDS
- [ ] SDKs
  - [x] [Python](https://github.com/orc-analytics/orca-python)
  - [ ] Go!
  - [ ] Node
  - [ ] Rust

## Documentation

Full documentation can be found at [orc-a.io/docs](https://orc-a.io/docs).

## 📄 License

Orca is licensed under the [GNU General Public License v3.0](./LICENSE.md).
