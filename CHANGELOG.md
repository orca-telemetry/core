# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

### Added
- Added a gRPC procedure to expose the internal state of Orca (Window types and Algorithms)

## [v0.10.2] - 02-12-2025

### Fixed

- Volume configuration within the Orca CLI to be compatible with PostgreSQL 18

## [v0.10.1] - 28-09-2025

### Changed

- Update CLI with some UX improvements

## [v0.10.0] - 28-09-2025

### Changed

- How algorithm definitions are defined as unique. Unique definitions are now a composite of algorithm, processor, and window type.
- How metadata fields are controlled within the DB. Each metadata field is now checked for consistency with existing window types that use them. If consistency is broken - an error is raised, with the fix being to bump the version of the window type.

## [v0.9.0]

### Changed

- Made metadata creation way more explicit with dedicated messages.

## [v0.8.0]

### Added

- TLS when communicating with processors in production

## [v0.7.0] - 14-09-2025

### Fixed

- Mismatch in DAG resolver

### Updated

- The way key configurations are stored, e.g. database URL, port and log level - as environment variables instead of command line args

## [v0.6.3] - 12-09-2025

### Fixed 

- Changed to orc-analytics.

### Added

- Data operations within the gRPC service so common operations can be performed on the orca data.
- Annotation operation so annotations can be left on periods of time

## [v0.5.0] - 15-06-2025

### Added

- Added check on processor registration whether the the registration introduces a circular dependancy

### Fixed

- Updated the install script to create common install dirs if they do not exist
- Updated CLI to safe render colours given the term config. Avoids issues when using a terminal with poor X11 support

## [v0.4.0] - 18-05-2025

### Modified

- Modified the protobufs to use `time_from` and `time_to` rather than `from` and `to` to avoid
  clashes with reserved words

## [v0.3.0] - 15-05-2025

### Added

- Printout of the docker network gateway IP on `orca status`
- A timer on the postgres instance being ready to accept connections before connecting

## [v0.2.0] - 12-05-2025

- Moved orca core deployement to github packages
- Integrated orca core package into CLI

## [v0.1.0] - 12-05-2025

### Added

- CLI & converted repo to a monorepo.
- Updated build stages.

## [v0.0.0] - 06-05-2025

### Added

- Initial implementation that accepts processor registrations, and can emit windows to processors.
- Can now handle results.
- Only the dependencies that the stage needs are passed in.

### Changed

### Removed

[unreleased]: https://github.com/orc-analytics/Orca/compare/v0.10.1...HEAD
[v0.10.1]: https://github.com/orc-analytics/Orca/compare/v0.10.0...v0.10.1
[v0.10.0]: https://github.com/orc-analytics/Orca/compare/v0.9.0...v0.10.0
[v0.9.0]: https://github.com/orc-analytics/Orca/compare/v0.8.0...v0.9.0
[v0.8.0]: https://github.com/orc-analytics/Orca/compare/v0.7.0...v0.8.0
[v0.7.0]: https://github.com/orc-analytics/Orca/compare/v0.6.0...v0.7.0
[v0.6.0]: https://github.com/orc-analytics/Orca/compare/v0.5.0...v0.6.0
[v0.5.0]: https://github.com/orc-analytics/Orca/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/orc-analytics/Orca/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/orc-analytics/Orca/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/orc-analytics/Orca/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/orc-analytics/Orca/compare/v0.0.0...v0.1.0
