# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.1] - 2024-09-27
### Details
#### Changed
- Bump clap from 4.5.17 to 4.5.18

Bumps [clap](https://github.com/clap-rs/clap) from 4.5.17 to 4.5.18.
- [Release notes](https://github.com/clap-rs/clap/releases)
- [Changelog](https://github.com/clap-rs/clap/blob/master/CHANGELOG.md)
- [Commits](https://github.com/clap-rs/clap/compare/clap_complete-v4.5.17...clap_complete-v4.5.18)

---
updated-dependencies:
- dependency-name: clap
  dependency-type: direct:production
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] <support@github.com>
Co-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com> by @dependabot[bot] in [#27](https://github.com/vigimite/aqueducts/pull/27)
- Bump thiserror from 1.0.63 to 1.0.64

Bumps [thiserror](https://github.com/dtolnay/thiserror) from 1.0.63 to 1.0.64.
- [Release notes](https://github.com/dtolnay/thiserror/releases)
- [Commits](https://github.com/dtolnay/thiserror/compare/1.0.63...1.0.64)

---
updated-dependencies:
- dependency-name: thiserror
  dependency-type: direct:production
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] <support@github.com>
Co-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com> by @dependabot[bot] in [#26](https://github.com/vigimite/aqueducts/pull/26)
- Bump arrow-odbc from 12.1.0 to 12.2.0

Bumps [arrow-odbc](https://github.com/pacman82/arrow-odbc) from 12.1.0 to 12.2.0.
- [Changelog](https://github.com/pacman82/arrow-odbc/blob/main/Changelog.md)
- [Commits](https://github.com/pacman82/arrow-odbc/compare/v12.1.0...v12.2.0)

---
updated-dependencies:
- dependency-name: arrow-odbc
  dependency-type: direct:production
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] <support@github.com>
Co-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com> by @dependabot[bot] in [#25](https://github.com/vigimite/aqueducts/pull/25)
- Merge branch 'main' of https://github.com/vigimite/aqueducts

#### Fixed
- Incorrect record batch handling for custom ODBC insert case by @vigimite
- Incorrect record batch handling for custom ODBC insert case (2nd case)

## [0.6.0] - 2024-09-17
### Details
#### Changed
- Updated deps, make destination name public, include ')' for ttl check by @vigimite
- Add Custom write mode for ODBC destination by @FGahan in [#23](https://github.com/vigimite/aqueducts/pull/23)

#### Removed
- Remove object_store as a dependency (unused) by @vigimite

## [0.5.0] - 2024-08-20
### Details
#### Added
- Add InMemory source and destination by @vigimite

#### Changed
- Update release workflow by @vigimite
- Update schema docs to include ODBC destinations by @vigimite
- Bump datafusion to v40 by @vigimite
- Bump datafusion to v41 and delta-rs to 0.19.0 by @vigimite
- Update object_store requirement from 0.10 to 0.11

Updates the requirements on [object_store](https://github.com/apache/arrow-rs) to permit the latest version.
- [Changelog](https://github.com/apache/arrow-rs/blob/master/CHANGELOG-old.md)
- [Commits](https://github.com/apache/arrow-rs/compare/object_store_0.10.0...object_store_0.10.2)

---
updated-dependencies:
- dependency-name: object_store
  dependency-type: direct:production
...

Signed-off-by: dependabot[bot] <support@github.com>
Co-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com> by @dependabot[bot] in [#20](https://github.com/vigimite/aqueducts/pull/20)

## [0.4.0] - 2024-07-28
### Details
#### Added
- Add support for ODBC destinations by @vigimite in [#17](https://github.com/vigimite/aqueducts/pull/17)

#### Changed
- Make json schema generation optional by @vigimite

## [0.3.2] - 2024-07-06
### Details
#### Changed
- Revert "prepare release v0.3.1"

This reverts commit 38d4980a888255348531121e6e542cc4882f3542. by @vigimite

#### Fixed
- Don't coerce LargeUtf8 and LargeBinary into smaller types

#### Removed
- Revert "fix: remove schema validation due to incorrect casting of LargeUtf8"

This reverts commit a924aaf86a721a15134965094c5eb1b65bb5e6a2. by @vigimite

~## [0.3.1] - 2024-07-06~ **YANKED**
~### Details~
~#### Removed~
~- Remove schema validation due to incorrect casting of LargeUtf8 by @vigimite~

## [0.3.0] - 2024-06-24
### Details
#### Changed
- Upgrade deltalake to 0.18.1 by @vigimite

## [0.3.0-rc1] - 2024-06-22
### Details
#### Added
- Add json support, add JSONSchema, add docs by @vigimite

#### Changed
- Upgrade datafusion to 0.39.0 by @vigimite
- Add initial mkDocs documentation by @vigimite
- Update serde_yml requirement from 0.0.8 to 0.0.10 
- Implement parallel processing of stages by @vigimite in [#14](https://github.com/vigimite/aqueducts/pull/14)
- Logging changes by @vigimite
- Feat (aqueducts-cli): enable json querying functionality by @vigimite
- Improve param substitution handling by @vigimite
- Add changelog by @vigimite

#### Fixed
- Delta-rs regression https://github.com/delta-io/delta-rs/issues/2602 by @vigimite

## [0.2.2] - 2024-05-27
### Details
#### Changed
- Add dependabot config by @vigimite
- V0.2.2 dont install cli with odbc by default by @vigimite

## [0.2.1] - 2024-05-27
### Details
#### Added
- Add parsing aqueducts definition from string by @FGahan in [#3](https://github.com/vigimite/aqueducts/pull/3)

#### Changed
- Add badges to README and decrease logo size by @vigimite
- Bump version to v0.2.1 by @vigimite

## New Contributors
* @FGahan made their first contribution in [#3](https://github.com/vigimite/aqueducts/pull/3)

## [0.2.0] - 2024-05-25
### Details
#### Changed
- Cargo fmt by @vigimite
- Fix readme typo by @erjanmx
- Merge pull request #1 from erjanmx/fix-readme-typo

Fix readme typo by @vigimite in [#1](https://github.com/vigimite/aqueducts/pull/1)
- Added initial odbc support for source by @vigimite
- Fix readme typo by @erjanmx
- Updated README by @vigimite
- Merge pull request #2 from vigimite/odbc_arrow_support

feat: add odbc support for source by @vigimite in [#2](https://github.com/vigimite/aqueducts/pull/2)

## New Contributors
* @erjanmx made their first contribution

## [0.1.2] - 2024-05-22
### Details
#### Changed
- Added storage_options param to file and dir sources by @vigimite
- Increase version to 0.1.2 by @vigimite
- Add object store registration for file based destinations by @vigimite
- Change ref of vec to slice by @vigimite

#### Fixed
- Fix clippy lints by @vigimite
- Fix clippy lints for cli by @vigimite

## [0.1.1] - 2024-05-21
### Details
#### Changed
- Update readme by @vigimite
- Updated release workflow and bump version by @vigimite
- Removed unnecessary condition by @vigimite

## [0.1.0] - 2024-05-21
### Details
#### Changed
- Initial commit by @vigimite
- Initial commit by @vigimite
- Update image position by @vigimite
- Move logo to root by @vigimite
- Updates to Cargo.toml by @vigimite
- Add version to aqueducts-cli for aqueducts by @vigimite
- Add ci test & build and fix incorrect email by @vigimite
- Remove verbose flag from builds and tests by @vigimite
- Add caching to github workflow by @vigimite
- Add release workflow by @vigimite
- Cleaned up workflows by @vigimite

[0.6.1]: https://github.com/vigimite/aqueducts/compare/v0.6.0..v0.6.1
[0.6.0]: https://github.com/vigimite/aqueducts/compare/v0.5.0..v0.6.0
[0.5.0]: https://github.com/vigimite/aqueducts/compare/v0.4.0..v0.5.0
[0.4.0]: https://github.com/vigimite/aqueducts/compare/v0.3.2..v0.4.0
[0.3.2]: https://github.com/vigimite/aqueducts/compare/v0.3.1..v0.3.2
[0.3.1]: https://github.com/vigimite/aqueducts/compare/v0.3.0..v0.3.1
[0.3.0]: https://github.com/vigimite/aqueducts/compare/v0.3.0-rc1..v0.3.0
[0.3.0-rc1]: https://github.com/vigimite/aqueducts/compare/v0.2.2..v0.3.0-rc1
[0.2.2]: https://github.com/vigimite/aqueducts/compare/v0.2.1..v0.2.2
[0.2.1]: https://github.com/vigimite/aqueducts/compare/v0.2.0..v0.2.1
[0.2.0]: https://github.com/vigimite/aqueducts/compare/v0.1.2..v0.2.0
[0.1.2]: https://github.com/vigimite/aqueducts/compare/v0.1.1..v0.1.2
[0.1.1]: https://github.com/vigimite/aqueducts/compare/v0.1.0..v0.1.1

<!-- generated by git-cliff -->
