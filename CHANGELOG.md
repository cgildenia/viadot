# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Tasks:
  - DownloadGitHubFile
  - AzureDataLakeDownload
  - AzureDataLakeUpload
  - AzureDataLakeToDF
  - ReadAzureKeyVaultSecret
  - CreateAzureKeyVaultSecret

### Changed
- tasks now use secrets for credential management (azure tasks use azure Key Vault secrets)


### Removed


### Fixed
- Fix SQLite tests