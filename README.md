# Preservica Mass Modification Tool

[![Supported Versions](https://img.shields.io/pypi/pyversions/preservica_mass_modify.svg)](https://pypi.org/project/preservica_mass_modify)

A Python CLI for making bulk updates to existing Preservica entities from a spreadsheet.

The tool supports XIP metadata updates (title, description, security), identifiers, retention policy updates, moves, optional delete mode, XML metadata merge/update, descendant processing, and optional upload-mode workflows.

This tool relies on making various API calls which may be limited by your version of Preservica.

## Table of Contents

- [Quick Start](#quick-start)
- [Version & Package Info](#version--package-info)
- [Why Use This Tool?](#why-use-this-tool)
- [Key Features](#key-features)
- [Authentication](#authentication)
	- [Credentials File](#credentials-file)
	- [Username + Keyring](#username--keyring)
- [Input Spreadsheet](#input-spreadsheet)
	- [Required Columns](#required-columns)
	- [Supported Metadata Columns](#supported-metadata-columns)
	- [Identifier Columns](#identifier-columns)
	- [Retention / Move / Delete Columns](#retention--move--delete-columns)
- [XML Metadata](#xml-metadata)
	- [Print/Convert Local XML Templates](#printconvert-local-xml-templates)
	- [Print/Convert Remote XML Templates](#printconvert-remote-xml-templates)
	- [Exact vs Flat Matching](#exact-vs-flat-matching)
- [Descendants Mode](#descendants-mode)
- [Continue/Resume Behaviour](#continueresume-behaviour)
- [Options File](#options-file)
- [CLI Reference](#cli-reference)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [Developers](#developers)
- [Contributing](#contributing)

## Quick Start

### 1) Install

```bash
pip install -U preservica_mass_modify
```

### 2) Run with username/server (password prompt)

```bash
preservica_modify \
	-i /path/to/updates.xlsx \
	-u your.username@example.com \
	-s yourtenant.preservica.com
```

### 3) Run with credentials file

```bash
preservica_modify \
	-i /path/to/updates.xlsx \
	--use-credentials /path/to/credentials.properties
```

## Version & Package Info

**Python Version**

- Python 3.10+ recommended

**Core dependencies**

- `pypreservica`
- `pandas`
- `openpyxl`
- `lxml`
- `keyring`

## Why Use This Tool?

This tool is designed for operational bulk-change workflows where many entities must be updated consistently and safely from tabular input.

Typical use cases:

- Apply metadata changes to many entities at once
- Add/update identifiers in a controlled way
- Update retention assignments in bulk
- Add or merge XML descriptive metadata templates
- Apply updates to descendants of selected folders

## Key Features

- Spreadsheet-driven updates for Preservica folders/assets
- Drop-in/drop-out column model (only columns present are acted on)
- Optional blank override mode (`--blank-override`) for intentional clears
- XML metadata support (`exact` or `flat` matching)
- Print/convert local or remote XML templates for spreadsheet preparation
- Descendant processing with fine-grained include flags
- Optional keyring-based password retrieval/storage
- Continue token support to resume after interruption
- Dummy mode for dry-run style validation of flow

## Authentication

You can authenticate using either:

- `--use-credentials` (recommended for scheduled/automated jobs)
- `-u/--username` with `-s/--server` (interactive)

### Credentials File

If no path is supplied, the CLI looks for `credentials.properties` in the current working directory.

Example:

```properties
username=your.username@example.com
password=your-password
server=yourtenant.preservica.com
tenant=optional-tenant
```

### Username + Keyring

```bash
preservica_modify \
	-i /path/to/updates.xlsx \
	-u your.username@example.com \
	-s yourtenant.preservica.com \
	--use-keyring \
	--save-password
```

- `--use-keyring`: retrieves stored password first
- `--save-password`: stores entered password for future runs
- `--keyring-service`: defaults to `preservica_modify`

## Input Spreadsheet

Current CLI validation accepts:

- `.xlsx`
- `.csv`
- `.json`
- `.xml`

Note: internal dataframe loaders support additional formats, but CLI-level validation currently enforces the two formats above.

### Required Columns

At minimum, include:

- `Entity Ref`

For explicit entity lookup mode, also include:

- `Document type` (`SO` for folder, `IO` for asset)

If `Document type` is omitted, the tool attempts lazy entity resolution (asset first, then folder).

### Supported Metadata Columns

- `Title`
- `Description`
- `Security`

Only present columns are used.

### Identifier Columns

Supported patterns:

- `Identifier` (defaults key to `code`)
- `Identifier:<key>` (custom identifier key)
- `Archive_Reference` (defaults key to configured identifier default `code`)
- `Accession_Reference` (defaults key to configured accession key `accref`)

### Retention / Move / Delete Columns

- `Retention Policy`
- `Move to` (UUID format expected)
- `Delete` (requires delete mode and credentials file)

## XML Metadata

XML templates can be read from local metadata directory or from your Preservica system.

### Print/Convert Local XML Templates

```bash
preservica_modify -i /path/to/input.xlsx --print-xmls
preservica_modify -i /path/to/input.xlsx --convert-xmls xlsx
```

### Print/Convert Remote XML Templates

```bash
preservica_modify -i /path/to/input.xlsx -u user -s server --print-remote-xmls
preservica_modify -i /path/to/input.xlsx -u user -s server --convert-remote-xmls csv
```

### Exact vs Flat Matching

Enable metadata mode with `-m` / `--metadata`:

```bash
# Defaults to exact if -m supplied without value
preservica_modify -i /path/to/input.xlsx -u user -s server -m

preservica_modify -i /path/to/input.xlsx -u user -s server -m exact
preservica_modify -i /path/to/input.xlsx -u user -s server -m flat
```

- `exact`: path-based matching for more deterministic updates
- `flat`: local-name style matching for simpler spreadsheets

## Descendants Mode

Apply updates to descendants using `-d/--descendants` with one or more options:

- `include-assets`
- `include-folders`
- `include-title`
- `include-description`
- `include-security`
- `include-retention`
- `include-xml`
- `include-identifiers`

Example:

```bash
preservica_modify \
	-i /path/to/input.xlsx \
	-u user -s server \
	-d include-assets include-xml include-identifiers
```

## Continue/Resume Behaviour

The tool stores progress in a continue token file alongside your input file:

- `<input_file>_continue.txt`

On interruption (`Ctrl+C`), progress can be resumed on the next run.

Resume handling is enabled by default in the current CLI workflow.

## Options File

Column names and certain defaults can be changed via options properties file.

Default path:

- `preservica_modify/options/options.properties`

Override with:

```bash
preservica_modify -i /path/to/input.xlsx -u user -s server -opt /path/to/options.properties
```

Current defaults include:

- `ENTITY_REF=Entity Ref`
- `DOCUMENT_TYPE=Document type`
- `TITLE_FIELD=Title`
- `DESCRIPTION_FIELD=Description`
- `SECURITY_FIELD=Security`
- `RETENTION_FIELD=Retention Policy`
- `MOVETO_FIELD=Move to`
- `DELETE_FIELD=Delete`
- `IDENTIFIER_FIELD=Identifier`
- `IDENTIFIER_DEFAULT=code`
- `FILE_PATH=FullName`

## CLI Reference

### Core

- `-i, --input` (required)
- `--dummy`
- `--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`
- `--log-file [PATH]`
- `-opt, --options-file PATH`

### Modification options

- `-del, --delete`
- `-up, --upload-mode`
- `-clr, --blank-override`
- `-d, --descendants ...`

### XML metadata options

- `-mdir, --metadata_dir PATH`
- `-m, --metadata [flat|exact]`
- `--print-xmls`
- `--print-remote-xmls`
- `--convert-xmls [xlsx|csv|json|ods]`
- `--convert-remote-xmls [xlsx|csv|json|ods]`

### Authentication options

- `--use-credentials [PATH]`
- `-u, --username USERNAME`
- `-s, --server SERVER`
- `--tenant TENANT`
- `--use-keyring`
- `--save-password`
- `--keyring-service NAME`
- `--test-login`

## Examples

### Test credentials only

```bash
preservica_modify -i /path/to/input.xlsx -u user -s server --test-login
```

### Update title/description/security from spreadsheet

```bash
preservica_modify -i /path/to/input.xlsx -u user -s server
```

### Apply XML updates from local metadata templates

```bash
preservica_modify \
	-i /path/to/input.xlsx \
	-u user -s server \
	-mdir /path/to/metadata \
	-m exact
```

### Clear values using blanks intentionally

```bash
preservica_modify -i /path/to/input.xlsx -u user -s server --blank-override
```

### Delete mode (credentials required)

```bash
preservica_modify \
	-i /path/to/input.xlsx \
	--use-credentials /path/to/credentials.properties \
	--delete
```

## Troubleshooting

- **Invalid input file**: ensure `--input` points to an existing `.xlsx` or `.csv` when running CLI validation.
- **Login failures**: verify server format, username/tenant, or credentials file values.
- **Delete mode blocked**: delete requires `--use-credentials`.
- **No updates happening**: confirm column headers match configured names exactly.
- **XML not applied**: check metadata mode (`flat` vs `exact`) and template/header alignment.
- **Move failures**: ensure `Move to` values are valid UUIDs.
- **Resume confusion**: remove stale `<input>_continue.txt` to force full restart.

## Developers

### Local install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Run tests

```bash
pytest
```

## Contributing

Issues and pull requests are welcome.

- Homepage: https://github.com/CPJPRINCE/presvica_mass_modify
- Issues: https://github.com/CPJPRINCE/presvica_mass_modify/issues

Please include:

- CLI command used
- input sample (sanitised)
- expected vs actual behaviour
- traceback/log excerpts