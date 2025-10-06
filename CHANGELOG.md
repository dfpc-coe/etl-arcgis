# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### v7.15.0

- :rocket: Add default Query Strategy

### v7.14.0

- :rocket: Add the ability to use the queryTopFeatures endpoint

### v7.13.1

- :arrow_up: Update all core deps

### v7.13.0

- :arrow_up: Update base image to node@22

### v7.12.0

- :tada: Strongly type ephemeral store

### v7.11.1

- :bug: Fix issue where `remarks` field was not updated

### v7.11.0

- :tada: Migrate to latest ETL Base

### v7.10.0

- :tada: Add continuous insert mode

### v7.9.0

- :rocket: Migrate to shared auth strategy

### v7.8.2

- :bug: Support Env Changes

### v7.8.1

- :bug: Downstream users should filter Null Island if they so desire

### v7.8.0

- :rocket: Post features to ESRI Layer

### v7.7.0

- :rocket: Post features to ESRI Layer

### v7.6.0

- :rocket: Continue to sketch outgoing config

### v7.5.0

- :rocket: Continue to sketch outgoing config

### v7.4.0

- :rocket: Update Core Deps

### v7.3.2

- :arrow_up: Update Core Deps

### v7.3.1

- :bug: Fix Syntax

### v7.3.0

- :rocket: Continue to sketch out outgoing behavior

### v7.2.1

- :bug: Fix build Error

### v7.2.0

- :rocket: Provide empty incoming-output schema if incoming is not yet configured

### v7.1.0

- :rocket: Prelim sketch of outgoing data

### v7.0.0

- :tada: Update to `CloudTAK@v6`

### v6.3.1

- :bug: Remove webhook type

### v6.3.0

- :tada: Add Capabilities endpoint

### v6.2.1

- :rocket: Add ability to specify arbitrary query params (Array)

### v6.2.0

- :rocket: Add ability to specify arbitrary query params

### v6.1.0

- :rocket: Improved auth updates

### v6.0.0

- :tada: Update to use ephemeral store

### v5.8.0

- :rocket: Ignore Features from upstream that do not have geometry

### v5.7.0

- :rocket: If not configured, return empty schema

### v5.6.0

- :arrow_up: Update ETL Base for better error responses

### v5.5.0

- :arrow_up: Use new PATCH path for Layer

### v5.4.0

- :arrow_up: Update to latest ETL to push empty features to the server

### v5.3.0

- :arrow_up: Fix date parsing issue in ESRI-Dump library

### v5.2.1

- :rocket: Remove unused imports

### v5.2.0

- :arrow_up: Use latest ETL Library
- :rocket: Drop ARCGIS_TIMEZONE in favour of generic config timezone

### v5.1.3

- :bug: Fix Lints

### v5.1.2

- :arrow_up: Use latest version of base

### v5.1.1

- :bug: Use new instance method for schema

### v5.1.0

- :rocket: Update to use new default fns

### v5.0.1

- :arrow_up: Move ESLint to devDeps

### v5.0.0

- :arrow_up: Update ETL
- :rocket: Use `.properties.metadata`

### v4.1.0

- :arrow_up: Update ETL

### v4.0.2

- :arrow_up: Update Core Deps

### v4.0.1

- :arrow_up: Update Core Deps

### v4.0.0

- :rocket: Update to latest token strategy

### v3.9.2

- :rocket: Update GH Actions

### v3.9.1

- :arrow_up: Bump ESRI-Dump Version

### v3.9.0

- :tada: Add support for local TimeZone mapping

### v3.8.0

- :rocket: ESRI-Dump will now output Dates as Strings

### v3.7.0

- :bug: Use Referer header where possible when using a token

### v3.6.2

- :tada: Attempt server auth if no portal is given but username/password are

### v3.6.1

- :bug: Look for Expires

### v3.6.0

- :rocket: Update to generic ESRI Endpoint

### v3.5.0

- :bug: Support count: 0 in Esri Query

### v3.4.0

- :tada: Add support for schema response with Token/Portal
- :rocket: Unified ESRIDump config
- :bug: Fix `ARCGIS_QUERY` Env

### v3.3.0

- :rocket: Add support for `ARCGIS_QUERY` Environment

### v3.2.0

- :rocket: UnMulti Multi Geoms

### v3.1.0

- :rocket: Add support for ARCGIS/ESRI Portals with Username/Password => Token

### v3.0.0

- :rocket: Switch to built in environment display

### v2.1.1

- :rocket: Log number of features obtained

### v2.1.0

- :bug: Set `feature.id`

### v2.0.2

- :bug: await schema call

### v2.0.1

- :bug: Remove Debug Code

### v2.0.0

- :tada: Update to ETL@2 and support output schema

### v1.4.0

- :rocket: Migrate to TypeScript
- :tada: Fully support Headers & Params

### v1.3.0

- :tada: Pass HEADERS & PARAMS to ESRIDump

### v1.2.0

- :rocket: Write to CoT endpoint

### v1.1.1

- :rocket: Move to new environment location

### v1.1.0

- :rocket: Add ESRI-Dump@2

### v1.0.0

- :tada: Initial Commit
