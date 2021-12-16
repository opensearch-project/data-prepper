# Trace Analytics Schema Versioning

The purpose of this document is to outline the structure and versioning formats followed by Data Prepper (DP) the Trace Analytics (TA) plugin for OpenSearch Dashboards. Each individual schema shall have its own supporting document explaining its structure, fields, and purpose.

## Tenets

1. Schemas shall follow the [Semantic Versioning 2.0.0 spec](https://semver.org/), excluding patch version numbers.
2. Schema versions shall be detached from Data Prepper and Trace Analytics plugin versions.
3. Index and index template names shall only include the major version (e.g. "otel-v1-apm-span"). The minor version shall be included within the actual schema as a field.
4. Forward and backward-compatibility promises shall only apply to schemas of the same major version.
5. A major version increase shall require Data Prepper (writer) artifacts to be made available before Trace Analytics plugin (reader) updates are made available.
6. A major version increase shall result in a new index template and indexes being created in an OpenSearch cluster.

## Versioning Format

A schema will be versioned following the [Semantic Versioning 2.0.0 spec](https://semver.org/). A schema version will include a major and minor version number. Patch version numbers will not be used as "patching" is not applicable to a versioned schema; all changes no matter how trivial will have implications.

1. **Major versions** will be incremented for breaking, backwards-incompatible changes.
2. **Minor versions** will be incremented for backwards-compatible feature additions. This can be thought of an "append-only" change to the schema.

**Schema versions are detached from Data Prepper and Trace Analytics plugin versions.** A TA plugin version increase does not necessarily affect the version of the schema or Data Prepper. Instead, both DP and TA versions will be *compatible* with a specific schema version. Examples include:

* Trace Analytics plugin v1.5 includes features built on schema version 1.2.0
* Data Prepper v1.1 emits documents following schema version 1.2.0

### Major version changes

Major version changes include removing a field, renaming a field, or changing an existing field's datatype.

* Schema 1.0 to 2.0 *changes the type of a field* from Keyword to Numeric
* Schema 1.0 to 2.0 *removes* *field* "latency" from the schema
* Schema 1.0 to 2.0 *renames field* "end" to "endTime"
    * A rename is effectively a field addition and removal in a single operation

### Minor version changes

Minor version changes include adding a new field or adding a new nested field.

* Schema 1.2 to 1.3 adds a new field, "fieldC", as a Keyword
* Schema 2.11 to 2.12 adds a new nested field, "name" to an existing collection, "parentSpan", resulting in "parentSpan.name"

## Compatibility Promises

The following compatibility promises are made *only for schemas of the same major version*.

* ***Backwards compatibility*** - Trace Analytics UI features built on version 1.x of the schema **will not break, but may degrade** if data from a **prior** 1.x schema version is used.
* ***Forwards compatibility*** - Trace Analytics UI features built on version 1.x of the schema **will not break** if data from a **later** 1.x schema version is used.

### Read-compatibility examples

1. A plugin built on schema 1.1 but consuming schema 1.2 data will function 100% without issue
2. A plugin built on schema 1.2 but consuming schema 1.1 data will continue to function, however some features might be degraded
3. A plugin built on schema 1.0 but consuming schema 2.0 data is not guaranteed to function
4. A plugin built on schema 2.0 but consuming schema 1.0 data is not guaranteed to function

### Write-compatibility examples

1. A writer built on schema 1.2 but writing to a cluster containing schema 1.1 data will succeed
2. A writer built on schema 1.1 but writing to a cluster containing schema 1.2 data will succeed
3. A writer built on schema 1.0 but writing to a cluster containing schema 2.0 data will succeed
4. A writer built on schema 2.0 but writing to a cluster containing schema 1.0 data will succeed

### Handling minor version updates

Minor version updates will occur as new fields are needed to support new Trace Analytics features. The steps to update a schema minor version are to:

1. Ensure both TA and DP owners are aligned with requirements
2. Update the schema in the schema repo
    1. Add new fields to the schema JSON
    2. Increment the `version` field of the schema by 1
    3. Update the documentation to describe the new fields
3. Add test data to the TA plugin test suite
    1. Don't update existing data in place, instead add new data following the new schema version. The test suite is expected to pass with a range of minor versions being tested at the same time.
4. Update DP to start emitting documents following the new schema version
5. Add new features to the TA plugin utilizing the new data

### Handling major version updates

As schema owners, we will do our best to avoid introducing major version changes. However, as our schemas are heavily tied to the OpenTelemetry spec, there is always the risk of an upstream backwards-incompatible change requiring a major version increase.

The following procedures may be considered while performing a major upgrade across both Data Prepper and the Trace Analytics plugin.

#### Approach #1: Upgrade both Data Prepper and the Trace Analytics plugin as simultaneously as possible
The simplest approach to perform a major version upgrade is to simultaneously upgrade both Data Prepper and the Trace Analytics plugin. This approach will result in a usability drop while both components are on differing major versions.

As an example, assume both DP and TA are being upgraded from v1 to v2:
* If DP is upgraded to v2 before TA, then the old TA v1 dashboard will not visualize the new data ingested by DP v2.
* IF TA is upgraded to v2 before DP, then the new TA v2 dashboard will not visualize the old data ingested by DP v1.

Both component upgrades should happen as close as possible to each other to minimize this window.

#### Approach #2: Run both old and new Data Prepper versions side-by-side until the Trace Analytics plugin is upgraded
To avoid usability downtime of the Trace Analytics plugin, run two versions of Data Prepper simultaneously before upgrading the plugin.

* DP v1 writes to the v1 index
* DP v2 writes to the v2 index
* TA v1 reads from the v1 index
* TA v2 reads from the v2 index

Upgrading the Trace Analytics plugin from v1 to v2 will result in no usability downtime, as both plugin versions will have populated indexes to read from. This approach is more complex in that it requires additional Data Prepper instances and results in duplicate data being written (until the old DP instances are shut down).

#### Mitigating data loss
Upgrading Data Prepper and the Trace Analytics plugin to a new major version schema will result in data written to old indexes being unusable. If users wish to avoid this data loss, the [reindexing APIs](https://opensearch.org/docs/latest/opensearch/reindex-data/) must be used. Additionally, transforms might required depending on the differences between the two major schema versions.
