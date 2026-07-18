# RSS Source

This source allows Data Prepper to poll one or more RSS/Atom feeds and convert
their items into Data Prepper Events. Each feed is polled on a schedule, items
are deduplicated within a run, and new items are written to the buffer.

## Basic Usage

Provide one or more feeds under `feeds`. Each feed requires a `url` and may
optionally set a `name`, a per-feed `polling_frequency`, and `authentication`.

### Example

```yaml
rss-pipeline:
  source:
    rss:
      workers: 2
      polling_frequency: PT5M
      feeds:
        opensearch-forum:
          url: https://forum.opensearch.org/latest.rss
        example-news:
          url: https://api.example.com/v2/rss?partnerKey=abc123
          polling_frequency: PT1M
        internal:
          url: https://private.example.com/feed.xml
          authentication:
            basic:
              username: "${{aws_secrets:rss-credentials:username}}"
              password: "${{aws_secrets:rss-credentials:password}}"
  sink:
    - opensearch:
        hosts: ["https://opensearch:9200"]
        index: rss-items
```

The `authentication` example above references a secret named `rss-credentials`
defined in `data-prepper-config.yaml` under the `aws` extension. The
`${{aws_secrets:<secret-name>:<key>}}` form resolves the given key from that
secret at startup. See the
[AWS secrets configuration](../../data-prepper-plugins/aws-plugin) for details.

## Event Schema

Each item is emitted as an event of type `rss-item` with these top-level body
fields:

| Field | Description |
|-------|-------------|
| `title` | Item title |
| `link` | Item link |
| `description` | Item description / summary |
| `pub_date` | Item publication date |
| `guid` | Item GUID, falling back to `link` when absent |
| `feed_name` | The feed's key from the `feeds` map (always present) |
| `feed_url` | The configured feed URL with its query string redacted (always present) |

Additional feed channel details are attached as **event metadata** (not stored
in the document body), and only when the feed provides them:

| Metadata attribute | Description |
|--------------------|-------------|
| `feed_title` | The feed channel's `<title>` |
| `feed_link` | The feed channel's `<link>` (the publisher's site) |
| `feed_language` | The feed channel's `<language>` |
| `feed_categories` | The feed channel's `<category>` values |

`feed_name` and `feed_url` are body fields, so they are searchable and can be
used directly for per-feed routing at the sink. `guid` is well suited to
`document_id` for sink-side upserts. For example:

```yaml
  sink:
    - opensearch:
        hosts: ["https://opensearch:9200"]
        index: "rss-${/feed_name}-%{yyyy.MM.dd}"
        document_id: "${/guid}"
```

The channel metadata attributes are attached only when the feed provides them,
so **sink configuration must not depend on an attribute that may be absent** —
routing and document IDs should key off the always-present `feed_name`,
`feed_url`, or `guid` body fields.

## Configuration Options

All Duration values support ISO-8601 notation ("PT15M", "PT20.345S") as well as
simple notation for seconds ("60s") and milliseconds ("1500ms").

Top level:

* `feeds` (Required): A non-empty map of feeds to poll. Each key is the feed
  name (attached to events as `feed_name` and useful for index routing); each
  value is a feed configuration.
* `polling_frequency` (Optional): Duration - default polling frequency for feeds
  that do not set their own. Defaults to 5 minutes.
* `workers` (Optional): Integer - size of the polling thread pool, bounded by the
  number of feeds. Defaults to 1.

Per feed (values in the `feeds` map):

* `url` (Required): The RSS/Atom feed URL to read from.
* `polling_frequency` (Optional): Duration - overrides the top-level default for
  this feed.
* `authentication.basic` (Optional): `username` and `password` for HTTP Basic
  Auth. Supply credentials via secrets references rather than inline plaintext.

## Failure Handling

Feeds are polled independently. A failing feed is logged (with its URL query
string redacted), increments a per-feed failure metric, and backs off
exponentially before retrying; it never stops the other feeds and never
permanently stops polling.

## Not Yet Supported

The following are planned as follow-up work and are not part of this version:

* Durable, cross-restart, cross-node deduplication via source coordination.
* End-to-end acknowledgments.
* Bearer-token / custom-header authentication.
