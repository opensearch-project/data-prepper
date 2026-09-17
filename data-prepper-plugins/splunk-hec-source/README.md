# Splunk HEC Source

This source implements a [Splunk HTTP Event Collector (HEC)](https://docs.splunk.com/Documentation/Splunk/latest/Data/HECRESTendpoints) compatible HTTP server, so existing Splunk forwarders and any HEC-compatible client can send data to Data Prepper without modification. It extends the shared `BaseHttpSource`.

## Usage

```yaml
hec-pipeline:
  source:
    splunk_hec:
      port: 8088
      ssl: true
      ssl_certificate_file: "/path/to/server.crt"
      ssl_key_file: "/path/to/server.key"
      tokens:
        - token: "${{aws_secrets:hec-secrets:infra-token}}"
          defaults:
            index: "infrastructure"
            sourcetype: "syslog"
        - token: "${{aws_secrets:hec-secrets:app-token}}"
          defaults:
            index: "application"
  sink:
    - opensearch:
        hosts: ["https://opensearch:9200"]
        index: "${index}"
```

Provide credentials via secret references rather than inline plaintext, and enable `ssl` so the `Authorization: Splunk <token>` header is not sent in cleartext.

## Endpoints

All endpoints are served under `path` (default `/services/collector`):

| Method + Path | Purpose |
|---|---|
| `POST /event` | Ingest one or more HEC events as concatenated JSON objects. |
| `POST /raw` | Ingest plain text, one event per line; metadata from query parameters. |
| `POST /ack` | Poll indexer acknowledgement status (only when `acknowledgements: true`). |
| `GET /health` | Returns `{"text":"HEC is healthy","code":17}`. No authentication. |

## Authentication

Each request must carry `Authorization: Splunk <token>`. A missing header returns `401` code 2, a malformed header (wrong scheme or empty token) returns `401` code 3, a well-formed but unknown token returns `403` code 4, and a token configured with `enabled: false` returns `403` code 1. Tokens are compared in constant time.

## Event Schema

Each HEC event becomes a Data Prepper event, built by `HecEventBuilder` through the injected `EventFactory`. A string `event` becomes a `message` field; a JSON object `event` is flattened to top-level fields when `flatten_event` is true (default) or nested otherwise. `host`, `source`, `sourcetype` become fields, `index` becomes routing metadata, `fields` are merged, and `time` becomes `@timestamp` (falling back to the current time when absent or unparseable). The `/raw` endpoint resolves the same metadata from its query parameters and the per-token defaults, so both endpoints attach the same routing metadata.

## Configuration Options

Duration values accept ISO-8601 ("PT5M") or simple notation ("60s", "1500ms").

Top level:
* `port` (Optional): listen port. Default `8088`.
* `path` (Optional): base path. Default `/services/collector`.
* `tokens` (Required): non-empty list of accepted HEC tokens; each may set `enabled` and per-token `defaults` (`index`, `sourcetype`, `source`, `host`, `fields`).
* `flatten_event` (Optional): flatten JSON object events into top-level fields. Default `true`.
* `raw_line_breaker` (Optional): literal delimiter for the raw endpoint. Default newline.
* `default_sourcetype` (Optional): sourcetype when unspecified. Default `httpevent`. This is a free-form string because Splunk sourcetypes are defined by each Splunk deployment and are not drawn from a fixed set.
* `acknowledgements` (Optional): enable the HEC indexer ack protocol. Default `false`.
* `acknowledgement_expiry` (Optional): retention for ack state. Default `300s`.
* `warn_future_timestamps` (Optional): warn on timestamps more than one hour in the future. Default `false`.
* SSL/TLS options (`ssl`, `ssl_certificate_file`, `ssl_key_file`, and related) are inherited from the common HTTP server configuration. Unlike the other HTTP sources, `ssl` defaults to `true` for this source, so `ssl_certificate_file` and `ssl_key_file` are required unless you set `ssl: false`.
* `authentication` (Not supported): the Splunk HEC protocol authenticates with the `Authorization: Splunk <token>` header, which this source validates through the `tokens` option. Configuring the shared `authentication` plugin is rejected so that there is only one authentication mechanism in effect.

## Backpressure

When the buffer is full, the source returns `503` code 9 (Server is busy); Splunk forwarders back off and retry, so no data is lost.

## Metrics

Counters and summaries for requests received/success/failed, auth failures, events received/written, request size, events per request, request latency, buffer-full, and parse errors; with acknowledgements enabled, ack requests/confirmed/pending/expired.

## Not Yet Supported

Durable cross-restart deduplication, bearer-token / custom-header authentication, and health reporting based on buffer fill level are planned as follow-ups.
