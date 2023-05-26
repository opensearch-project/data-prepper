# User Agent Processor
This processor parses User-Agent (UA) string in an event and add the parsing result to the event.

## Basic Example
An example configuration for the process is as follows:
```yaml
...
  processor:
    - user_agent:
        source: "ua"
        target: "user_agent"
...
```

Assume the event contains the following user agent string:
```json
{
  "ua":  "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1"
}
```

The processor will parse the "ua" field and add the result to the specified target in the following format compatible with Elastic Common Schema (ECS):
```
{
  "user_agent": {
    "original": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
    "os": {
        "version": "13.5.1",
        "full": "iOS 13.5.1",
        "name": "iOS"
    },
    "name": "Mobile Safari",
    "version": "13.1.1",
    "device": {
        "name": "iPhone"
    }
  },
  "ua":  "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1"
}
```

## Configuration
* `source` (Required) — The key to the user agent string in the Event that will be parsed.
* `target` (Optional) — The key to put the parsing result in the Event. Defaults to `user_agent`.
* `exclude_original` (Optional) — Whether to exclude original user agent string from the parsing result. Defaults to false.
* `cache_size` (Optional) - Cache size to use in the parser. Should be a positive integer. Defaults to 1000.
* `tags_on_parse_failure` (Optional) - Tags to add to an event if the processor fails to parse the user agent string.
