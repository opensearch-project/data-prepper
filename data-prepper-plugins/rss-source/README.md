# RSS Source

This source allows Data Prepper to read a single RSS feed URL and parse RSS Items into Data Prepper Events.

## Basic Usage

This source requires an RSS/Atom feed URL as a required configuration option. The RSS Source will then extract the RSS items from the URL and convert the items into Data Prepper Events.

### Example

The following configuration shows a basic pipeline configuration of RSS source which reads a feed URL using the default `polling_frequency`

```
source:
  rss:
    url: "https://forum.opensearch.org/latest.rss"
```

## Configuration Options

All Duration values are a string that represents a duration. They support ISO_8601 notation string ("PT20.345S", "PT15M", etc.) as well as simple notation Strings for seconds ("60s") and milliseconds ("1500ms").

* `url` (Required) : The RSS feed URL to read from.
* `polling_frequency` (Optional) : Duration - The frequency to retrieve the RSS feed data. Defaults to 5 minutes.
