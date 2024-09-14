# Data Prepper Plugin Schema CLI

This module includes the SDK and CLI for generating schemas for Data Prepper pipeline plugins.

## CLI Usage

```
./gradlew :data-prepper-plugin-schema-cli:run --args='--plugin_type=processor --plugin_names=grok --output_folder=/path/to/schemas'
```

* plugin_type: A required parameter specifies type of processor. Valid options are `source`, `buffer`, `processor`, `route`, `sink`.
* plugin_names: An optional parameter filters the result by plugin names separated by `,`, e.g. `grok,date`.
* output_folder: An optional parameter to specify the output folder path.
