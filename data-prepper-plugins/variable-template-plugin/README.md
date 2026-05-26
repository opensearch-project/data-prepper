# variable-template-plugin

Resolves dynamic value expressions in Data Prepper pipeline configurations at startup. Has to be enabled via config otherwhise it gets interpreted as a string. File and store allows all standard formats like .env, .txt, etc.

## Configuration

```yaml
extensions:
  variable_sources:
    resolvers:
      env: true
      file: true
      store:
        enabled: true
        sources:
          - /path/to/.env
          - /path/to/another.env
```

## Resolvers

### env

Reads a value from the process environment.

```yaml
password: ${{env:OPENSEARCH_PASSWORD}}
```

The environment variable must be set when Data Prepper starts. If it is not set, startup fails with a descriptive error.

### file

Reads the contents of a file from disk. Leading and trailing whitespace (including newlines) is stripped, so files written with a trailing newline behave as expected. Both absolute and relative paths are supported.

```yaml
password: ${{file:/var/secrets/opensearch/password}}
```

### store

Reads key/value pairs from one or more `.env`-style files and resolves them by key.

```yaml
image: ${{store:OPENSEARCH_URL}}
```

`.env, .txt or other` file format:
```
# comments are ignored
KEY=value
QUOTED="also works"
SPECIAL=p@$$w0rd!
OPENSEARCH_URL=https://host.docker.internal:9200
```

Rules:
- Lines starting with `#` are comments and are ignored
- Empty lines are ignored
- Format is `KEY=VALUE`
- Values may be quoted with `"` or `'` — quotes are stripped
- Inline comments (`KEY=value # comment`) are stripped
- If multiple store files define the same key, the last file wins
- Missing `=`, empty key, or missing source file causes startup to fail with a descriptive error

## Behaviour

All values are resolved once at pipeline startup and are static for the lifetime of the process. Changes to environment variables or file contents require a restart to take effect.

## Coexistence with other plugins

This plugin uses the Data Prepper extension framework's `PluginConfigValueTranslator` interface. It coexists with the `aws-plugin` (`${{aws_secrets:...}}`) and any other translator plugins without conflict, provided the framework patch in `DataPrepperExtensionPoints` is applied

## Known Limitations

Because of how the variables get templated it wont work if you use the variables in the aws-extension config.
And you can only set variables as standalone. So you cant define them inside a string. This applies also to expression.
This will be supported in upcomming feature.

## Wont work:
```yaml
URL: ${{env:OPENSEARCH_HOST}}:9200
```

```yaml
processor:
    - add_entries:
        entries:
          - key: "Resolve worked?"
            value: "yes"
            add_when: "/event_key == ${{store:TEST_KEY}}"
```

## Full Setup:
```yaml
# data-prepper-config.yaml
ssl: false
extensions:
  variable_sources:
    resolvers:
      env: true
      file: true
      store:
        enabled: true
        sources:
          - /var/secrets/.env
          - /var/secrets/store.txt
  aws:
    secrets:
      aws_example_secret:
        secret_id: dataprepper_local
        region: us-east-1
  geoip_service:
    maxmind:
```

```yaml
# pipelines.yaml
simple-sample-pipeline:
  workers: 2
  delay: "5000"
  source:
    random:
  processor:
    - add_entries:
        entries:
          - key: "env-variable"
            value: "${{env:DEMO_ENVIRONMENT_KEY}}"
          - key: "file-variable"
            value: ${{file:/var/secrets/secret-file.txt}}
          - key: "store_first_variable"
            value: ${{store:STORE_FIRST_ENTRY}}
          - key: "store_second_variable"
            value: ${{store:STORE_SECOND_ENTRY}}
          - key: "aws_secret_variable"
            value: "${{aws_secrets:aws_example_secret}}"
  sink:
    - stdout:
    - file:
        path: ${{store:OUTPUT_FILE}}
    - opensearch:
        hosts: ["${{store:OPENSEARCH_HOST}}"]
        index: ${{store:INDEX_NAME}}
        username: "admin"
        password: "${{env:OPENSEARCH_PASSWORD}}"
        insecure: true
```

```json
# stdout sink
{"message":"058b3a98-fc63-428c-b43b-e9c85ce469c7","env-variable":"VALUE_FROM_ENVIRONMENT","file-variable":"value-from-secret-file","store_first_variable":"FIRST_STORE_VALUE","store_second_variable":"SECOND_STORE_VALUE","aws_secret_variable":"{\"aws_demo_key\":\"secret_from_aws\"}"}
```

```json
# document in opensearch
{
  "_index" : "index-name-from-store",
  "_id" : "C-V-7JwB-wKI_oflC0Kg",
  "_score" : 1.0,
  "_source" : {
    "message" : "058b3a98-fc63-428c-b43b-e9c85ce469c7",
    "env-variable" : "VALUE_FROM_ENVIRONMENT",
    "file-variable" : "value-from-secret-file",
    "store_first_variable" : "FIRST_STORE_VALUE",
    "store_second_variable" : "SECOND_STORE_VALUE",
    "aws_secret_variable" : "{\"aws_demo_key\":\"secret_from_aws\"}"
}
```