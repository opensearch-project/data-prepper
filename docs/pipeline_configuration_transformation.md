## Pipeline Configuration Transformation
Supports transformation of pipeline configuration from user provided configuration to
a transformed configuration based on template and rules.

## Usage

User provided configuration passes through rules, if the rules are valid,
the template for the transformations are dynamically chosen and applied.

**User config**
```aidl
simple-pipeline:
  source:
    someSource:
      hostname: "database.example.com"
      port: "27017"
  sink:   
    - opensearch:
       hosts: ["https://search-service.example.com"]
       index: "my_index"

```

**Template**
```aidl
"<<pipeline-name>>-transformed":
  source: "<<$.*.someSource>>"
  sink:
    - opensearch:
       hosts: "<<$.*.sink[?(@.opensearch)].opensearch.hosts>>"
       port: "<<$.*.someSource.documentdb.port>>"
       index: "<<$.*.sink[0].opensearch.index>>"
       aws:
        sts_role_arn: "arn123"
        region: "us-test-1"
       dlq:
          s3:
              bucket: "test-bucket"
```

**Rule**
```
apply_when:
  - "$..source.someSource"
  ```

**Expected Transformed Config**
```aidl
simple-pipeline-transformed:
  source:
    someSource:
      hostname: "database.example.com"
      port: "27017"
  sink:
    - opensearch:
        hosts: ["https://search-service.example.com"]
        port: "27017"
        index: "my_index"
        aws:
          sts_role_arn: "arn123"
          region: "us-test-1"
        dlq:
          s3:
            bucket: "test-bucket"
```

### Assumptions
1. In the template definition, Deep scan or recursive expressions like`$..` is NOT supported. Always use a more specific expression.
In the event specific variables in a path are not known, use wildcards.
2. User could provide multiple pipelines in their user config but 
there can be only one pipeline that can support transformation.
3. There cannot be multiple transformations in a single pipeline.
4. `<<$ .. >>` is the placeholder in the template.
`<< pipeline-name >>` is handled differently as compared to other placeholders
as other placeholders are jsonPaths.

### Developer Guide
When defining a placeholder for routes. Always define it as route.
```aidl
routes: "<<$.<<pipeline-name>>.routes>>" # routes or route (defined as alias) will be transformed to routes in json as routes will be primarily picked in pipelineModel.
```