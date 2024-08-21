A dummy plugin is used specifically when we want to use pipeline transformation for a 
plugin that does not exist.

We can define a rule and template for this plugin by creating a plugin folder and
place rule for which the pipeline configuration would be valid and a corresponding
template to transform to when the rule is valid.

For further details on transformation refer:
/docs/pipeline_configuration_transformation.md


For Example:

User Config:
```yaml
test-pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - dummy_plugin:
  sink:
    - stdout:
```

Here dummy_plugin is not really a plugin that is defined in dataprepper, but we can 
use the pipeline transformation to convert the user-config to different config based 
on the template.

Rule:
```yaml
plugin_name: "dummy-plugin"
apply_when:
  - "$.test-pipeline.processor[?(@.dummy_plugin)]"
```

Template:
```yaml
test-pipeline:
  source:
    file:
      path: "/tmp/input-file.log"
  processor:
    - string_converter:
        upper_case: true
  sink:
    - noop:
```

Output:
```yaml
test-pipeline:
  source:
    file:
      path: "/tmp/input-file.log"
  processor:
    - string_converter:
        upper_case: true
  sink:
    - noop:
```