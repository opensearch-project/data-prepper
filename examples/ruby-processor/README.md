## For all examples
Build docker image:

```./gradlew clean :release:docker:docker -Prelease```

Docker compose

```docker-compose up```

## Flow log duration example
```
docker run --name data-prepper -v ${PWD}/examples/ruby-processor/flow-log-duration/pipeline.yaml:/usr/share/data-prepper/pipelines/pipeline.yaml -v ${PWD}/examples/config/example-data-prepper-config.yaml:/usr/share/data-prepper/config/data-prepper-config.yaml --network "data-prepper_opensearch-net" opensearch-data-prepper:2.4.0-SNAPSHOT
```

## Event sampling example

```
docker run --name data-prepper -v ${PWD}/examples/ruby-processor/event-sampling/pipeline.yaml:/usr/share/data-prepper/pipelines/pipeline.yaml -v ${PWD}/examples/config/example-data-prepper-config.yaml:/usr/share/data-prepper/config/data-prepper-config.yaml -v ${PWD}/examples/ruby-processor/event-sampling/ruby_code.rb:/usr/share/data-prepper/ruby_code.rb --network "data-prepper_opensearch-net" opensearch-data-prepper:2.4.0-SNAPSHOT
```

## Localization example

```
docker run --name data-prepper  \
-v ${PWD}/examples/ruby-processor/localization/pipeline.yaml:/usr/share/data-prepper/pipelines/pipeline.yaml  \
-v ${PWD}/examples/config/example-data-prepper-config.yaml:/usr/share/data-prepper/config/data-prepper-config.yaml  \
-v ${PWD}/examples/ruby-processor/localization/translate_events.rb:/usr/share/data-prepper/translate_events.rb  \
-v ${PWD}/examples/ruby-processor/localization/es_en.yaml:/usr/share/data-prepper/es_en.yaml \
-v ${PWD}/examples/ruby-processor/localization/ingest.txt:/usr/share/data-prepper/ingest.txt \
     --network "data-prepper_opensearch-net" opensearch-data-prepper:2.4.0-SNAPSHOT
```

## Logstash filter example
1. Remove all logstash-specific code

```
docker run --name data-prepper  \
-v ${PWD}/examples/ruby-processor/logstash-filter/pipeline.yaml:/usr/share/data-prepper/pipelines/pipeline.yaml  \
-v ${PWD}/examples/config/example-data-prepper-config.yaml:/usr/share/data-prepper/config/data-prepper-config.yaml  \
-v ${PWD}/examples/ruby-processor/logstash-filter/code:/usr/share/data-prepper/code  \
-v ${PWD}/examples/ruby-processor/logstash-filter/ingest.txt:/usr/share/data-prepper/ingest.txt \
     --network "data-prepper_opensearch-net" opensearch-data-prepper:2.4.0-SNAPSHOT
```

### Logstash XML filter example
Logstash filters can be run via the Ruby processor. However, the Logstash filter source code will need to be modified to work with the Ruby processor.

The primary modifications to source code are converting Logstash-specific implementation details to Ruby implementation details.
1. Change all config Symbols to built up Symbols (because the Data Prepper Ruby processor does not reimplement the Logstash config DSL)
2. Change the `@logger` variable to `LOG`, the Data Prepper logger. Or delete if you do not require logging
3. Change the class signature to not involve Logstash, remove any Logstash-specific imports, and change any Logstash-specific errors to generic `LOG.error` calls. 
4. Remove the `filter_matched` call that adds additional logstash logic from the config; this is not supported out of the box by the Ruby processor.
5. Change Logstash event API calls to Data Prepper event API calls
   6. In particular, change `set` to `put` and `tag({tag_to_add})` to `add_tags([{tag_to_add}])`. 
Once Logstash-specific implementation details have been removed, write the `init` and `process` methods within the file. Generally, `init` should create a new instance of the filter and `process(event)` should call `<logstash_filter_object>.filter(event)`

There may be additional considerations if a filter is tightly coupled to the Logstash ecosystem or contains gems that are not bundled in. For example, the XML filter relies on XML_Simple, and we have reimplemented its functionality instead of additionally bundling the XML_Simple gem.  
