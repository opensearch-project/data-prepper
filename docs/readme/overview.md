# Simple Ingest Transformation Utility Pipeline

Simple Ingest Transformation Utility Pipeline (SITUP) is an open source utility service. SITUP is a server side data collector with abilities to filter, enrich, transform, normalize and aggregate data for downstream analytics and visualization.
The broader vision for SITUP is to enable an end-to-end data analysis life cycle from gathering raw logs to facilitating sophisticated and actionable interactive ad-hoc analyses on the data. 

## Concepts

![Situp Pipeline](images/SitupPipeline.png)

Below are the fundamental concepts of SITUP,

### Pipeline
A SITUP pipeline has four key components a *source*, a *buffer*, one or more *processors*, and one or more *sinks*. A single instance of SITUP can have one or more pipelines. A pipeline definition contains two required components *source* and *sink*. If other components are missing the SITUP pipeline will use default buffer and no-op processor. All the components are pluggable and enable customers to plugin their custom implementations. Please note that custom implementations will have implications on guarantees however the basic interfaces will remain same.

### Source
Source is the input component of a pipeline, it defines the mechanism through which a SITUP pipeline will consume records. A pipeline can have only one source. Source component could consume records either by receiving over http/s or reading from external endpoints like Kafka, SQS, Cloudwatch etc.  Source will have its own configuration options based on the type like the format of the records (string/json/cloudwatch logs/open telemetry trace) , security, concurrency threads etc . The source component will consume records and write them to the buffer component. 

### Buffer
The buffer component will act as the layer between the *source* and *sink.* The buffer could either be in-memory or disk based. The default buffer will be in-memory queue bounded by the number of records called `bounded_blocking`. If the buffer component is not explicitly mentioned in the pipeline configuration, the default `bounded_blocking` will be used.

### Sink
Sink in the output component of pipeline, it defines the one or more destinations to which a SITUP pipeline will publish the records. A sink destination could be either services like elastic search, s3 or another SITUP pipeline. By using another SITUP pipeline as sink, we could chain multiple SITUP pipelines. Sink will have its own configuration options based on the destination type like security, request batching etc. 

### Processor
Processor component of the pipeline, these are intermediary processing units using which users can filter, transform and enrich the records into desired format before publishing to the sink. The processor is an optional component of the pipeline, if not defined the records will be published in the format as defined in the source. You can have more than one processor, and they are executed in the order they are defined in the pipeline spec.

### Sample Pipeline configuration

#### Minimal components
```
sample-pipeline:
  source:
    file:
        path: path/to/input-file
  sink:
    - file:
        path: path/to/output-file
```

This configuration reads from file source and writes to a file source. It uses default options for other options for other components.

#### All components

```
sample-pipeline:
  workers: 4 #Number of workers
  delay: 100 # in milliseconds, how often the workers should run
  source:
    file:
        path: path/to/input-file
 buffer:
    bounded_blocking:
      buffer_size: 1024 # max number of records the buffer will accept
      batch_size: 256 # max number of records the buffer will drain for each read
 processors:
    - string_converter:
       upper_case: true
 sink:
   - file:
       path: path/to/output-file
```

The above pipeline has a file source that reads string records from the `input-file`. The source pushes the data to buffer bounded by max size of `1024`. The pipeline configured to have `4` workers each of them reading maximum of `256` records from the buffer for every `100 milliseconds`. Each worker will execute the `string_converter` processor and write the output of the processor to the `output-file`




## Pipeline Connectors

Some use-cases requires to use one or more heterogeneous sinks/processors. This means the pipeline architecture is now dependent on heterogeneous components which are diverse and could impact the availability and performance of one another. To avoid this, SITUP offers Pipeline Connectors. Pipeline Connectors help to process data from single source in multiple pipelines which are made up of heterogeneous components. Pipeline connectors are simple components which act as sink and source.

### Sample configuration 

```
input-pipeline:
  source:
    file:
      path: path/to/input-file
  sink:
    - pipeline:
       name: "output-pipeline-1"
    - pipeline:
       name: "output-pipeline-2"
output-pipeline-1:
  source:
    pipeline:
      name: "input-pipeline"
  processors:
    - string_converter:
       upper_case: true
  sink:
    - file:
        path: path/to/output-1-file
output-pipeline-2:
  source:
    pipeline:
      name: "test-pipeline-1"
  processors:
    - string_converter:
       upper_case: false
  sink:
    - file:
        path: path/to/output-2-file
```

The above configuration uses the Pipeline Connectors. `input-pipeline` is configured with `output-pipeline-1` and `output-pipeline-2` as sink. With the help of pipeline connectors we can read onec from the input file and write upper case values to `output-1-file` and lower case values to `output-2-file`.
