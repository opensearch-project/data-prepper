# Error Handling

In Data Prepper Pipeline, errors should be handled by implementation of plugin components and should not throw any uncaught runtime exceptions. If thrown the pipeline will stop immediately or halt eventually based on the component that throws the exception.

Below are the different scenario of error,

## Single Pipeline
Single pipeline is a pipeline that doesn't use the pipeline connectors.

* If an exception encountered when creating a pipeline, the pipeline stops and exits using `system.exit(1)`. 
* If an exception encountered during pipeline `start()` method, the pipeline stops with appropriate logs. 
* If an exception encountered in Pipeline `processors` or `sinks` during runtime, the pipeline stops with appropriate logs.

## Connected Pipelines

Connected Pipelines is scenario where two or more pipelines connected using the Pipeline Connector.


* If an exception encountered when creating any of the connected pipelines, all the connected pipelines stop and exits using `system.exit(1)`. 
* If an exception encountered during pipeline `start()` method in any of the connected pipelines, the pipeline that encountered the exception will shutdown and other pipelines will run but not receive or process any data as a connected pipeline is unavailable.
* If an exception encountered in Pipeline `processors` or `sinks` during runtime in any of the connected pipelines, the pipeline that encountered the exception will shutdown and other pipelines will run for a while and shutdown as a connected pipeline is unavailable.

