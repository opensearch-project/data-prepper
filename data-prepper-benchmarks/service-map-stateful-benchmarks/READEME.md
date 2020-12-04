# Service Map Stateful Benchmarks

This package contains benchmarks for the service map stateful processor using JMH: https://openjdk.java.net/projects/code-tools/jmh/ . 

Integration with gradle is done with the following gradle plugin for JMH: https://github.com/melix/jmh-gradle-plugin.

The plugin creates a source set for the JMH benchmarks, and provides a few gradle tasks for running and building the benchmarks.

## Running the tests via gradle task

Tests can be run via the "jmh" gradle task provided by the plugin. The README for the plugin provides the various parameters that
can be provided to the plugin. 

## Running the tests via JAR

To run the tests via JAR, you can build the benchmark jar using the gradle task "jmhJar". This jar is an executable jar 
that runs the benchmark tests. Example command:

```
java -jar service-map-stateful-benchmarks-0.1-beta-jmh.jar -r 600 -i 2 -p batchSize=100 -p windowDurationSeconds=180
```

The above command will run the benchmarks for 600 seconds (10 minutes) per iteration, 2 iterations. It also
sets the batchSize and windowDurationSeconds benchmark parameters. 
