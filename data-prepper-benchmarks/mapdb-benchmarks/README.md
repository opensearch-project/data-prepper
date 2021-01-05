# MapDb Benchmarks

This package uses JMH (https://openjdk.java.net/projects/code-tools/jmh/) to benchmark the MapDb Prepper State plugin. 
To use jmh benchmarking easily with gradle, this package uses a jmh gradle plugin  (https://github.com/melix/jmh-gradle-plugin/) . 
Details on configuration and other options can be found there. 

To run the benchmarks from this directory, run the following command:

```
../../gradlew jmh
```

To build an executable standalone jar of these benchmarks, run:

```
../../gradlew jmhJar
```