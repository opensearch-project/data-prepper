# How to run performance tests

### Run all simulations
```shell
./gradlew :performance-test:gatlingRun
```

If you need to run a task several times in a row (with different environment or configuration), you may use the --rerun-tasks flag.

example:

```shell
./gradlew --rerun-tasks :performance-test:gatlingRun
```

### Run specific test

To run TargetRpsSimulation. Source code available at `./performance-test/src/gatling/java/org/opensearch/dataprepper/test/performance/TargetRpsSimulation.java`

```shell
# ./gradlew gatlingRun-<simulation-class-path>
./gradlew :performance-test:gatlingRun-org.opensearch.dataprepper.test.performance.TargetRpsSimulation
```

### Verify Gatling scenarios compile
```shell
./gradlew :performance-test:compileGatlingJava
```

# Gatling Documentation
[Gatling Quickstart](https://gatling.io/docs/gatling/tutorials/quickstart/)
[Gatling Advanced Simulations](https://gatling.io/docs/gatling/tutorials/advanced/)
[Passing Command Line Parameters](https://gatling.io/docs/gatling/guides/passing_parameters/)
[Gatling Gradle Plugin](https://gatling.io/docs/gatling/reference/current/extensions/gradle_plugin/)

[Building performance test simulation documentation](../docs/simulation_development.md)
