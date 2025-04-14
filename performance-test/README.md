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

### Configurations

You can configure the test in order to target different endpoints with different configurations.
Supply the configurations as Java system variables on the command line.

For example, you can configure the port and use HTTPS with a custom certificate with the following:

```
./gradlew -Dport=7099 -Dprotocol=https -Dpath=/simple-sample-pipeline/logs -Djavax.net.ssl.keyStore=examples/demo/test_keystore.p12 :performance-test:gatlingRun-org.opensearch.dataprepper.test.performance.SingleRequestSimulation
```

**Available configuration options:**

* `host` - The host name or a comma-delimited list of host names. Defaults to `localhost`.
* `port` - The destination port. The default value is `2021`.
* `protocol` - The scheme to use in the URL. Can be `http` or `https`. Defaults to `http`.
* `path` - The path of the HTTP endpoint. This uses the default `http` path of `/log/ingest`.
* `authentication` - The authentication to use with the target. Currently supports `aws_sigv4`.
* `aws_region` - The AWS region to use in signing. Required with `aws_sigv4` authentication.
* `aws_service` - The AWS service name to use in signing. Required with `aws_sigv4` authentication.


### Using with Amazon OpenSearch Ingestion

This performance tool also works with [Amazon OpenSearch Ingestion](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/ingestion.html).

Setup your AWS credentials to meet your needs. Then run a command similar to the following:

```
./gradlew -Dhost=<your-custom-dns>.<aws-region>.osis.amazonaws.com -Dprotocol=https -Dpath=<your-path> -Dport=443 -Dauthentication=aws_sigv4 -Daws_region=<aws-region> -Daws_service=osis :performance-test:gatlingRun-org.opensearch.dataprepper.test.performance.SingleRequestSimulation
```

### Verify Gatling scenarios compile
```shell
./gradlew :performance-test:compileGatlingJava
```

### Deploying

You can also create an uber-jar that has everything needed to run the Gatling performance tests so that you can
deploy them easily.

```shell
./gradlew :performance-test:assemble
```

This will create an uber jar in `performance-test/build/libs` with the name `opensearch-data-prepper-performance-test-${VERSION}.jar`

You can run this using a command similar to the following.

```shell
java -jar performance-test/build/libs/opensearch-data-prepper-performance-test-2.11.0-SNAPSHOT.jar -s org.opensearch.dataprepper.test.performance.StaticRequestSimulation
```

# Gatling Documentation
[Gatling Quickstart](https://gatling.io/docs/gatling/tutorials/quickstart/)
[Gatling Advanced Simulations](https://gatling.io/docs/gatling/tutorials/advanced/)
[Passing Command Line Parameters](https://gatling.io/docs/gatling/guides/passing_parameters/)
[Gatling Gradle Plugin](https://gatling.io/docs/gatling/reference/current/extensions/gradle_plugin/)

[Building performance test simulation documentation](../docs/simulation_development.md)
