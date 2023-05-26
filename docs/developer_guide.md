# Data Prepper Developer Guide

This page is for anybody who wishes to contribute code to Data Prepper. Welcome!

## Contributions

First, please read our [contribution guide](../CONTRIBUTING.md) for more information on how to contribute to Data Prepper.

## Installation Prerequisites

### Java Versions

Building Data Prepper requires JDK 11 or 17. The Data Prepper Gradle build runs in a Java 11 or 17 JVM, but uses
[Gradle toolchains](https://docs.gradle.org/current/userguide/toolchains.html) to compile the Java
code using Java 11. If you have a JDK 11 installed locally, Gradle will use your installed JDK 11. If you
do not, Gradle will install JDK 11.

All main source code builds on JDK 11, so it must be compatible with Java 11. The test code
(unit and integration tests) runs using JDK 11.


## Building from source

The assemble task will build the Jar files and create a runnable distribution without running the integration
tests. If you are just looking to build Data Prepper from source, this build is faster than running the integration test suite.

To build the project from source, run the following command from the project root:

```
./gradlew assemble
```


### Full Project Build

Running the build command will assemble the Jar files needed
for running DataPrepper. It will also run the integration test
suite.

To build, run the following command from the project root:

```
./gradlew build
```


## Running the project

Before running Data Prepper, check that configuration files (see [configuration](configuration.md) docs for more 
information) have been put in the respective folders under Data Prepper home directory. When building from source, 
Data Prepper home directory is at `release/archives/linux/build/install/opensearch-data-prepper-$VERSION-linux-x64` 
($VERSION is the current version as defined in [gradle.properties](../gradle.properties)). The configuration files 
should be put in the following folders:

1. `data-prepper-config.yaml` in `config/` folder
2. `pipelines.yaml` in `pipelines/` folder

Go to home directory:
```
cd release/archives/linux/build/install/opensearch-data-prepper-$VERSION-linux-x64
```

Data Prepper can then be run with the following commands:
```
bin/data-prepper
```

You can also supply your own pipeline configuration file path followed by the server configuration file path, but the support for this 
method will be dropped in a future release. Example:
```
bin/data-prepper pipelines.yaml data-prepper-config.yaml
```

Additionally, Log4j 2 configuration file is read from `config/log4j2.properties` in the application's home directory.


## Building & Running the Docker Image

In some cases, you may wish to build a local Docker image and run it. This is useful if you are making a change to the
Docker image, are looking to run a bleeding-edge Docker image, or are needing a custom-built Docker image of Data Prepper.

### Building the Docker Image

To build the Docker image, run:

```
./gradlew clean :release:docker:docker
```

If successful, the Docker image will be available locally.
The repository is `opensearch-data-prepper` and the tag is
the current version as defined in [gradle.properties](../gradle.properties).

You can run the following command in Linux environments to see
your Data Prepper Docker images:

```
docker images | grep opensearch-data-prepper
```

The results will look somewhat like the following:
```
opensearch-data-prepper   2.0.0-SNAPSHOT   3e81ef26250c   23 hours ago   566MB
```

### Running from a Local Docker Image

If you build a local Docker image, you can run it using a variation on the following command. You
may wish to change the ports you map depending on your specific pipeline configuration.

```
docker run \
-p 21890:21890 \
-v ${PWD}/pipelines.yaml:/usr/share/data-prepper/pipelines/pipelines.yaml \
-v ${PWD}/data-prepper-config.yaml:/usr/share/data-prepper/config/data-prepper-config.yaml \
opensearch-data-prepper:2.0.0-SNAPSHOT
```


## Contributing your Code to Data Prepper

When you are ready to contribute a change to Data Prepper, please create a GitHub Pull Request (PR). Your PR should target `main`.

The Data Prepper maintainers will review your PR and merge it once it is approved.

Some changes containing bug fixes or security fixes may be eligible for a patch release.
If you believe your change should be a patch release, please see [Backporting](#backporting)

### Branches

The Data Prepper maintainers use the `main` branch for the next upcoming release (major or minor).

Near the time of the next release, we create a release branch for that upcoming
release (e.g. `1.2`). We perform our release builds from this branch. Any patch
releases also build from that release branch.

### <a name="identification_keys">Backporting</a>

When you create a PR which targets `main` and need this change as a patch to a previous version
of Data Prepper, use the auto backport GitHub Action. All you need to do is add the label
`backport <version>` to your PR which is targeting `main`. After the PR is merged, the GitHub
Action will create a new PR to cherry-pick those changes into the `<version>` branch.
A Data Prepper maintainer will need to approve and merge the backported code into the target branch.

The auto-generated PR will be on a branch named `backport/backport-<original PR number>-to-<version>`.

Data Prepper supports patch releases only on the latest version (e.g. 2.1) and on the last version
for the previous major release (e.g. 1.4 after 2.0 has been released). These releases are
only for bug fixes or security fixes. Please use backports only for bug and security fixes
and only targeting candidate releases. You can ask about backporting in your PR or by creating a GitHub
issue to request that a previous change be backported.

## Coding Guidance

### Documentation

Documentation is very important for users of Data Prepper and contributors. We are using the
following conventions for documentation.

1. Document features in the OpenSearch [documentation-website](https://github.com/opensearch-project/documentation-website). This makes the documentation available at https://opensearch.org/docs/latest/data-prepper.
2. Document any development guidance in this repository in markdown. In particular, for plugins, use the `README.md` file within the plugin project to note anything developers should keep in mind.
3. Provide Javadocs for all public classes, methods, and fields. Plugins need not follow this guidance since their classes are generally not exposed.
4. Avoid commenting within code, unless it is required to understand that code.

#### Documentation Process

When you submit a feature PR, please be sure to also submit a new "Documentation issue" 
[issue in the documentation-website](https://github.com/opensearch-project/documentation-website/issues/new/choose) project.

Please include in this feature a link to the GitHub issue which has information on the feature.
This GitHub issue will often have sample configurations and explanations of the options available to users.
Please also provide any additional guidance for the team doing the documentation.
Please include a link to that documentation issue in the PR you created for Data Prepper.

You are also welcome to submit a PR directly in the [documentation-website](https://github.com/opensearch-project/documentation-website).

### Code

For the most part, we use common Java conventions. Here are a few things to keep in mind.

1. Use descriptive names for classes, methods, fields, and variables.
2. Avoid abbreviations unless they are widely accepted
3. Use final on all variables which are not reassigned
4. Wildcard imports are not allowed.
5. Static imports are preferred over qualified imports when using static methods
6. Prefer creating non-static methods whenever possible. Static methods should generally be avoid as they are often used as a shortcut. Sometimes static methods are the best solution such as when using a builder.
7. Public utility or “common” classes are not permitted.
    1. They are fine in test code
    2. They are fine if package protected
8. Use Optional for return values if the value may not be present. This should be preferred to returning null.
9. Do not create checked exceptions, and do not throw checked exceptions from public methods whenever possible. In general, if you call a method with a checked exception, you should wrap that exception into an unchecked exception.
    1. Throwing checked exceptions from private methods is acceptable.

### Formatting

Please use the following formatting guidelines:

* Java indent is 4 spaces. No tabs.
* Maximum line width is 140 characters
* We place opening braces at the end of the line, rather than on its own line

The official formatting rules for this project are committed as a Checkstyle configuration in [`config/checkstyle/checkstyle.xml`](../config/checkstyle/checkstyle.xml).

If you are using IntelliJ, you can use the unofficial Checkstyle IDEA plugin. [These instructions](https://stackoverflow.com/a/26957047/650176) may be useful for configuring the rules.

### Dependencies

1. You should first raise an issue in the Data Prepper project if you are interested in adding a new dependency to the core projects.
2. Avoid using dependencies which provide similar functionality to existing dependencies.
    1. For example, this project uses Jackson, so do not add Gson
    2. If core Java has the function or feature, prefer it over an external library. Example: Guava’s hashcode and equals methods when Java’s Objects class has them.

### Testing

We have the following categories for tests:

* Unit tests - Test a single class in isolation.
* Integration tests - Test a large component or set of classes in isolation.
* End-to-end tests - Tests which run an actual Data Prepper. The should generally be in the [`e2e-test`](../e2e-test) project.


Testing Guidelines:

1. Use JUnit 5 for all new test suites
   1. You are encouraged to update existing JUnit 4 tests to JUnit 5, but this is not necessary.
2. Use Hamcrest of assertions
3. Use Mockito for mocking
4. Each class should have a unit test.
5. Unit test class names should end with Test.
6. Each large component should have an integration test.
   1. A good example is a plugin. Plugins should have their own integration tests which integrate all of the plugin’s classes. However, these tests do not run a full Data Prepper.
7. Integration test class names should end with IT.
8. Test names should indicate what is being tested, if we see a failed test we should be able to look at the test name and have a good idea about what just failed with minimal context about the code being written
   1. Two good approaches may be used, depending on what you are testing:
      1. methodUnderTest_condition_result
      2. test_when_something_condition_then_something_else
   2. Please avoid generic test names like “testSuccess”

### Gradle

1. Our Gradle builds use Groovy, so follow our normal Java styles in the build files. For example, use camel case rather than snake case.
2. Use Gradle strings (single quote) unless you need string interpolation. If you need string interpolation, use a GString (double quotes)

## CI Builds

Before merging in your PR, the Data Prepper continuous integration (CI) builds must pass. These builds
run as GitHub Actions.

If an Action is failing, please view the log and determine what is causing your commit to fail. If a test
fails, please check the *Summary* section of that Action. There may be artifacts for the test results. You
can download these and view the result information. Additionally, many builds have *Unit Test Results* job
which includes a summary of the results.

## More Information

We have the following pages for specific development guidance on the topics:

* [Plugin Development](plugin_development.md)
* [Error Handling](error_handling.md)
* [Logs](logs.md)
