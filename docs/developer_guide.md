# Data Prepper Developer Guide

This page is for anybody who wishes to contribute code to Data Prepper. Welcome!

## Contributions

First, please read our [contribution guide](../CONTRIBUTING.md) for more information on how to contribute to Data Prepper.

## Installation Prerequisites

### JDK Versions

Running Data Prepper requires JDK 8 and above.

Running the integration tests requires JDK 14 or 15.


## Building from source

The assemble task will build the Jar files without running the integration
tests. You can use these jar files for running DataPrepper. If you are just
looking to use DataPrepper and modify it, this build
is faster than running the integration test suite and only requires JDK 8+.

To build the project from source, run

```
./gradlew assemble
```

from the project root. 

### Full Project Build

Running the build command will assemble the Jar files needed
for running DataPrepper. It will also run the integration test
suite.

To build, run

```
./gradlew build
```

from the project root.

## Running the project

After building, the project can be run from the executable JAR **data-prepper-core-$VERSION**
found in the **build/libs** directory of the **data-prepper-core** subproject. The executable JAR takes
two arguments:
1. A Pipeline configuration file
2. A Data Prepper configuration file

See [configuration](configuration.md) docs for more information.

Example java command:
```
java -jar data-prepper-core-$VERSION.jar pipelines.yaml data-prepper-config.yaml
```

Optionally add `"-Dlog4j.configurationFile=config/log4j2.properties"` to the command if you would like to pass a custom log4j2 properties file. If no properties file is provided, Data Prepper will default to the log4j2.properties file in the *shared-config* directory.

## Coding Guidance

### Documentation

Documentation is very important for users of Data Prepper and contributors. We are using the
following conventions for documentation.

1. Document features in markdown. Plugins should have detailed documentation in a `README.md` file in the plugin project directory. Documentation for all of Data Prepper should be in the [docs](../docs) directory.
2. Provide Javadocs for all public classes, methods, and fields. Plugins need not follow this guidance since their classes are generally not exposed.
3. Avoid commenting within code, unless it is required to understand that code.

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

## More Information

We have the following pages for specific development guidance on the topics:

* [Plugin Development](plugin_development.md)
* [Error Handling](error_handling.md)
* [Logs](logs.md)
