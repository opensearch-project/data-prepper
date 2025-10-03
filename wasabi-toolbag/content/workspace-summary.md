# Data Prepper Workspace Summary

## Overview

Data Prepper is a sophisticated Java-based data processing framework designed for ingesting, transforming, and routing observability data (logs, metrics, and traces). The project follows a plugin-based architecture with over 100 modules organized into core components and specialized plugins.

## Project Structure

### Core Architecture
- **Multi-module Gradle project** with 19 core modules and 40+ plugin modules
- **Plugin-based extensibility** allowing custom processors, sources, and sinks
- **Event-driven processing** with support for complex data pipelines
- **Distributed peer forwarding** for horizontal scaling

### Key Modules
- `data-prepper-api`: Core APIs and interfaces for plugin development
- `data-prepper-core`: Main processing engine and pipeline management
- `data-prepper-event`: Event model and handling framework
- `data-prepper-expression`: Expression language for data transformation
- `data-prepper-plugins/`: Collection of specialized plugins (S3, OpenSearch, Kafka, etc.)
- `data-prepper-test/`: Testing framework and utilities

## Technology Stack

### Languages and Versions
- **Java 11** (primary language, enforced via toolchain)
- **Gradle 8.8** (build system with custom plugins)
- **JSON/YAML** for configuration and data formats

### Key Dependencies
- **AWS SDK 2.30.23** (BOM-managed across all modules)
- **Jackson 2.17.2** for JSON processing
- **SLF4J 2.0.6** for logging
- **Armeria 1.32.5** for HTTP/gRPC communication
- **OpenSearch Java Client 2.20.0** for search functionality
- **JUnit 5.8.2** with Mockito 5.12.0 for testing

### Security Considerations
- Extensive CVE-based dependency pinning (Netty, Log4j, Apache Commons)
- Input validation and sanitization frameworks
- Secure configuration management

## Build System

### Gradle Configuration
- **Version catalogs** for dependency management (`libs` and `testLibs`)
- **Multi-project build** with sophisticated dependency graphs
- **Parallel execution** support with configurable worker counts
- **Custom build tasks** for release coordination and testing

### Code Quality Tools
- **Checkstyle 10.26.1** with custom rules and import controls
- **Spotless 6.22.0** for automated code formatting
- **JaCoCo** code coverage with 65% minimum threshold
- **License reporting** for dependency compliance

### Quality Standards
- **No System.out.println** usage (enforced via Checkstyle)
- **No star imports** - explicit import statements required
- **Proper modifier ordering** (public → protected → private → abstract → static → final)
- **UTF-8 encoding** enforced across all text files

## Testing Framework

### Testing Stack
- **JUnit 5 (Jupiter)** as primary testing framework
- **Mockito 5.12.0** for mocking and test doubles
- **Hamcrest 2.2** for assertion matchers
- **AssertJ** for fluent assertions (used in some modules)

### Test Organization
- **Unit tests**: `src/test/java` in each module
- **Integration tests**: `src/integrationTest/java` with separate source sets
- **End-to-end tests**: Dedicated `e2e-test/` modules with containerized environments
- **Performance tests**: Gatling-based load testing in `performance-test/`

### Testing Patterns
- **@Test** annotations with JUnit 5
- **@ExtendWith(MockitoExtension.class)** for Mockito integration
- **@TestInstance(TestInstance.Lifecycle.PER_CLASS)** for stateful tests
- **@ParameterizedTest** for data-driven testing
- **Comprehensive Javadoc** with `@since` tags

## Logging and Metrics

### Logging Framework
```java
// SLF4J Logger Usage Pattern
private static final Logger LOG = LoggerFactory.getLogger(ClassName.class);

// Supported Log Levels
LOG.error("Error message", exception);
LOG.warn("Warning message");
LOG.info("Info message");
LOG.debug("Debug message");
LOG.trace("Trace message");
```

### Metrics Framework
```java
// Micrometer Metrics Usage
Counter counter = Metrics.counter("my.counter", "tag", "value");
Timer.Sample sample = Timer.start(Metrics.globalRegistry);
sample.stop(Timer.builder("my.timer").register(Metrics.globalRegistry));
```

## Plugin Development

### Plugin Architecture
- **@DataPrepperPlugin** annotation for plugin registration
- **Processor**, **Source**, **Sink** interfaces for different plugin types
- **Configuration classes** with `@JsonProperty` annotations
- **Plugin framework** provides lifecycle management and dependency injection

### Plugin Structure
```
data-prepper-plugins/my-plugin/
├── build.gradle                    # Plugin dependencies
├── src/main/java/
│   ├── MyPlugin.java               # @DataPrepperPlugin implementation
│   └── MyPluginConfig.java         # @JsonProperty configuration
├── src/test/java/                  # Unit tests
├── src/integrationTest/java/       # Integration tests
└── src/main/resources/
    └── META-INF/services/          # Service registration
```

### Configuration Patterns
```java
@JsonClassDescription("My plugin configuration")
public class MyPluginConfig {
    @JsonProperty("my_property")
    @JsonPropertyDescription("Description of property")
    private String myProperty = "default_value";

    @Valid
    @JsonProperty("nested_config")
    private NestedConfig nestedConfig;
}
```

## Development Workflow

### Code Style Guidelines
- **4-space indentation** (no tabs)
- **100-character line limit** (suggested, not enforced)
- **CamelCase** for classes, **camelCase** for methods/variables
- **final** modifier for method parameters where appropriate
- **OpenSearch Contributors** copyright header format

### Build Commands
```bash
# Run all tests
./gradlew test integrationTest

# Run tests for specific module
./gradlew :data-prepper-plugins/s3-sink:test

# Code quality checks
./gradlew checkstyleMain checkstyleTest

# Generate test coverage report
./gradlew jacocoTestReport

# Build all modules
./gradlew build

# Parallel build with custom worker count
./gradlew build --parallel --max-workers=4
```

### Plugin Validation
- Structure validation (required directories, build.gradle)
- Annotation validation (@DataPrepperPlugin, @JsonProperty)
- Integration validation (tests, service registration)
- Documentation validation (Javadoc coverage)

## Available Tools

The workspace includes three custom tools to enhance development productivity:

### GradleTestRunner
Intelligently runs Gradle tests with filtering by module, test category, and patterns. Parses and formats JUnit XML results for analysis.

**Usage**: Run specific test suites, filter by test type (unit/integration/e2e), analyze test results

### JavaCodeAnalyzer
Comprehensive code quality analysis including CheckStyle violations, code coverage analysis, dependency analysis, and logging pattern validation.

**Usage**: Aggregate quality metrics, identify coverage gaps, detect dependency conflicts, validate logging patterns

### PluginValidator
Validates data-prepper plugin structure and compliance with framework patterns, annotations, and integration requirements.

**Usage**: Validate new plugins, ensure compliance with architecture patterns, check for proper service registration

## Key Development Practices

### Error Handling
- Use SLF4J logging instead of System.out/err
- Provide meaningful error messages with context
- Handle exceptions appropriately with proper logging levels

### Configuration Management
- Use `@JsonProperty` annotations for all configuration fields
- Provide `@JsonPropertyDescription` for documentation
- Implement proper validation with `@Valid` annotations
- Use sensible defaults and document required vs optional fields

### Testing Standards
- Maintain minimum 65% code coverage (target: 75%)
- Write both unit and integration tests
- Use proper test structure with setup/teardown
- Mock external dependencies appropriately

### Documentation Requirements
- Comprehensive Javadoc with `@since` tags
- Clear configuration documentation
- Integration examples and usage patterns
- Performance characteristics and limitations

## Data Pipeline Patterns

### Common Processing Flows
1. **Log Processing**: Raw logs → Parse → Transform → Route → Store
2. **Metrics Processing**: Raw metrics → Aggregate → Transform → Export
3. **Trace Processing**: Spans → Group → Transform → Store

### Configuration Examples
```yaml
# Basic pipeline configuration
source:
  http:
    port: 8080

processor:
  - grok:
      patterns_files_glob: "/patterns/*.grok"
  - date:
      from_time_received: true

sink:
  - opensearch:
      hosts: ["https://opensearch:9200"]
      index: "logs-%{yyyy.MM.dd}"
```

This workspace provides a robust foundation for building and extending data processing pipelines with strong emphasis on code quality, testing, and maintainability.
