# Workspace Summary

## Overview
This workspace contains the Office 365 source plugin for OpenSearch Data Prepper, designed to ingest Office 365 audit and DLP data. The plugin is part of the larger Data Prepper ecosystem and integrates with Microsoft Graph API for data retrieval.

## Project Structure
```
src/main/java/org/opensearch/dataprepper/plugins/source/office365/
├── auth/                 # Authentication handling
├── configuration/        # Configuration classes
├── utils/               # Utility classes and constants
└── [root]               # Core plugin classes
```

## Technology Stack

### Core Technologies
- **Language**: Java
- **Build System**: Gradle
- **Framework**: Spring Framework (context and web modules)

### Key Dependencies
- Microsoft Graph API (v5.65.0)
- Microsoft Authentication Library (MSAL4J v1.13.9)
- Azure Identity (v1.11.1)
- Spring Framework
- Lombok (v1.18.30)
- Jackson (JSON/YAML processing)
- Micrometer (metrics)
- JSoup (v1.18.3)
- Commons IO

### Testing Framework
- JUnit Jupiter (v5.9.2)
- Data Prepper test common module
- Spring test support

## Development Guidelines

### Code Style
- 4 spaces for indentation
- 120 character line length limit
- Standard Java naming conventions:
  - PascalCase for classes
  - camelCase for methods and variables
  - UPPER_SNAKE_CASE for constants

### Logging
- Uses SLF4J logging framework
- Log levels:
  - TRACE: Detailed debugging
  - INFO: State changes and major operations
  - WARN: Non-critical issues
  - ERROR: Critical failures

### Metrics
- Uses Micrometer through PluginMetrics wrapper
- Metrics created in constructors
- Counter metrics for tracking operations

### Testing Practices
- Tests typically located in `src/test/java`
- Test files named with pattern `*Test.java` or `Test*.java`
- JUnit 5 features available
- Spring testing support integrated
- Data serialization testing support

## Architecture

### Plugin Architecture
- Extends CrawlerSourcePlugin
- Uses Data Prepper plugin annotations
- Event-based data processing
- Spring dependency injection

### Authentication
- Microsoft OAuth integration
- Azure Identity support
- Configurable authentication providers

### Configuration
- Annotation-based configuration
- Support for multiple audit content types
- YAML configuration support through Jackson

## Development Workflow
1. Build using Gradle
2. Tests run through JUnit Platform
3. Configuration through YAML files
4. Metrics exposed via Micrometer
5. Logging through SLF4J
