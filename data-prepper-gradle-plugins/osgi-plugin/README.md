# Data Prepper OSGi Gradle Plugin

A standalone, publishable Gradle plugin that prepares a Data Prepper plugin JAR
for OSGi consumption by baking an OSGi-compliant manifest into it at build time.

This replaces the runtime JAR-to-bundle adaptation previously performed by
`BundleAdapter` in `plugin-framework-osgi`. By generating the manifest at build
time, plugins are valid OSGi bundles from the moment they are built — no runtime
repackaging is required.

## Plugin ID

```
org.opensearch.dataprepper.osgi
```

## Usage

### Internal Data Prepper plugin projects

In your plugin's `build.gradle`:

```groovy
plugins {
    id 'org.opensearch.dataprepper.osgi'
}
```

### External plugin authors

Add the plugin to your `buildscript` dependencies or plugin management:

```groovy
// settings.gradle
pluginManagement {
    repositories {
        mavenCentral()
        // or wherever Data Prepper publishes its artifacts
    }
    plugins {
        id 'org.opensearch.dataprepper.osgi' version '<data-prepper-version>'
    }
}
```

Then in your plugin project's `build.gradle`:

```groovy
plugins {
    id 'org.opensearch.dataprepper.osgi'
}
```

## Requirements

Your project must include a resource file at:

```
src/main/resources/META-INF/data-prepper.plugins.properties
```

With the following property:

```properties
org.opensearch.dataprepper.plugin.packages=com.example.myplugin
```

The value is a comma-separated list of Java package names that contain your
`@DataPrepperPlugin`-annotated classes. The plugin scans these packages at
bundle activation time to register your plugin classes with the OSGi service
registry.

If this file is absent, the build will fail with a clear error message.

## Generated Manifest Headers

When applied, this plugin produces the following OSGi manifest headers in the
output JAR:

| Header | Value | Derivation |
|--------|-------|------------|
| `Bundle-SymbolicName` | `org.opensearch.dataprepper.plugin.<sanitized-name>` | Project name with non-alphanumeric chars replaced by dots |
| `Bundle-Version` | OSGi-normalized project version | `2.16.0-SNAPSHOT` becomes `2.16.0`; ensures 3-part numeric |
| `Export-Package` | All packages except `*.internal.*` | Computed by bnd from compiled bytecode |
| `Import-Package` | `*` (bnd default) | Computed by bnd from bytecode dependency analysis |
| `Bundle-Activator` | `org.opensearch.dataprepper.plugin.osgi.LegacyPluginBundleActivator` | Fixed; provided by `plugin-framework-osgi` at runtime |
| `DataPrepper-Plugin-Classes` | Comma-separated package names | Read from `data-prepper.plugins.properties` |

## How it works

1. The plugin applies `biz.aQute.bnd.builder` to the consuming project.
2. After project evaluation, it reads `data-prepper.plugins.properties` from the
   main resource directories.
3. It configures the jar task's bnd instructions with the headers listed above.
4. At jar time, bnd analyzes the compiled bytecode to compute accurate
   `Import-Package` and `Export-Package` values — this is more reliable than the
   runtime heuristic approach that `BundleAdapter` used.

## Comparison with BundleAdapter

| Aspect | BundleAdapter (runtime) | This plugin (build time) |
|--------|------------------------|--------------------------|
| When it runs | At application startup | At build time |
| Import-Package | Static list of shared API packages | bnd computes from actual bytecode |
| Export-Package | Discovered by scanning JAR entries | bnd computes from compiled classes |
| Bundle-Version | Always `1.0.0` | Actual project version (OSGi-normalized) |
| Performance | Rewrites JARs at startup | Zero startup cost |
| External plugins | Must be adapted at runtime | Already valid bundles |
