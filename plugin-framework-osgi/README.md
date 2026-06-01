# Plugin Framework OSGi

This module provides Apache Felix OSGi integration for Data Prepper's plugin system, enabling classloader isolation and deploy-time bundle validation.

## Architecture

```
┌─────────────────────────────────────────────┐
│            OsgiFrameworkRunner               │
│         (feature flag: data-prepper.plugin.framework)     │
│                    │                         │
│          PluginProviderLoader                │
│          ┌────────┴────────┐                 │
│   ┌──────────────┐   ┌───────────────────┐  │
│   │    Legacy     │   │   OSGi            │  │
│   │  Classpath    │   │                   │  │
│   │  Provider     │   │ OsgiPluginRegistry│  │
│   │  (fallback)   │   │   (priority)      │  │
│   └──────────────┘   │        │           │  │
│                       │ FelixPluginManager │  │
│                       │        │           │  │
│                       │ StaticBundleLoader  │  │
│                       └───────────────────┘  │
└─────────────────────────────────────────────┘
```

In OSGi mode, both providers coexist: the OSGi registry is consulted first, with the classpath provider as fallback for plugins not yet bundled.

## Components

| Class | Purpose |
|-------|---------|
| `OsgiFrameworkRunner` | Entry point that bootstraps the OSGi framework and registers the plugin provider |
| `PluginProviderLoader` | Merges OSGi and classpath providers — OSGi takes priority, classpath is fallback (lives in `data-prepper-plugin-framework`) |
| `FelixPluginManager` | Manages embedded Felix OSGi framework lifecycle (init, start, stop) |
| `OsgiPluginRegistry` | Implements `PluginProvider` backed by OSGi service registry |
| `StaticBundleLoader` | Installs, resolves, and starts bundles; validates OSGi manifests; emits Micrometer metrics |
| `BundleClassLoaderScope` | Manages TCCL for bundle activation and service scanning |
| `BundleHealthCheck` | Queries bundle states on demand and reports structural framework health |
| `BundleResolutionErrorTranslator` | Converts OSGi resolution errors into human-readable diagnostics |
| `DataPrepperOsgiPackages` | Defines the set of Data Prepper packages exported to the OSGi framework (build-time generated from data-prepper-api) |
| `LegacyPluginBundleActivator` | Reads the `DataPrepper-Plugin-Classes` manifest header and registers discovered plugin classes as OSGi services |

## Plugin Resolution Precedence

When an OSGi bundle and a classpath plugin declare the same plugin name, **the OSGi bundle wins**. This is intended behaviour: bundles shadow same-named classpath plugins, so a plugin can be moved into a bundle while its classpath copy is still present.

The precedence comes from provider ordering rather than from registration order. `OsgiFrameworkRunner` registers `OsgiPluginRegistry` through `PluginProviderLoader.registerProvider`, which appends it to a separate list of additional providers. `PluginProviderLoader.getPluginProviders()` then — in OSGi mode only, and only once at least one additional provider exists — returns the additional (OSGi) providers first, followed by the classpath providers. `DefaultPluginFactory` resolves a plugin name by taking the first provider that returns a match (`findFirst`), so the OSGi registry is always consulted before the classpath.

In legacy mode, and in OSGi mode before the OSGi provider has registered, only the classpath providers are returned.

## Lifecycle

The OSGi framework follows a **static lifecycle**:

1. **Startup**: Felix starts, bundles are installed, resolved, and started to ACTIVE state. The `OsgiPluginRegistry` is registered as a `PluginProvider`.
2. **Runtime**: Bundles remain in ACTIVE state for the duration of the process. No runtime install/uninstall occurs.
3. **Shutdown**: Felix is stopped and all bundles are torn down.

If any failure occurs during framework initialization, startup **aborts immediately** (fail-fast). There is no silent fallback to legacy mode.

## Feature Flag

Control the plugin framework mode via system properties:

```bash
# Legacy mode (default) - uses existing classpath scanning
-Ddata-prepper.plugin.framework=legacy

# OSGi mode - uses Felix framework
-Ddata-prepper.plugin.framework=osgi

# Directory containing plugin JARs (required in OSGi mode)
-Ddata-prepper.plugin.bundles.dir=/path/to/plugin/bundles
```

The `data-prepper.plugin.bundles.dir` property tells the OSGi framework where to find plugin JARs. If not set, the framework starts with no plugin bundles and only classpath fallback is available.

## Rollback Procedure

1. Set `-Ddata-prepper.plugin.framework=legacy` (or remove the property entirely)
2. Restart Data Prepper
3. The system reverts to classpath-based plugin discovery

No code changes or redeployment required for rollback.

## Plugin Developer Migration Guide

### Existing Plugins

Existing plugins must apply the `org.opensearch.dataprepper.plugin` Gradle plugin to their build so that OSGi manifest headers are baked into the plugin JAR at build time. A JAR without a `Bundle-SymbolicName` header is rejected at startup with a clear error message pointing to the Gradle plugin.

Applying the Gradle plugin is necessary but not sufficient for every plugin — see [Current Limitations](#current-limitations).

### New Plugins (Optional OSGi-Native)

New plugins can optionally include OSGi manifest headers for better integration:

```
Bundle-SymbolicName: org.opensearch.dataprepper.plugin.myplugin
Bundle-Version: 1.0.0
Export-Package: org.opensearch.dataprepper.plugins.myplugin
Import-Package: org.opensearch.dataprepper.model.annotations,
 org.opensearch.dataprepper.model.processor
```

### Current Limitations

The OSGi path is currently proven only for plugins whose dependencies fall within the packages the host exports. Today that is every `org.opensearch.dataprepper.*` package from `data-prepper-api` except `org.opensearch.dataprepper.plugins.*`, plus a small fixed third-party set:

- `com.fasterxml.jackson.annotation`
- `jakarta.validation`
- `jakarta.validation.constraints`
- `org.slf4j`
- `org.opensearch.dataprepper.plugin.osgi` (the framework's own package, for the bundle activator)

A plugin that imports third-party packages outside that set will fail bundle resolution at startup. For example, `file-source` imports `com.fasterxml.jackson.databind`, and the `opensearch` plugin depends on the AWS SDK, Jackson databind, and the OpenSearch clients — none of which the host exports. Such a plugin resolves only once the packages it needs are either added to the host export list defined by `DataPrepperOsgiPackages` (generated by the `generateSharedPackagesResource` task in `plugin-framework-osgi/build.gradle`), or shipped inside the bundle itself or alongside it as additional bundles.

## SPI / ServiceLoader

The OSGi framework manages the Thread Context ClassLoader (TCCL) at the plugin invocation boundary so that standard `ServiceLoader.load(X)` calls work correctly for plugin code.

### How it works

During bundle activation and service scanning, the framework wraps those calls in a `BundleClassLoaderScope` that sets the TCCL to the bundle's own classloader. This means:

- **Bundle activation (`BundleActivator.start`)**: Plugin code run during activation has the correct TCCL.
- **Service lookup**: When the `OsgiPluginRegistry` resolves a plugin class through its publishing bundle, the TCCL is set to that bundle's classloader.

### What is NOT covered

SPI calls on **threads the plugin spawns itself** (background executors, async callbacks, timers) are NOT automatically covered. Those threads do not inherit the managed TCCL because:
1. The TCCL is only managed at the framework's invocation boundary (construction / lifecycle calls).
2. New threads created by the plugin start with whatever TCCL was in effect when they were created, which may not be the bundle classloader.

**Plugin developer responsibility**: If your plugin spawns its own threads and uses `ServiceLoader.load(X)` on those threads, you must either:
- Set the TCCL on your thread explicitly: `Thread.currentThread().setContextClassLoader(getClass().getClassLoader())`
- Use the two-argument form: `ServiceLoader.load(X, getClass().getClassLoader())`

### Libraries that already work without TCCL

Some libraries (e.g., Jackson with `ObjectMapper.findAndRegisterModules()`) pass an explicit classloader to their SPI discovery calls. These work correctly under OSGi without any TCCL management.

## Metrics

The OSGi framework emits Micrometer metrics for operational observability:

| Metric | Type | Description |
|--------|------|-------------|
| `osgi.plugin.bundlesLoaded` | Counter | Number of bundles successfully loaded |
| `osgi.plugin.bundlesFailed` | Counter | Number of bundles that failed to load |
| `osgi.plugin.resolutionDuration` | Timer | Time spent resolving bundle dependencies |
| `osgi.plugin.bundlesActive` | Gauge | Current number of active bundles |

## Dynamic Loading (Future / Test-Only)

A `PluginHotLoader` class exists in test scope for experimental integration tests that exercise dynamic bundle lifecycle. Production hot-reload is **not supported** in the initial release and is pending future design work. The hot loader is not on the production classpath.
