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
| `OsgiFrameworkRunner` | Entry point that bootstraps the OSGi framework and registers the plugin provider; implements `PluginProviderRegistrar` so Spring orders it before `DefaultPluginFactory` |
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

In legacy mode, only the classpath providers are returned.

Registration is guaranteed to happen before the first lookup rather than merely happening early: `OsgiFrameworkRunner` implements `PluginProviderRegistrar`, and `DefaultPluginFactory` takes an `Optional<PluginProviderRegistrar>` constructor argument. Spring must therefore fully initialize the runner — including its `@PostConstruct`, which starts Felix and registers `OsgiPluginRegistry` — before it can construct the factory, and no plugin can be looked up before the factory exists. The dependency is `Optional` because the only implementation lives in this module, which core depends on at runtime only; in legacy mode the argument is simply empty. `PluginProviderLoader.getPluginProviders()` re-reads the provider list on every lookup as defense in depth rather than as the ordering guarantee itself.

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

# Directory containing plugin bundle JARs (required in OSGi mode)
-Ddata-prepper.plugin.bundles.dir=/path/to/plugin/bundles
```

The `data-prepper.plugin.bundles.dir` property tells the OSGi framework where to find plugin bundle JARs. If not set, the framework starts with no plugin bundles and only classpath fallback is available.

Every `.jar` in that directory is treated as a plugin bundle and must carry OSGi manifest headers; a JAR without a `Bundle-SymbolicName` aborts startup with an error pointing at the Gradle plugin. The two exceptions are the `-sources.jar` and `-javadoc.jar` sidecars that Gradle and Maven publish next to a real artifact — those are skipped, because a build's output directory routinely contains them and they are never bundles. Any other non-bundle JAR is still rejected rather than silently ignored, so a plugin that forgot to apply the Gradle plugin fails loudly instead of going missing at pipeline start. Point the property at a directory holding only plugin bundles (and their sidecars), not at a general-purpose library directory.

Keep exactly one build of each bundle in that directory. OSGi permits several versions of one `Bundle-SymbolicName` to be installed side by side, and each of them registers the same plugin names, so a stale JAR left behind by a previous upgrade makes it undefined which version a pipeline actually gets. Replace bundles rather than adding to them.

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
- `org.slf4j.spi`
- `org.opensearch.dataprepper.plugin.osgi` (the framework's own package, for the bundle activator)

Each third-party package is exported with the version resolved for this build, because an export with no version attribute defaults to `0.0.0` and satisfies none of the version ranges bnd computes for a plugin bundle.

A plugin that imports third-party packages outside that set will fail bundle resolution at startup. For example, `file-source` imports `com.fasterxml.jackson.databind`, and the `opensearch` plugin depends on the AWS SDK, Jackson databind, and the OpenSearch clients — none of which the host exports. Such a plugin resolves only once the packages it needs are either added to the host export list defined by `DataPrepperOsgiPackages` (generated by the `generateSharedPackagesResource` task in `plugin-framework-osgi/build.gradle`), or shipped inside the bundle itself or alongside it as additional bundles.

## Third-Party Dependencies

OSGi support adds exactly two third-party tools to Data Prepper, both pinned to exact versions — never a range — so that resolution is reproducible.

| Coordinate | Scope | License | Project | Why this version |
| --- | --- | --- | --- | --- |
| `org.apache.felix:org.apache.felix.framework:7.0.5` | Runtime, this module only | Apache-2.0 (Apache Software Foundation) | [apache/felix-dev](https://github.com/apache/felix-dev) | Latest published release of the framework |
| `biz.aQute.bnd:biz.aQute.bnd.gradle:6.4.0` | Build time, `data-prepper-gradle-plugins/osgi-plugin` only | Apache-2.0 OR EPL-2.0 | [bndtools/bnd](https://github.com/bndtools/bnd) | Latest 6.x; 7.x is Java 17 bytecode and cannot load on Data Prepper's Java 11 Gradle JVM |

Both artifacts are published to Maven Central with detached PGP signatures (`.asc`). No other new third-party coordinate is introduced: every remaining dependency of this module (`javax.inject:1`, `javax.annotation:javax.annotation-api:1.3.2`, `io.micrometer:micrometer-core`, `org.slf4j:slf4j-api`) is already used at the same version elsewhere in the repository, and the OSGi core API (`org.osgi.framework.*`) is contained in the Felix framework JAR rather than pulled in as a separate `org.osgi:osgi.core` dependency.

The Felix framework is on the classpath in every build, but it is inert unless `data-prepper.plugin.framework=osgi` is set: in the default legacy mode no framework is created and no Felix code runs.

## SPI / ServiceLoader

The framework sets the Thread Context ClassLoader (TCCL) to the plugin's own bundle classloader at three specific boundaries, so that a standard `ServiceLoader.load(X)` call made from plugin code at those points resolves against the plugin bundle's own `META-INF/services`.

### Boundaries that are covered

| Boundary | Where | Mechanism |
|----------|-------|-----------|
| Bundle activation (`BundleActivator.start`) | `LegacyPluginBundleActivator.start` | `BundleClassLoaderScope.of(bundle)` |
| Plugin class resolution from the service registry | `OsgiPluginRegistry.addPluginService` | `BundleClassLoaderScope.of(bundle)` |
| Plugin construction (the annotated constructor) | `PluginCreator.newPluginInstance` | TCCL switched to `pluginClass.getClassLoader()` for the `newInstance` call, then restored |

The construction boundary lives in `data-prepper-plugin-framework` rather than here, because it applies to both frameworks: it switches the TCCL only when the classloader that defined the plugin class differs from the current TCCL, which under classpath mode is never, and under OSGi is always. It covers extensions too, since `ExtensionLoader` shares the same `PluginCreator`.

`OsgiIT` proves the construction boundary end to end: the `osgi_test_echo` test plugin's constructor resolves a service whose only declaration lives inside the plugin bundle, so the value the test reads back off the processed event could not have come from the test's own classpath.

### What is NOT covered

**Runtime invocation is not covered.** Once a plugin is constructed, calls into it — `Processor.execute`, `Sink.output`, source callbacks — run on pipeline worker threads with whatever TCCL those threads carry, which is not the bundle classloader. Wrapping every such call would mean a TCCL swap on the per-batch hot path, so the framework does not do it. Resolve SPI services in your constructor (where the TCCL is managed) and hold the result, or pass an explicit classloader as described below.

SPI calls on **threads the plugin spawns itself** (background executors, async callbacks, timers) are likewise not covered. Those threads do not inherit the managed TCCL because:
1. The TCCL is only managed at the three boundaries listed above.
2. New threads created by the plugin start with whatever TCCL was in effect when they were created, which may not be the bundle classloader.

**Plugin developer responsibility**: If your plugin resolves services outside its constructor — at runtime, or on its own threads — you must either:
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
