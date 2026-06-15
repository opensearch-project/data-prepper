/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * Wraps existing Data Prepper plugin JARs as OSGi bundles without requiring
 * plugin code changes. Generates OSGi MANIFEST.MF headers from existing
 * plugin metadata.
 */
public class BundleAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(BundleAdapter.class);
    private static final String BUNDLE_SYMBOLIC_NAME = "Bundle-SymbolicName";
    private static final String BUNDLE_VERSION = "Bundle-Version";
    private static final String BUNDLE_ACTIVATOR = "Bundle-Activator";
    private static final String IMPORT_PACKAGE = "Import-Package";
    private static final String EXPORT_PACKAGE = "Export-Package";
    private static final String DATAPREPPER_PLUGIN_CLASSES = "DataPrepper-Plugin-Classes";

    /**
     * Packages exported by the Data Prepper framework that plugins may import.
     * Only these packages are shared via the parent classloader — all other
     * dependencies remain isolated within each bundle's own classloader.
     */
    private static final List<String> SHARED_API_PACKAGES;
    static {
        final ArrayList<String> pkgs = new ArrayList<>(DataPrepperOsgiPackages.SHARED_API_PACKAGES);
        pkgs.add(DataPrepperOsgiPackages.SLF4J_PACKAGE);
        pkgs.add(DataPrepperOsgiPackages.FRAMEWORK_PACKAGE);
        SHARED_API_PACKAGES = Collections.unmodifiableList(pkgs);
    }

    private static final String ADAPTED_BUNDLES_SUBDIR = "adapted-bundles";

    private final BundleContext bundleContext;
    private final File adaptedBundlesDir;

    public BundleAdapter(final BundleContext bundleContext) {
        this.bundleContext = Objects.requireNonNull(bundleContext, "bundleContext must not be null");
        final File dataFile = bundleContext.getDataFile(ADAPTED_BUNDLES_SUBDIR);
        if (dataFile != null) {
            this.adaptedBundlesDir = dataFile;
        } else {
            this.adaptedBundlesDir = new File(System.getProperty("java.io.tmpdir"), "dp-osgi-adapted");
        }
        if (!adaptedBundlesDir.exists()) {
            if (!adaptedBundlesDir.mkdirs() && !adaptedBundlesDir.exists()) {
                throw new IllegalStateException(
                        "Failed to create adapted-bundles directory: " + adaptedBundlesDir.getAbsolutePath());
            }
        }
    }

    /**
     * Adapts a legacy plugin JAR into an OSGi-compatible bundle and installs it.
     *
     * @param jarFile the plugin JAR file
     * @return the installed Bundle
     * @throws IOException if the JAR cannot be read or the adapted bundle cannot be written
     * @throws BundleException if the bundle cannot be installed
     */
    public Bundle adaptAndInstall(final File jarFile) throws IOException, BundleException {
        Objects.requireNonNull(jarFile, "jarFile must not be null");
        if (!jarFile.exists()) {
            throw new IllegalArgumentException("JAR file does not exist: " + jarFile);
        }

        if (isOsgiBundle(jarFile)) {
            LOG.debug("JAR {} is already an OSGi bundle, installing directly", jarFile.getName());
            return bundleContext.installBundle(jarFile.toURI().toString());
        }

        final File adaptedJar = createAdaptedBundle(jarFile);
        return bundleContext.installBundle(adaptedJar.toURI().toString());
    }

    /**
     * Checks if a JAR already has OSGi bundle headers.
     */
    boolean isOsgiBundle(final File jarFile) throws IOException {
        try (JarFile jar = new JarFile(jarFile)) {
            final Manifest manifest = jar.getManifest();
            return manifest != null &&
                    manifest.getMainAttributes().getValue(BUNDLE_SYMBOLIC_NAME) != null;
        }
    }

    /**
     * Creates an adapted JAR with OSGi manifest headers injected.
     * The adapted file is written into the framework storage directory so it is
     * cleaned up alongside the Felix cache on framework re-init.
     * <p>
     * Writes to a temporary file first, then atomically renames to the target
     * location to avoid leaving partial files on failure.
     */
    File createAdaptedBundle(final File sourceJar) throws IOException {
        final String baseName = sourceJar.getName().replaceAll("\\.jar$", "");
        final File adaptedJar = new File(adaptedBundlesDir, "dp-osgi-" + baseName + ".jar");
        final File tempFile = new File(adaptedBundlesDir, "dp-osgi-" + baseName + ".jar.tmp");

        try {
            try (JarFile source = new JarFile(sourceJar)) {
                final Manifest manifest = buildOsgiManifest(source, baseName);

                try (FileOutputStream fos = new FileOutputStream(tempFile);
                     JarOutputStream target = new JarOutputStream(fos, manifest)) {
                    final Enumeration<JarEntry> entries = source.entries();
                    while (entries.hasMoreElements()) {
                        final JarEntry entry = entries.nextElement();
                        if ("META-INF/MANIFEST.MF".equals(entry.getName())) {
                            continue;
                        }
                        target.putNextEntry(new ZipEntry(entry.getName()));
                        try (InputStream is = source.getInputStream(entry)) {
                            is.transferTo(target);
                        }
                        target.closeEntry();
                    }
                }
            }

            try {
                Files.move(tempFile.toPath(), adaptedJar.toPath(), StandardCopyOption.ATOMIC_MOVE);
            } catch (final IOException e) {
                Files.move(tempFile.toPath(), adaptedJar.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (final IOException e) {
            tempFile.delete();
            throw e;
        }

        LOG.debug("Created adapted OSGi bundle: {}", adaptedJar.getName());
        return adaptedJar;
    }

    private Manifest buildOsgiManifest(final JarFile sourceJar, final String baseName) throws IOException {
        final Manifest original = sourceJar.getManifest();
        final Manifest manifest = original != null ? new Manifest(original) : new Manifest();
        final Attributes attrs = manifest.getMainAttributes();

        if (attrs.getValue(Attributes.Name.MANIFEST_VERSION) == null) {
            attrs.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        }

        attrs.putValue(BUNDLE_SYMBOLIC_NAME, "org.opensearch.dataprepper.plugin." + sanitizeName(baseName));
        attrs.putValue(BUNDLE_VERSION, "1.0.0");

        // Only import Data Prepper API packages from the parent classloader.
        // All other dependencies stay isolated within the bundle's own classloader.
        final List<String> bundlePackages = discoverPackages(sourceJar);
        final List<String> importPackages = new ArrayList<>(SHARED_API_PACKAGES);
        // Remove any packages the bundle itself provides — those should not be imported
        importPackages.removeAll(bundlePackages);

        attrs.putValue(IMPORT_PACKAGE, buildVersionedImportPackage(importPackages));

        // Discover plugin classes from properties file
        final List<String> pluginClasses = discoverPluginClasses(sourceJar);
        if (!pluginClasses.isEmpty()) {
            attrs.putValue(DATAPREPPER_PLUGIN_CLASSES, String.join(",", pluginClasses));
            attrs.putValue(BUNDLE_ACTIVATOR, LegacyPluginBundleActivator.class.getName());
        }

        // Export all packages found in the JAR
        if (!bundlePackages.isEmpty()) {
            attrs.putValue(EXPORT_PACKAGE, String.join(",", bundlePackages));
        }

        return manifest;
    }

    /**
     * Builds the Import-Package header value with semver-compatible version ranges
     * for Data Prepper API packages. The slf4j package is imported without a
     * Data Prepper version range (it uses its own versioning scheme).
     *
     * @param importPackages the list of packages to import (already filtered)
     * @return the formatted Import-Package header value
     */
    static String buildVersionedImportPackage(final List<String> importPackages) {
        final String apiVersion = DataPrepperApiVersion.resolveVersion();
        final String importRange = DataPrepperApiVersion.computeImportRange(apiVersion);

        final List<String> formatted = new ArrayList<>(importPackages.size());
        for (final String pkg : importPackages) {
            if (DataPrepperOsgiPackages.SHARED_API_PACKAGES.contains(pkg)) {
                formatted.add(pkg + ";version=\"" + importRange + "\"");
            } else {
                // Non-API packages (e.g. slf4j) — no Data Prepper version range
                formatted.add(pkg);
            }
        }
        return String.join(",", formatted);
    }

    private List<String> discoverPluginClasses(final JarFile jarFile) throws IOException {
        final List<String> classes = new ArrayList<>();
        final ZipEntry propsEntry = jarFile.getEntry("META-INF/data-prepper.plugins.properties");
        if (propsEntry != null) {
            try (InputStream is = jarFile.getInputStream(propsEntry)) {
                final Properties props = new Properties();
                props.load(is);
                final String packages = props.getProperty("org.opensearch.dataprepper.plugin.packages", "");
                if (!packages.isEmpty()) {
                    // Store the package names — actual class discovery happens at bundle activation
                    for (final String pkg : packages.split(",")) {
                        if (!pkg.trim().isEmpty()) {
                            classes.add(pkg.trim());
                        }
                    }
                }
            }
        }
        return classes;
    }

    private List<String> discoverPackages(final JarFile jarFile) {
        final Set<String> packages = new TreeSet<>();
        final Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            final JarEntry entry = entries.nextElement();
            final String name = entry.getName();
            if (name.endsWith(".class") && name.contains("/")) {
                final String pkg = name.substring(0, name.lastIndexOf('/')).replace('/', '.');
                packages.add(pkg);
            }
        }
        return new ArrayList<>(packages);
    }

    private static String sanitizeName(final String name) {
        return name.replaceAll("[^a-zA-Z0-9.]", ".");
    }
}
