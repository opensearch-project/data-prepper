/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gradle.plugin.osgi;

import aQute.bnd.gradle.BundleTaskExtension;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.bundling.Jar;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Gradle plugin that prepares a Data Prepper plugin JAR for OSGi consumption
 * by generating an OSGi-compliant MANIFEST.MF at build time using the bnd tool.
 * <p>
 * This plugin eliminates the need for runtime JAR-to-bundle adaptation (previously
 * done by {@code BundleAdapter}) by baking all required OSGi headers into the JAR
 * during the build. When applied, it:
 * <ul>
 *   <li>Applies the {@code biz.aQute.bnd.builder} plugin to the project</li>
 *   <li>Configures the jar task's bnd instructions to produce a valid OSGi bundle</li>
 *   <li>Reads {@code META-INF/data-prepper.plugins.properties} from the project's
 *       resources to determine the {@code DataPrepper-Plugin-Classes} header value</li>
 * </ul>
 * <p>
 * <b>Usage:</b> Apply this plugin in a Data Prepper plugin project's {@code build.gradle}:
 * <pre>
 * plugins {
 *     id 'org.opensearch.dataprepper.osgi'
 * }
 * </pre>
 * <p>
 * The consuming project must contain a {@code META-INF/data-prepper.plugins.properties} file
 * in its resources with the property {@code org.opensearch.dataprepper.plugin.packages}
 * listing the plugin's package names (comma-separated). If this file is absent, the
 * build will fail with a clear error message.
 * <p>
 * <b>Generated Manifest Headers:</b>
 * <ul>
 *   <li>{@code Bundle-SymbolicName}: {@code org.opensearch.dataprepper.plugin.<sanitized-project-name>}</li>
 *   <li>{@code Bundle-Version}: project version normalized to OSGi format (SNAPSHOT stripped)</li>
 *   <li>{@code Export-Package}: all project packages except internal ones (computed by bnd)</li>
 *   <li>{@code Import-Package}: computed by bnd from bytecode analysis</li>
 *   <li>{@code Bundle-Activator}: {@code org.opensearch.dataprepper.plugin.osgi.LegacyPluginBundleActivator}</li>
 *   <li>{@code DataPrepper-Plugin-Classes}: comma-separated package names from properties file</li>
 * </ul>
 *
 * @see <a href="https://github.com/opensearch-project/data-prepper/issues/6760">Issue #6760</a>
 */
public class DataPrepperOsgiPlugin implements Plugin<Project> {

    /**
     * Plugin ID for registration and external consumption.
     */
    public static final String PLUGIN_ID = "org.opensearch.dataprepper.osgi";

    private static final String BND_BUILDER_PLUGIN_ID = "biz.aQute.bnd.builder";
    private static final String PROPERTIES_RESOURCE_PATH = "META-INF/data-prepper.plugins.properties";
    private static final String PLUGIN_PACKAGES_PROPERTY = "org.opensearch.dataprepper.plugin.packages";
    private static final String BUNDLE_ACTIVATOR_CLASS =
            "org.opensearch.dataprepper.plugin.osgi.LegacyPluginBundleActivator";

    /**
     * Pattern matching a Maven/Gradle version string: major.minor.micro[-qualifier]
     */
    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:[.-](.+))?");

    @Override
    public void apply(final Project project) {
        project.getPluginManager().apply(JavaPlugin.class);
        project.getPluginManager().apply(BND_BUILDER_PLUGIN_ID);

        final Logger logger = project.getLogger();

        // Use afterEvaluate so that the consuming project's source sets and version are fully resolved
        project.afterEvaluate(p -> configureBndBundle(p, logger));
    }

    private void configureBndBundle(final Project project, final Logger logger) {
        final String symbolicName = computeSymbolicName(project);
        final String bundleVersion = toOsgiVersion(project.getVersion().toString());
        final String pluginClasses = resolvePluginClasses(project, logger);

        // Determine the project's own packages for export.
        // Use the plugin packages property as the base package prefix for export.
        final String exportPattern = pluginClasses != null && !pluginClasses.isEmpty()
                ? pluginClasses.replace(",", ".*,") + ".*"
                : "*";

        // Build the bnd instructions string.
        // Export-Package is limited to the plugin's own packages (avoids fat-jar behavior).
        // Import-Package uses bnd's default bytecode analysis to compute dependencies.
        final StringBuilder bndInstructions = new StringBuilder();
        bndInstructions.append("Bundle-SymbolicName: ").append(symbolicName).append('\n');
        bndInstructions.append("Bundle-Version: ").append(bundleVersion).append('\n');
        bndInstructions.append("Export-Package: !*.internal.*,").append(exportPattern).append(";-noimport:=true").append('\n');
        bndInstructions.append("Import-Package: *").append('\n');
        bndInstructions.append("Private-Package: ").append(exportPattern).append('\n');

        if (pluginClasses != null && !pluginClasses.isEmpty()) {
            bndInstructions.append("Bundle-Activator: ").append(BUNDLE_ACTIVATOR_CLASS).append('\n');
            bndInstructions.append("DataPrepper-Plugin-Classes: ").append(pluginClasses).append('\n');
        }

        // Configure via the jar task's bundle extension (added by biz.aQute.bnd.builder)
        final Jar jarTask = (Jar) project.getTasks().getByName("jar");
        final BundleTaskExtension bundleExtension = jarTask.getExtensions().findByType(BundleTaskExtension.class);
        if (bundleExtension == null) {
            throw new GradleException("The 'bundle' extension (BundleTaskExtension) was not found on the jar task. "
                    + "Ensure the biz.aQute.bnd.builder plugin is properly applied.");
        }

        bundleExtension.bnd(bndInstructions.toString());

        logger.lifecycle("Data Prepper OSGi plugin: configured bundle '{}' version '{}'",
                symbolicName, bundleVersion);
    }

    /**
     * Computes the OSGi Bundle-SymbolicName from the project.
     * <p>
     * Strategy: {@code org.opensearch.dataprepper.plugin.<sanitized-project-name>}
     * <p>
     * This matches BundleAdapter's scheme where the symbolic name was
     * {@code org.opensearch.dataprepper.plugin.<sanitized-jar-base-name>}.
     * At build time we use the Gradle project name (which by convention matches
     * the JAR base name).
     *
     * @param project the Gradle project
     * @return the symbolic name string
     */
    static String computeSymbolicName(final Project project) {
        return "org.opensearch.dataprepper.plugin." + sanitizeName(project.getName());
    }

    /**
     * Converts a Maven/Gradle version string to a valid OSGi version.
     * <ul>
     *   <li>{@code "2.16.0-SNAPSHOT"} becomes {@code "2.16.0"}</li>
     *   <li>{@code "2.16.0"} stays {@code "2.16.0"}</li>
     *   <li>{@code "2.16"} becomes {@code "2.16.0"}</li>
     * </ul>
     * The SNAPSHOT qualifier is stripped entirely because OSGi versions
     * do not support SNAPSHOT ordering semantics. This mirrors the logic
     * in {@code DataPrepperOsgiPackages.toOsgiVersion()}.
     *
     * @param version the raw version string
     * @return a valid OSGi version string (major.minor.micro or major.minor.micro.qualifier)
     */
    static String toOsgiVersion(final String version) {
        if (version == null || version.isEmpty()) {
            return "0.0.0";
        }

        final Matcher matcher = VERSION_PATTERN.matcher(version);
        if (!matcher.matches()) {
            return "0.0.0";
        }

        final String major = matcher.group(1);
        final String minor = matcher.group(2);
        final String micro = matcher.group(3) != null ? matcher.group(3) : "0";
        final String qualifier = matcher.group(4);

        if (qualifier == null || "SNAPSHOT".equalsIgnoreCase(qualifier)) {
            return major + "." + minor + "." + micro;
        }

        // Convert non-SNAPSHOT qualifier to valid OSGi qualifier (alphanumeric only)
        final String sanitizedQualifier = qualifier.replaceAll("[^a-zA-Z0-9]", "");
        if (sanitizedQualifier.isEmpty()) {
            return major + "." + minor + "." + micro;
        }
        return major + "." + minor + "." + micro + "." + sanitizedQualifier;
    }

    /**
     * Resolves the DataPrepper-Plugin-Classes header value by reading the
     * {@code META-INF/data-prepper.plugins.properties} file from the consuming
     * project's main resource source directories.
     *
     * @param project the Gradle project
     * @param logger  the project logger
     * @return comma-separated package names from the properties file
     * @throws GradleException if the properties file is not found or the required property is missing
     */
    private String resolvePluginClasses(final Project project, final Logger logger) {
        final SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        final SourceSet mainSourceSet = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME);

        for (final File resourceDir : mainSourceSet.getResources().getSrcDirs()) {
            final File propsFile = new File(resourceDir, PROPERTIES_RESOURCE_PATH);
            if (propsFile.exists()) {
                try (FileInputStream fis = new FileInputStream(propsFile)) {
                    final Properties props = new Properties();
                    props.load(fis);
                    final String packages = props.getProperty(PLUGIN_PACKAGES_PROPERTY, "").trim();
                    if (!packages.isEmpty()) {
                        logger.info("Resolved DataPrepper-Plugin-Classes from {}: {}", propsFile, packages);
                        return packages;
                    }
                } catch (final IOException e) {
                    logger.warn("Failed to read {}: {}", propsFile, e.getMessage());
                }
            }
        }

        throw new GradleException(
                "Could not find " + PROPERTIES_RESOURCE_PATH + " in any resource directory of the '"
                        + project.getName() + "' project. "
                        + "This file must exist and contain the property '" + PLUGIN_PACKAGES_PROPERTY
                        + "' listing plugin package names (comma-separated). "
                        + "See the data-prepper-gradle-plugins/osgi-plugin README for details.");
    }

    /**
     * Sanitizes a name for use in an OSGi symbolic name by replacing
     * any character that is not alphanumeric or a dot with a dot.
     * This matches the sanitization logic in {@code BundleAdapter.sanitizeName()}.
     *
     * @param name the raw name
     * @return the sanitized name suitable for OSGi symbolic names
     */
    static String sanitizeName(final String name) {
        return name.replaceAll("[^a-zA-Z0-9.]", ".");
    }
}
