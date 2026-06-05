/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.types.selectors.SelectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class GlobPathResolver {

    private static final Logger LOG = LoggerFactory.getLogger(GlobPathResolver.class);
    private static final String WILDCARD_CHARS = "*?{[";

    private final List<String> includePatterns;
    private final List<String> excludePatterns;

    public GlobPathResolver(final List<String> includePatterns, final List<String> excludePatterns) {
        Objects.requireNonNull(includePatterns, "includePatterns must not be null");
        this.includePatterns = List.copyOf(includePatterns);
        this.excludePatterns = excludePatterns != null
                ? List.copyOf(excludePatterns)
                : Collections.emptyList();
    }

    public Set<Path> resolve() {
        final Set<Path> result = new HashSet<>();
        final Map<Path, List<String>> includesByBase = groupByBaseDirectory(includePatterns);

        for (final Map.Entry<Path, List<String>> entry : includesByBase.entrySet()) {
            final Path baseDir = entry.getKey();
            if (!Files.isDirectory(baseDir)) {
                LOG.warn("Base directory does not exist or is not a directory: {}", baseDir);
                continue;
            }
            final DirectoryScanner scanner = new DirectoryScanner();
            scanner.setBasedir(baseDir.toFile());
            scanner.setIncludes(entry.getValue().toArray(new String[0]));
            final List<String> relativeExcludes = relativizeExcludes(baseDir, excludePatterns);
            if (!relativeExcludes.isEmpty()) {
                scanner.setExcludes(relativeExcludes.toArray(new String[0]));
            }
            scanner.addDefaultExcludes();
            scanner.setErrorOnMissingDir(false);
            scanner.scan();
            for (final String included : scanner.getIncludedFiles()) {
                result.add(baseDir.resolve(included).toAbsolutePath().normalize());
            }
        }
        return result;
    }

    public boolean matches(final Path path) {
        final String normalized = path.toAbsolutePath().normalize().toString();
        boolean included = false;
        for (final String pattern : includePatterns) {
            if (SelectorUtils.matchPath(toAbsolutePattern(pattern), normalized)) {
                included = true;
                break;
            }
        }
        if (!included) {
            return false;
        }
        for (final String pattern : excludePatterns) {
            if (SelectorUtils.matchPath(toAbsolutePattern(pattern), normalized)) {
                return false;
            }
        }
        return true;
    }

    public Set<Path> getWatchDirectories() {
        final Set<Path> directories = new HashSet<>();
        for (final String pattern : includePatterns) {
            directories.add(extractBaseDirectory(pattern));
        }
        return directories;
    }

    static Path extractBaseDirectory(final String pattern) {
        final String normalized = Paths.get(pattern).toAbsolutePath().normalize().toString();
        final StringBuilder staticPrefix = new StringBuilder();

        for (int i = 0; i < normalized.length(); i++) {
            final char c = normalized.charAt(i);
            if (WILDCARD_CHARS.indexOf(c) >= 0) {
                break;
            }
            staticPrefix.append(c);
        }

        String prefix = staticPrefix.toString();
        final int lastSep = prefix.lastIndexOf('/');
        if (lastSep > 0) {
            prefix = prefix.substring(0, lastSep);
        }

        final Path result = Paths.get(prefix).toAbsolutePath().normalize();
        if (Files.isDirectory(result)) {
            return result;
        }
        final Path parent = result.getParent();
        return Objects.requireNonNullElse(parent, result);
    }

    private static Map<Path, List<String>> groupByBaseDirectory(final List<String> patterns) {
        final Map<Path, List<String>> grouped = new LinkedHashMap<>();
        for (final String pattern : patterns) {
            final Path baseDir = extractBaseDirectory(pattern);
            final Path absolute = Paths.get(pattern).toAbsolutePath().normalize();
            grouped.computeIfAbsent(baseDir, k -> new ArrayList<>())
                    .add(baseDir.relativize(absolute).toString());
        }
        return grouped;
    }

    private static List<String> relativizeExcludes(final Path baseDir, final List<String> excludes) {
        final List<String> result = new ArrayList<>(excludes.size());
        for (final String exclude : excludes) {
            final Path absolute = Paths.get(exclude).toAbsolutePath().normalize();
            if (absolute.startsWith(baseDir)) {
                result.add(baseDir.relativize(absolute).toString());
            }
        }
        return result;
    }

    private static String toAbsolutePattern(final String pattern) {
        return Paths.get(pattern).toAbsolutePath().normalize().toString();
    }
}