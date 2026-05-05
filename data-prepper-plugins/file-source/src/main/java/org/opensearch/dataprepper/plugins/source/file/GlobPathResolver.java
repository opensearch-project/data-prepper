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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

public final class GlobPathResolver {

    private static final Logger LOG = LoggerFactory.getLogger(GlobPathResolver.class);
    private static final String WILDCARD_CHARS = "*?{[";

    private final List<String> includePatterns;
    private final List<String> excludePatterns;
    private final List<PathMatcher> includeMatchers;
    private final List<PathMatcher> excludeMatchers;

    public GlobPathResolver(final List<String> includePatterns, final List<String> excludePatterns) {
        this.includePatterns = Objects.requireNonNull(includePatterns, "includePatterns must not be null");
        this.excludePatterns = excludePatterns != null ? excludePatterns : Collections.emptyList();
        this.includeMatchers = buildMatchers(this.includePatterns);
        this.excludeMatchers = buildMatchers(this.excludePatterns);
    }

    public Set<Path> resolve() {
        final Set<Path> result = new HashSet<>();
        final Set<Path> baseDirectories = getWatchDirectories();

        for (final Path baseDir : baseDirectories) {
            if (!Files.isDirectory(baseDir)) {
                LOG.warn("Base directory does not exist or is not a directory: {}", baseDir);
                continue;
            }
            walkDirectory(baseDir, result);
        }

        return result;
    }

    void walkDirectory(final Path baseDir, final Set<Path> result) {
        walkDirectory(baseDir, createFileVisitor(result));
    }

    void walkDirectory(final Path baseDir, final SimpleFileVisitor<Path> visitor) {
        try {
            Files.walkFileTree(baseDir, visitor);
        } catch (final IOException e) {
            LOG.warn("Failed to walk directory tree at: {}", baseDir, e);
        }
    }

    SimpleFileVisitor<Path> createFileVisitor(final Set<Path> result) {
        return new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
                final Path normalized = file.toAbsolutePath().normalize();
                if (matches(normalized)) {
                    result.add(normalized);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(final Path file, final IOException exc) {
                LOG.warn("Failed to access file during glob resolution: {}", file, exc);
                return FileVisitResult.CONTINUE;
            }
        };
    }

    public boolean matches(final Path path) {
        final Path normalized = path.toAbsolutePath().normalize();

        boolean included = false;
        for (final PathMatcher matcher : includeMatchers) {
            if (matcher.matches(normalized)) {
                included = true;
                break;
            }
        }

        if (!included) {
            return false;
        }

        for (final PathMatcher matcher : excludeMatchers) {
            if (matcher.matches(normalized)) {
                return false;
            }
        }

        return true;
    }

    public Set<Path> getWatchDirectories() {
        final Set<Path> directories = new HashSet<>();
        for (final String pattern : includePatterns) {
            final Path baseDir = extractBaseDirectory(pattern);
            directories.add(baseDir);
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

    private static List<PathMatcher> buildMatchers(final List<String> patterns) {
        final List<PathMatcher> matchers = new ArrayList<>(patterns.size());
        for (final String pattern : patterns) {
            try {
                final String absolutePattern = Paths.get(pattern).toAbsolutePath().normalize().toString();
                matchers.add(FileSystems.getDefault().getPathMatcher("glob:" + absolutePattern));
            } catch (final PatternSyntaxException | InvalidPathException e) {
                LOG.error("Invalid glob pattern '{}': {}", pattern, e.getMessage());
                throw new IllegalArgumentException("Invalid glob pattern: " + pattern, e);
            }
        }
        return matchers;
    }
}
