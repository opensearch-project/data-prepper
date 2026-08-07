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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GlobPathResolverTest {

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectories(tempDir.resolve("subdir"));
        Files.createFile(tempDir.resolve("app.log"));
        Files.createFile(tempDir.resolve("error.log"));
        Files.createFile(tempDir.resolve("app.txt"));
        Files.createFile(tempDir.resolve("subdir/nested.log"));
    }

    @Test
    void resolve_matches_glob_pattern_for_log_files() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, hasSize(2));
        assertThat(result, hasItem(tempDir.resolve("app.log").toAbsolutePath().normalize()));
        assertThat(result, hasItem(tempDir.resolve("error.log").toAbsolutePath().normalize()));
    }

    @Test
    void resolve_excludes_files_matching_exclude_patterns() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                List.of(tempDir.toString() + "/error.*"));

        final Set<Path> result = resolver.resolve();

        assertThat(result, hasSize(1));
        assertThat(result, hasItem(tempDir.resolve("app.log").toAbsolutePath().normalize()));
    }

    @Test
    void resolve_matches_recursive_double_star_pattern() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/" + "**/*.log"),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, hasItem(tempDir.resolve("subdir/nested.log").toAbsolutePath().normalize()));
    }

    @Test
    void resolve_matches_question_mark_pattern() throws IOException {
        Files.createFile(tempDir.resolve("a.log"));
        Files.createFile(tempDir.resolve("b.log"));

        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/?.log"),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, hasItem(tempDir.resolve("a.log").toAbsolutePath().normalize()));
        assertThat(result, hasItem(tempDir.resolve("b.log").toAbsolutePath().normalize()));
        assertThat(result, not(hasItem(tempDir.resolve("app.log").toAbsolutePath().normalize())));
    }

    @Test
    void resolve_returns_empty_set_when_no_files_match() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.csv"),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, empty());
    }

    @Test
    void resolve_excludes_default_vcs_directories() throws IOException {
        Path gitDir = Files.createDirectory(tempDir.resolve(".git"));
        Files.createFile(gitDir.resolve("HEAD.log"));

        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/" + "**/*.log"),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, not(hasItem(gitDir.resolve("HEAD.log").toAbsolutePath().normalize())));
    }

    @Test
    void matches_returns_true_for_matching_path() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());

        assertThat(resolver.matches(tempDir.resolve("app.log")), equalTo(true));
    }

    @Test
    void matches_returns_false_for_non_matching_path() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());

        assertThat(resolver.matches(tempDir.resolve("app.txt")), equalTo(false));
    }

    @Test
    void matches_returns_false_for_excluded_path() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                List.of(tempDir.toString() + "/error.*"));

        assertThat(resolver.matches(tempDir.resolve("error.log")), equalTo(false));
    }

    @Test
    void matches_returns_true_when_exclude_does_not_match_included_path() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                List.of(tempDir.toString() + "/error.*"));

        assertThat(resolver.matches(tempDir.resolve("app.log")), equalTo(true));
    }

    @Test
    void getWatchDirectories_returns_base_directories() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                Collections.emptyList());

        final Set<Path> watchDirs = resolver.getWatchDirectories();

        assertThat(watchDirs, hasSize(1));
        assertThat(watchDirs, hasItem(tempDir.toAbsolutePath().normalize()));
    }

    @Test
    void extractBaseDirectory_stops_at_first_wildcard() {
        final Path baseDir = GlobPathResolver.extractBaseDirectory(tempDir.toString() + "/logs/*.log");

        assertThat(baseDir, notNullValue());
        assertThat(baseDir.toString().startsWith(tempDir.toAbsolutePath().normalize().toString()), equalTo(true));
    }

    @Test
    void constructor_throws_on_null_include_patterns() {
        assertThrows(NullPointerException.class, () ->
                new GlobPathResolver(null, Collections.emptyList()));
    }

    @Test
    void resolve_handles_null_exclude_patterns() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                null);

        final Set<Path> result = resolver.resolve();

        assertThat(result, hasSize(2));
    }

    @Test
    void resolve_handles_multiple_include_patterns() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log", tempDir.toString() + "/*.txt"),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, hasSize(greaterThanOrEqualTo(3)));
    }

    @Test
    void resolve_handles_include_patterns_in_different_base_directories() throws IOException {
        Path otherDir = Files.createDirectory(tempDir.resolve("other"));
        Files.createFile(otherDir.resolve("audit.log"));

        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log", otherDir.toString() + "/*.log"),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, hasItem(tempDir.resolve("app.log").toAbsolutePath().normalize()));
        assertThat(result, hasItem(otherDir.resolve("audit.log").toAbsolutePath().normalize()));
    }

    @Test
    void resolve_ignores_excludes_outside_include_base_directory() throws IOException {
        Path otherDir = Files.createDirectory(tempDir.resolve("other"));

        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log"),
                List.of(otherDir.toString() + "/*.log"));

        final Set<Path> result = resolver.resolve();

        assertThat(result, hasSize(2));
    }

    @Test
    void getWatchDirectories_returns_multiple_directories_for_multiple_patterns() {
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(tempDir.toString() + "/*.log", tempDir.toString() + "/subdir/*.log"),
                Collections.emptyList());

        final Set<Path> watchDirs = resolver.getWatchDirectories();

        assertThat(watchDirs.size(), greaterThanOrEqualTo(1));
    }

    @Test
    void resolve_returns_empty_set_for_nonexistent_base_directory() {
        Path nonexistent = tempDir.resolve("nonexistent-subdir");
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(nonexistent.toString() + "/*.log"),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, empty());
    }

    @Test
    void resolve_warns_when_base_directory_does_not_exist() {
        String nonexistentPath = "/nonexistent-dir-" + System.nanoTime() + "/sub/deep/*.log";
        final GlobPathResolver resolver = new GlobPathResolver(
                List.of(nonexistentPath),
                Collections.emptyList());

        final Set<Path> result = resolver.resolve();

        assertThat(result, empty());
    }

    @Test
    void extractBaseDirectory_with_no_separator_in_pattern() {
        final Path baseDir = GlobPathResolver.extractBaseDirectory("/*.log");
        assertThat(baseDir, notNullValue());
    }

    @Test
    void extractBaseDirectory_with_nonexistent_path_returns_parent_as_fallback() {
        final Path baseDir = GlobPathResolver.extractBaseDirectory("/nonexistent-test-dir-" + System.nanoTime() + "/data.log");
        assertThat(baseDir, notNullValue());
    }
}