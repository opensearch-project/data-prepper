/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.worker;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

class CarryoverRemoverTest {

    private final CarryoverRemover carryoverRemover = new CarryoverRemover();

    @Test
    void removeCarryover_emptyList_returnsEmpty() {
        final List<Integer> result = carryoverRemover.removeCarryover(new ArrayList<>());
        assertThat(result, hasSize(0));
    }

    @Test
    void removeCarryover_insertOnly_returnsAll() {
        final List<CarryoverRemover.ChangelogRow> rows = List.of(
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "INSERT", 0),
                new CarryoverRemover.ChangelogRow(List.of(2, "Bob", 25), "INSERT", 1)
        );
        final List<Integer> result = carryoverRemover.removeCarryover(new ArrayList<>(rows));
        assertThat(result, hasSize(2));
    }

    @Test
    void removeCarryover_deleteOnly_returnsAll() {
        final List<CarryoverRemover.ChangelogRow> rows = List.of(
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "DELETE", 0)
        );
        final List<Integer> result = carryoverRemover.removeCarryover(new ArrayList<>(rows));
        assertThat(result, hasSize(1));
    }

    @Test
    void removeCarryover_identicalDeleteInsertPair_removedAsCarryover() {
        final List<CarryoverRemover.ChangelogRow> rows = List.of(
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "DELETE", 0),
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "INSERT", 1)
        );
        final List<Integer> result = carryoverRemover.removeCarryover(new ArrayList<>(rows));
        assertThat(result, hasSize(0));
    }

    @Test
    void removeCarryover_updateProducesDeleteAndInsertWithDifferentValues() {
        final List<CarryoverRemover.ChangelogRow> rows = List.of(
                new CarryoverRemover.ChangelogRow(List.of(2, "Bob", 25), "DELETE", 0),
                new CarryoverRemover.ChangelogRow(List.of(2, "Bobby", 25), "INSERT", 1)
        );
        final List<Integer> result = carryoverRemover.removeCarryover(new ArrayList<>(rows));
        assertThat(result, hasSize(2));
    }

    @Test
    void removeCarryover_mixedCarryoverAndActualChanges() {
        // CoW UPDATE: file with 3 rows rewritten, 1 row changed
        final List<CarryoverRemover.ChangelogRow> rows = List.of(
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "DELETE", 0),
                new CarryoverRemover.ChangelogRow(List.of(2, "Bob", 25), "DELETE", 1),
                new CarryoverRemover.ChangelogRow(List.of(3, "Carol", 35), "DELETE", 2),
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "INSERT", 3),
                new CarryoverRemover.ChangelogRow(List.of(2, "Bobby", 25), "INSERT", 4),
                new CarryoverRemover.ChangelogRow(List.of(3, "Carol", 35), "INSERT", 5)
        );
        final List<Integer> result = carryoverRemover.removeCarryover(new ArrayList<>(rows));
        // Only id=2 (Bob->Bobby) should survive
        assertThat(result, hasSize(2));
    }

    @Test
    void removeCarryover_deleteWithNoMatchingInsert_survives() {
        // DELETE id=4 with no corresponding INSERT (actual delete)
        final List<CarryoverRemover.ChangelogRow> rows = List.of(
                new CarryoverRemover.ChangelogRow(List.of(3, "Carol", 35), "DELETE", 0),
                new CarryoverRemover.ChangelogRow(List.of(4, "Dave", 28), "DELETE", 1),
                new CarryoverRemover.ChangelogRow(List.of(3, "Carol", 35), "INSERT", 2)
        );
        final List<Integer> result = carryoverRemover.removeCarryover(new ArrayList<>(rows));
        // Only id=4 DELETE should survive
        assertThat(result, hasSize(1));
    }

    @Test
    void removeCarryover_nullValues_handledCorrectly() {
        final List<CarryoverRemover.ChangelogRow> rows = new ArrayList<>(List.of(
                new CarryoverRemover.ChangelogRow(Arrays.asList(1, "Alice", null), "DELETE", 0),
                new CarryoverRemover.ChangelogRow(Arrays.asList(1, "Alice", null), "INSERT", 1)
        ));
        final List<Integer> result = carryoverRemover.removeCarryover(rows);
        assertThat(result, hasSize(0));
    }

    @Test
    void removeCarryover_nullToNonNull_notCarryover() {
        // Schema evolution: null email -> non-null email
        final List<CarryoverRemover.ChangelogRow> rows = new ArrayList<>(List.of(
                new CarryoverRemover.ChangelogRow(Arrays.asList(3, "Carol", 35, null), "DELETE", 0),
                new CarryoverRemover.ChangelogRow(Arrays.asList(3, "Carol", 35, "carol@example.com"), "INSERT", 1)
        ));
        final List<Integer> result = carryoverRemover.removeCarryover(rows);
        assertThat(result, hasSize(2));
    }

    @Test
    void removeCarryover_duplicateIdenticalRows_removedInPairs() {
        // Two identical rows exist in the table (legitimate duplicates)
        // After sort: DELETE, DELETE, INSERT, INSERT
        // First DELETE+DELETE are adjacent (not a pair), then INSERT+INSERT
        // Algorithm pairs: (DELETE[0], INSERT[2]) via adjacent check after sort
        // Actually: DELETE, DELETE, INSERT, INSERT -> (DELETE, INSERT) pair removed, (DELETE, INSERT) pair removed
        // Wait - sort puts DELETE before INSERT for same data, so:
        // Sorted: D(Alice), D(Alice), I(Alice), I(Alice)
        // i=0: D + D -> not a pair (both DELETE), advance to i=1
        // i=1: D + I -> pair! skip both, i=3
        // i=3: I -> no pair, add to result
        // Result: 2 rows (first DELETE and last INSERT)
        final List<CarryoverRemover.ChangelogRow> rows = new ArrayList<>(List.of(
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "DELETE", 0),
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "DELETE", 1),
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "INSERT", 2),
                new CarryoverRemover.ChangelogRow(List.of(1, "Alice", 30), "INSERT", 3)
        ));
        final List<Integer> result = carryoverRemover.removeCarryover(rows);
        // D, D, I, I -> (D stays), (D+I pair removed), (I stays) = 2 remaining
        assertThat(result, hasSize(2));
    }
}
