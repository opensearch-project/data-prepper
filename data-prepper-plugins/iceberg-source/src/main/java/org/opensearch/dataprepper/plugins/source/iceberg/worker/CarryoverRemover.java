/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.iceberg.worker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Removes carryover rows from a list of changelog rows.
 * Carryover = identical DELETE + INSERT pairs produced by CoW file rewrites.
 *
 * Algorithm (matches Spark's RemoveCarryoverIterator):
 * 1. Sort all rows by data columns, with DELETE before INSERT for equal rows
 * 2. Walk the sorted list, skip adjacent DELETE+INSERT pairs with identical data
 */
public class CarryoverRemover {

    /**
     * A changelog row: the data columns as a comparable list, plus the operation type.
     */
    public static class ChangelogRow {
        private final List<Object> dataColumns;
        private final String operation; // "INSERT" or "DELETE"
        private final int originalIndex;

        public ChangelogRow(final List<Object> dataColumns, final String operation, final int originalIndex) {
            this.dataColumns = dataColumns;
            this.operation = operation;
            this.originalIndex = originalIndex;
        }

        public List<Object> getDataColumns() {
            return dataColumns;
        }

        public String getOperation() {
            return operation;
        }

        public int getOriginalIndex() {
            return originalIndex;
        }
    }

    /**
     * Removes carryover pairs from the given rows.
     * Returns the indices (in the original list) of rows that are NOT carryover.
     */
    public List<Integer> removeCarryover(final List<ChangelogRow> rows) {
        if (rows.isEmpty()) {
            return List.of();
        }

        rows.sort(Comparator
                .<ChangelogRow, String>comparing(r -> dataColumnsToString(r.getDataColumns()))
                .thenComparing(r -> "DELETE".equals(r.getOperation()) ? 0 : 1));

        final List<Integer> result = new ArrayList<>();
        int i = 0;
        while (i < rows.size()) {
            if (i + 1 < rows.size()
                    && "DELETE".equals(rows.get(i).getOperation())
                    && "INSERT".equals(rows.get(i + 1).getOperation())
                    && dataColumnsEqual(rows.get(i).getDataColumns(), rows.get(i + 1).getDataColumns())) {
                i += 2; // skip carryover pair
            } else {
                result.add(rows.get(i).getOriginalIndex());
                i++;
            }
        }
        return result;
    }

    private boolean dataColumnsEqual(final List<Object> a, final List<Object> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i) == null && b.get(i) == null) {
                continue;
            }
            if (a.get(i) == null || !a.get(i).equals(b.get(i))) {
                return false;
            }
        }
        return true;
    }

    private String dataColumnsToString(final List<Object> columns) {
        final StringBuilder sb = new StringBuilder();
        for (final Object col : columns) {
            sb.append(col == null ? "\0null\0" : col.toString()).append("\1");
        }
        return sb.toString();
    }
}
