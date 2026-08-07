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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Removes carryover rows from a list of changelog rows.
 * Carryover = identical DELETE + INSERT pairs produced by CoW file rewrites.
 *
 * Algorithm (aligned with Spark's RemoveCarryoverIterator):
 * 1. Sort all rows by data columns, with DELETE before INSERT for equal rows
 * 2. Walk the sorted list, track consecutive DELETE count, cancel with matching INSERTs
 */
public class CarryoverRemover {

    public static class ChangelogRow {
        private final List<Object> dataColumns;
        private final String operation;
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
                .<ChangelogRow, ComparableColumns>comparing(r -> new ComparableColumns(r.getDataColumns()))
                .thenComparing(r -> "DELETE".equals(r.getOperation()) ? 0 : 1));

        final List<Integer> result = new ArrayList<>();
        int i = 0;
        while (i < rows.size()) {
            final ChangelogRow current = rows.get(i);
            if (!"DELETE".equals(current.getOperation())) {
                result.add(current.getOriginalIndex());
                i++;
                continue;
            }

            // Current row is DELETE. Count consecutive identical DELETE rows.
            int deleteCount = 1;
            int j = i + 1;
            while (j < rows.size()
                    && "DELETE".equals(rows.get(j).getOperation())
                    && dataColumnsEqual(current.getDataColumns(), rows.get(j).getDataColumns())) {
                deleteCount++;
                j++;
            }

            // Cancel DELETE rows with matching INSERT rows
            while (j < rows.size()
                    && "INSERT".equals(rows.get(j).getOperation())
                    && dataColumnsEqual(current.getDataColumns(), rows.get(j).getDataColumns())
                    && deleteCount > 0) {
                deleteCount--;
                j++;
            }

            // Emit remaining uncancelled DELETE rows
            for (int k = 0; k < deleteCount; k++) {
                result.add(rows.get(i + k).getOriginalIndex());
            }

            i = j;
        }
        return result;
    }

    private boolean dataColumnsEqual(final List<Object> a, final List<Object> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!Objects.equals(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Comparable wrapper for column-by-column comparison, used as sort key.
     */
    private static class ComparableColumns implements Comparable<ComparableColumns> {
        private final List<Object> columns;

        ComparableColumns(final List<Object> columns) {
            this.columns = columns;
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compareTo(final ComparableColumns other) {
            final int len = Math.min(columns.size(), other.columns.size());
            for (int i = 0; i < len; i++) {
                final Object a = columns.get(i);
                final Object b = other.columns.get(i);
                if (a == null && b == null) {
                    continue;
                }
                if (a == null) {
                    return -1;
                }
                if (b == null) {
                    return 1;
                }
                final int cmp;
                if (a instanceof Comparable && b instanceof Comparable) {
                    cmp = ((Comparable<Object>) a).compareTo(b);
                } else {
                    cmp = a.toString().compareTo(b.toString());
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(columns.size(), other.columns.size());
        }
    }
}
