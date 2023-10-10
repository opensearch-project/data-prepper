/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.model;

import java.util.Map;

public class LoadStatus {

    private static final String TOTAL_FILES = "totalFiles";
    private static final String LOADED_FILES = "loadedFiles";
    private static final String TOTAL_RECORDS = "totalRecords";
    private static final String LOADED_RECORDS = "loadedRecords";

    private int totalFiles;

    private int loadedFiles;

    private int totalRecords;

    private int loadedRecords;

    public LoadStatus(int totalFiles, int loadedFiles, int totalRecords, int loadedRecords) {
        this.totalFiles = totalFiles;
        this.loadedFiles = loadedFiles;
        this.totalRecords = totalRecords;
        this.loadedRecords = loadedRecords;
    }

    public int getTotalFiles() {
        return totalFiles;
    }

    public void setTotalFiles(int totalFiles) {
        this.totalFiles = totalFiles;
    }

    public int getLoadedFiles() {
        return loadedFiles;
    }

    public void setLoadedFiles(int loadedFiles) {
        this.loadedFiles = loadedFiles;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    public int getLoadedRecords() {
        return loadedRecords;
    }

    public void setLoadedRecords(int loadedRecords) {
        this.loadedRecords = loadedRecords;
    }

    public Map<String, Object> toMap() {
        return Map.of(
                TOTAL_FILES, totalFiles,
                LOADED_FILES, loadedFiles,
                TOTAL_RECORDS, totalRecords,
                LOADED_RECORDS, loadedRecords
        );
    }

    public static LoadStatus fromMap(Map<String, Object> map) {
        return new LoadStatus(
                (int) map.get(TOTAL_FILES),
                (int) map.get(LOADED_FILES),
                (int) map.get(TOTAL_RECORDS),
                (int) map.get(LOADED_RECORDS)
        );
    }
}
