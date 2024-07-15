/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.Map;

public class LoadStatus {

    private static final String TOTAL_FILES = "totalFiles";
    private static final String LOADED_FILES = "loadedFiles";

    private int totalFiles;

    private int loadedFiles;

    public LoadStatus(int totalFiles, int loadedFiles) {
        this.totalFiles = totalFiles;
        this.loadedFiles = loadedFiles;
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

    public Map<String, Object> toMap() {
        return Map.of(
                TOTAL_FILES, totalFiles,
                LOADED_FILES, loadedFiles
        );
    }

    public static LoadStatus fromMap(Map<String, Object> map) {
        return new LoadStatus(
                ((Number) map.get(TOTAL_FILES)).intValue(),
                ((Number) map.get(LOADED_FILES)).intValue()
        );
    }
}
