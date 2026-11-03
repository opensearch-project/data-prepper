/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * State for a WriteResultPartition. Contains the table identifier and
 * Base64-encoded ManifestFile metadata for data and delete manifests.
 * The actual data/delete file metadata is stored in manifest files on storage,
 * not in the coordination store.
 */
public class WriteResultState {

    @JsonProperty("tableIdentifier")
    private String tableIdentifier;

    @JsonProperty("dataManifest")
    private String dataManifest;

    @JsonProperty("deleteManifest")
    private String deleteManifest;

    @JsonProperty("commitSequence")
    private Long commitSequence;

    @JsonProperty("subIndex")
    private Integer subIndex;

    public WriteResultState() {
    }

    public WriteResultState(final String tableIdentifier,
                            final String dataManifest,
                            final String deleteManifest) {
        this.tableIdentifier = tableIdentifier;
        this.dataManifest = dataManifest;
        this.deleteManifest = deleteManifest;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    /** Base64-encoded ManifestFile for data files, or null if no data files */
    public String getDataManifest() {
        return dataManifest;
    }

    /** Base64-encoded ManifestFile for delete files, or null if no delete files */
    public String getDeleteManifest() {
        return deleteManifest;
    }

    public Long getCommitSequence() {
        return commitSequence;
    }

    public void setCommitSequence(final Long commitSequence) {
        this.commitSequence = commitSequence;
    }

    public Integer getSubIndex() {
        return subIndex;
    }

    public void setSubIndex(final Integer subIndex) {
        this.subIndex = subIndex;
    }
}
