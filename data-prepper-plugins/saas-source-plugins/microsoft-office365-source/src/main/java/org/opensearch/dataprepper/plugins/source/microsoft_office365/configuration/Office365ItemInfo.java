/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.configuration;

import lombok.Builder;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.time.Instant;
import java.util.Map;

@Builder
public class Office365ItemInfo implements ItemInfo {
    private final String itemId;
    private final Instant eventTime;
    private final Map<String, Object> metadata;
    private final String partitionKey;
    private final Map<String, Object> keyAttributes;
    private final Instant lastModifiedAt;

    @Override
    public String getItemId() {
        return itemId;
    }

    @Override
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public Instant getEventTime() {
        return eventTime;
    }

    @Override
    public String getPartitionKey() {
        return partitionKey;
    }

    @Override
    public String getId() {
        return itemId;
    }

    @Override
    public Map<String, Object> getKeyAttributes() {
        return keyAttributes;
    }

    @Override
    public Instant getLastModifiedAt() {
        return lastModifiedAt;
    }
}
