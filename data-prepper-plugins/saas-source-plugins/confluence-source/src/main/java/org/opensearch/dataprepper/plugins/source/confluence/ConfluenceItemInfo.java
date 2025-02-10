/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceItem;
import org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceContentType;
import org.opensearch.dataprepper.plugins.source.confluence.utils.Constants;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.CONTENT_ID;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.CONTENT_TITLE;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.LAST_MODIFIED;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.SPACE_KEY;

@Setter
@Getter
public class ConfluenceItemInfo implements ItemInfo {
    private String space;
    // either a page or a BlogPost
    private String contentType;
    private String id;
    private String itemId;
    private Map<String, Object> metadata;
    private Instant eventTime;

    public ConfluenceItemInfo(String id,
                              String itemId,
                              String space,
                              String contentType,
                              Map<String, Object> metadata,
                              Instant eventTime
    ) {
        this.id = id;
        this.space = space;
        this.contentType = contentType;
        this.itemId = itemId;
        this.metadata = metadata;
        this.eventTime = eventTime;
    }

    public static ConfluenceItemInfoBuilder builder() {
        return new ConfluenceItemInfoBuilder();
    }

    @Override
    public String getPartitionKey() {
        return space + "|" + contentType + "|" + UUID.randomUUID();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Map<String, Object> getKeyAttributes() {
        return Map.of(Constants.SPACE, space);
    }

    @Override
    public Instant getLastModifiedAt() {
        long updatedAtMillis = getMetadataField(Constants.LAST_MODIFIED);
        long createdAtMillis = getMetadataField(Constants.CREATED);
        return createdAtMillis > updatedAtMillis ?
                Instant.ofEpochMilli(createdAtMillis) : Instant.ofEpochMilli(updatedAtMillis);
    }

    private Long getMetadataField(String fieldName) {
        Object value = this.metadata.get(fieldName);
        if (value == null) {
            return 0L;
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (Exception e) {
                return 0L;
            }
        }
        return 0L;
    }

    public static class ConfluenceItemInfoBuilder {
        private Map<String, Object> metadata;
        private Instant eventTime;
        private String id;
        private String itemId;
        private String space;
        private String contentType;

        public ConfluenceItemInfoBuilder() {
        }

        public ConfluenceItemInfo build() {
            return new ConfluenceItemInfo(id, itemId, space, contentType, metadata, eventTime);
        }

        public ConfluenceItemInfoBuilder withMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public ConfluenceItemInfoBuilder withEventTime(Instant eventTime) {
            this.eventTime = eventTime;
            return this;
        }

        public ConfluenceItemInfoBuilder withItemId(String itemId) {
            this.itemId = itemId;
            return this;
        }

        public ConfluenceItemInfoBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public ConfluenceItemInfoBuilder withSpace(String space) {
            this.space = space;
            return this;
        }

        public ConfluenceItemInfoBuilder withContentBean(ConfluenceItem contentItem) {
            Map<String, Object> contentItemMetadata = new HashMap<>();
            contentItemMetadata.put(SPACE_KEY, contentItem.getSpaceItem().getKey());
            contentItemMetadata.put(CONTENT_TITLE, contentItem.getTitle());
            contentItemMetadata.put(CREATED, contentItem.getCreatedTimeMillis());
            contentItemMetadata.put(LAST_MODIFIED, contentItem.getUpdatedTimeMillis());
            contentItemMetadata.put(CONTENT_ID, contentItem.getId());
            contentItemMetadata.put(ConfluenceService.CONTENT_TYPE, ConfluenceContentType.PAGE.getType());

            this.space = contentItem.getSpaceItem().getKey();
            this.id = contentItem.getId();
            this.contentType = ConfluenceContentType.PAGE.getType();
            this.itemId = contentItem.getId();
            this.metadata = contentItemMetadata;
            return this;
        }
    }

}