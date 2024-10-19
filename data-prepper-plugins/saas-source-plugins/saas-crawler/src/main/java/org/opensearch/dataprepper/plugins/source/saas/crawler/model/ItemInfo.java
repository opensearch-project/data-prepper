package org.opensearch.dataprepper.plugins.source.saas.crawler.model;


import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
public abstract class ItemInfo {

    /**
     * Use this field to store primary item of a repository. Primary item of a repository is something
     * which can be fetched/queried/obtained from repository just using its item ID.
     */
    String itemId;

    /**
     * Use this field to store items metadata. Item metadata can be any information other than item
     * contents itself which can be used to apply regex filtering, change data capture etc. general
     * assumption here is that fetching metadata should be faster than fetching entire Item
     */
    Map<String, String> metadata;

    /**
     * Process your change log events serially (preferably in a single thread) and ensure that you are
     * applying monotonously increasing timeStamp. If you don't do that, then SDK could miss latest
     * updates as it processes events out of order and it relies on this member to decide which change
     * log events to keep and which ones to discard.
     */
    @NonNull
    Long eventTime;

    public ItemInfo(String itemId) {
        this.itemId = itemId;
    }

    public ItemInfo(@NonNull String itemId, Map<String, String> metadata, @NonNull Long eventTime) {
        this.itemId = itemId;
        this.metadata = metadata;
        this.eventTime = eventTime;
    }

    public abstract String getPartitionKey();
    public abstract String getId();
    public abstract Map<String, String> getKeyAttributes();
}
