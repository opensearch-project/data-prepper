package org.opensearch.dataprepper.plugins.source.source_crawler.model;


import java.time.Instant;
import java.util.Map;


public interface ItemInfo {

    /**
     * Use this field to store primary item of a repository. Primary item of a repository is something
     * which can be fetched/queried/obtained from source service just using its item ID.
     */
    String getItemId();

    /**
     * Use this field to store items metadata. Item metadata can be any information other than item
     * contents itself which can be used to apply regex filtering, change data capture etc. general
     * assumption here is that fetching metadata should be faster than fetching entire Item
     */
    Map<String, Object> getMetadata();

    /**
     * Process your change log events serially (preferably in a single thread) and ensure that you are
     * applying monotonously increasing timeStamp. If you don't do that, then SDK could miss latest
     * updates as it processes events out of order and it relies on this member to decide which change
     * log events to keep and which ones to discard.
     */
    Instant getEventTime();

    String getPartitionKey();

    /**
     * Service specific Unique Id of this Item.
     *
     * @return String value indicating unique id of this item.
     */
    String getId();

    /**
     * Key attributes related to this Item.
     *
     * @return A map of key attributes of this Item.
     */
    Map<String, String> getKeyAttributes();

    /**
     * Service specific Item's last modified time
     *
     * @return Instant when this item was created
     */
    Instant getLastModifiedAt();
}
