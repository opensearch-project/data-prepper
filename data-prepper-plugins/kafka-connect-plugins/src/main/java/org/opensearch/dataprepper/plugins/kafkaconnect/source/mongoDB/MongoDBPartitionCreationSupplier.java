/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MongoDBPartitionCreationSupplier implements Function<Map<String, Object>, List<PartitionIdentifier>>  {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBPartitionCreationSupplier.class);
    private static final String DOCUMENTDB_PARTITION_KEY_FORMAT = "%s|%s|%s"; // partition format: <db.collection>|<gte>|<lt>
    private static final String GLOBAL_STATE_PARTITIONED_COLLECTION_KEY = "partitionedCollections";
    private final MongoDBConfig mongoDBConfig;

    public MongoDBPartitionCreationSupplier(final MongoDBConfig mongoDBConfig) {
        this.mongoDBConfig = mongoDBConfig;
    }

    @Override
    public List<PartitionIdentifier> apply(final Map<String, Object> globalStateMap) {
        Map<String, Object> partitionedCollections = (Map<String, Object>) globalStateMap.getOrDefault(GLOBAL_STATE_PARTITIONED_COLLECTION_KEY, new HashMap<>());
        List<String> collectionsToInitPartitions = this.getCollectionsToInitPartitions(mongoDBConfig, partitionedCollections);

        if (collectionsToInitPartitions.isEmpty()) {
            return Collections.emptyList();
        }

        final List<PartitionIdentifier> allPartitionIdentifiers = collectionsToInitPartitions
                .parallelStream()
                .flatMap(collectionName -> {
                    List<PartitionIdentifier> partitions = this.buildPartitions(collectionName);
                    partitionedCollections.put(collectionName, Instant.now().toEpochMilli());
                    return partitions.stream();
                })
                .collect(Collectors.toList());

        globalStateMap.put(GLOBAL_STATE_PARTITIONED_COLLECTION_KEY, partitionedCollections);
        return allPartitionIdentifiers;
    }

    private List<String> getCollectionsToInitPartitions(final MongoDBConfig mongoDBConfig,
                                                        final Map<String, Object> partitionedCollections) {
        return mongoDBConfig.getCollections()
                .stream()
                .map(MongoDBConfig.CollectionConfig::getCollectionName)
                .filter(collectionName -> !partitionedCollections.containsKey(collectionName))
                .collect(Collectors.toList());
    }

    private List<PartitionIdentifier> buildPartitions(final String collectionName) {
        List<PartitionIdentifier> collectionPartitions = new ArrayList<>();
        List<String> collection = List.of(collectionName.split("\\."));
        if (collection.size() < 2) {
            throw new IllegalArgumentException("Invalid Collection Name. Must as db.collection format");
        }
        MongoClient mongoClient = MongoDBHelper.getMongoClient(mongoDBConfig);
        MongoDatabase db = mongoClient.getDatabase(collection.get(0));
        MongoCollection<Document> col = db.getCollection(collection.get(1));

        long totalCount = col.countDocuments();
        long chunkSize = 6000L;
        long startIndex = 0;
        long endIndex = startIndex + chunkSize - 1;

        while (startIndex < totalCount) {
            MongoCursor<Document> cursor = col.find()
                    .projection(new Document("_id", 1))
                    .sort(new Document("_id", 1))
                    .skip((int) startIndex)
                    .limit(1)
                    .iterator();

            Document firstDoc = cursor.hasNext() ? cursor.next() : null;

            // Get second doc
            MongoCursor<Document> secondCursor = col.find()
                        .projection(new Document("_id", 1))
                        .sort(new Document("_id", 1))
                        .skip((int)endIndex)
                        .limit(1)
                        .iterator();
            if (!secondCursor.hasNext()) {
                // this means we have reached the end of the doc
                secondCursor = col.find()
                        .projection(new Document("_id", 1))
                        .sort(new Document("_id", -1))
                        .limit(1)
                        .iterator();
            }
            Document secondDoc = secondCursor.hasNext() ? secondCursor.next() : null;

            if (firstDoc != null && secondDoc != null) {
                Object gteValue = firstDoc.get("_id");
                Object lteValue = secondDoc.get("_id");

                LOG.info("Chunk of " + collectionName + ": {gte: " + gteValue.toString() + ", lte: " + lteValue.toString() + "}");
                collectionPartitions.add(
                        PartitionIdentifier
                                .builder()
                                .withPartitionKey(String.format(DOCUMENTDB_PARTITION_KEY_FORMAT, collectionName, gteValue, lteValue))
                                .build());
            }
            startIndex = endIndex + 1;
            endIndex = startIndex + chunkSize - 1;
        }
        return collectionPartitions;
    }
}
