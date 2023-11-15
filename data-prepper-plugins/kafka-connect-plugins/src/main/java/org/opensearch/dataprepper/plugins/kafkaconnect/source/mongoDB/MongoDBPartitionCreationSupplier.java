/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
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

public class MongoDBPartitionCreationSupplier implements Function<Map<String, Object>, List<PartitionIdentifier>> {
    public static final String GLOBAL_STATE_PARTITIONED_COLLECTION_KEY = "partitionedCollections";
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBPartitionCreationSupplier.class);
    private static final String DOCUMENTDB_PARTITION_KEY_FORMAT = "%s|%s|%s|%s"; // partition format: <db.collection>|<gte>|<lt>|<className>
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
        try (MongoClient mongoClient = MongoDBHelper.getMongoClient(mongoDBConfig)) {
            MongoDatabase db = mongoClient.getDatabase(collection.get(0));
            MongoCollection<Document> col = db.getCollection(collection.get(1));
            int chunkSize = this.mongoDBConfig.getExportConfig().getItemsPerPartition();
            FindIterable<Document> startIterable = col.find()
                    .projection(new Document("_id", 1))
                    .sort(new Document("_id", 1))
                    .limit(1);
            while (true) {
                try (MongoCursor<Document> startCursor = startIterable.iterator()) {
                    if (!startCursor.hasNext()) {
                        break;
                    }
                    Document startDoc = startCursor.next();
                    Object gteValue = startDoc.get("_id");
                    String className = gteValue.getClass().getName();

                    // Get end doc
                    Document endDoc = startIterable.skip(chunkSize - 1).limit(1).first();
                    if (endDoc == null) {
                        // this means we have reached the end of the doc
                        endDoc = col.find()
                                .projection(new Document("_id", 1))
                                .sort(new Document("_id", -1))
                                .limit(1)
                                .first();
                    }
                    if (endDoc == null) {
                        break;
                    }

                    Object lteValue = endDoc.get("_id");
                    LOG.info("Chunk of " + collectionName + ": {gte: " + gteValue.toString() + ", lte: " + lteValue.toString() + "}");
                    collectionPartitions.add(
                            PartitionIdentifier
                                    .builder()
                                    .withPartitionKey(String.format(DOCUMENTDB_PARTITION_KEY_FORMAT, collectionName, gteValue, lteValue, className))
                                    .build());

                    startIterable = col.find(Filters.gt("_id", lteValue))
                            .projection(new Document("_id", 1))
                            .sort(new Document("_id", 1))
                            .limit(1);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return collectionPartitions;
    }
}
