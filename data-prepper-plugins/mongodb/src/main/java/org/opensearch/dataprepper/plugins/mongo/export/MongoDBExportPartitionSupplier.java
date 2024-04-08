/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.plugins.mongo.client.BsonHelper;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.mongo.model.PartitionIdentifierBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class MongoDBExportPartitionSupplier implements Function<ExportPartition, PartitionIdentifierBatch> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBExportPartitionSupplier.class);
    private static final String MONGODB_PARTITION_KEY_FORMAT = "%s|%s|%s|%s"; // partition format: <db.collection>|<gte>|<lt>|<className>
    private static final String COLLECTION_SPLITTER = "\\.";
    public static final String EXPORT_PREFIX = "EXPORT-";

    private final MongoDBSourceConfig sourceConfig;

    public MongoDBExportPartitionSupplier(final MongoDBSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    private PartitionIdentifierBatch buildPartitions(final ExportPartition exportPartition) {
        final List<PartitionIdentifier> collectionPartitions = new ArrayList<>();
        final String collectionDbName = exportPartition.getCollection();
        List<String> collection = List.of(collectionDbName.split(COLLECTION_SPLITTER));
        if (collection.size() < 2) {
            throw new IllegalArgumentException("Invalid Collection Name. Must be in db.collection format");
        }
        final Optional<ExportProgressState> exportProgressStateOptional = exportPartition
                .getProgressState();
        final Object lastEndDocId = exportProgressStateOptional.map(
                ExportProgressState::getLastEndDocId).orElse(null);
        boolean isLastBatch = false;
        Object endDocId = null;
        try (MongoClient mongoClient = MongoDBConnection.getMongoClient(sourceConfig)) {
            final MongoDatabase db = mongoClient.getDatabase(collection.get(0));
            final MongoCollection<Document> col = db.getCollection(collection.get(1));
            final int partitionSize = exportPartition.getPartitionSize();
            FindIterable<Document> startIterable;
            if (lastEndDocId != null) {
                startIterable = col.find(Filters.gt("_id", lastEndDocId))
                        .projection(new Document("_id", 1))
                        .sort(new Document("_id", 1))
                        .limit(1);
            } else {
                startIterable = col.find()
                        .projection(new Document("_id", 1))
                        .sort(new Document("_id", 1))
                        .limit(1);
            }
            while (!Thread.currentThread().isInterrupted()) {
                try (final MongoCursor<Document> startCursor = startIterable.iterator()) {
                    if (!startCursor.hasNext()) {
                        isLastBatch = true;
                        break;
                    }
                    final Document startDoc = startCursor.next();
                    final Object gteValue = startDoc.get("_id");
                    final String className = gteValue.getClass().getName();

                    // Get end doc
                    Document endDoc = startIterable.skip(partitionSize - 1).limit(1).first();
                    if (endDoc == null) {
                        // this means we have reached the end of the doc
                        endDoc = col.find()
                                .projection(new Document("_id", 1))
                                .sort(new Document("_id", -1))
                                .limit(1)
                                .first();
                    }

                    final Object lteValue = endDoc.get("_id");
                    endDocId = lteValue;
                    final String gteValueString = BsonHelper.getPartitionStringFromMongoDBId(gteValue, className);
                    final String lteValueString = BsonHelper.getPartitionStringFromMongoDBId(lteValue, className);
                    LOG.info("Partition of " + collectionDbName + ": {gte: " + gteValueString + ", lte: " + lteValueString + "}");
                    collectionPartitions.add(
                            PartitionIdentifier
                                    .builder()
                                    .withPartitionKey(String.format(MONGODB_PARTITION_KEY_FORMAT, collectionDbName, gteValueString, lteValueString, className))
                                    .build());

                    startIterable = col.find(Filters.gt("_id", lteValue))
                            .projection(new Document("_id", 1))
                            .sort(new Document("_id", 1))
                            .limit(1);
                } catch (Exception e) {
                    LOG.error("Failed to read start cursor when build partitions", e);
                    throw new RuntimeException(e);
                }
            }
        }
        return new PartitionIdentifierBatch(collectionPartitions, isLastBatch, endDocId);
    }

    @Override
    public PartitionIdentifierBatch apply(final ExportPartition exportPartition) {
        return buildPartitions(exportPartition);
    }
}
