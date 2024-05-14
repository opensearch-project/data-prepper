/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.MongoClientException;
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
import org.opensearch.dataprepper.plugins.mongo.utils.DocumentDBSourceAggregateMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.MAX_KEY;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.buildGtQuery;

public class MongoDBExportPartitionSupplier implements Function<ExportPartition, PartitionIdentifierBatch> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBExportPartitionSupplier.class);
    private static final String MONGODB_PARTITION_KEY_FORMAT = "%s|%s|%s|%s|%s"; // partition format: <db.collection>|<gte>|<lt>|<gteClassName>|<lteClassName>
    private static final String COLLECTION_SPLITTER = "\\.";

    private final MongoDBSourceConfig sourceConfig;
    private final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics;

    public MongoDBExportPartitionSupplier(final MongoDBSourceConfig sourceConfig,
                                          final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics) {
        this.sourceConfig = sourceConfig;
        this.documentDBAggregateMetrics = documentDBAggregateMetrics;
    }

    private PartitionIdentifierBatch buildPartitions(final ExportPartition exportPartition) {
        documentDBAggregateMetrics.getExportApiInvocations().increment();
        final List<PartitionIdentifier> collectionPartitions = new ArrayList<>();
        final String collectionDbName = exportPartition.getCollection();
        List<String> collection = List.of(collectionDbName.split(COLLECTION_SPLITTER));
        if (collection.size() < 2) {
            documentDBAggregateMetrics.getExport4xxErrors().increment();
            throw new IllegalArgumentException("Invalid Collection Name. Must be in db.collection format");
        }
        final Optional<ExportProgressState> exportProgressStateOptional = exportPartition
                .getProgressState();
        final Object lastEndDocId = exportProgressStateOptional.map(
                ExportProgressState::getLastEndDocId).orElse(null);
        boolean isLastBatch = false;
        Object endDocId = lastEndDocId;
        try (MongoClient mongoClient = MongoDBConnection.getMongoClient(sourceConfig)) {
            final MongoDatabase db = mongoClient.getDatabase(collection.get(0));
            final MongoCollection<Document> col = db.getCollection(collectionDbName.substring(collection.get(0).length()+1));
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
                    final String gteClassName = gteValue.getClass().getName();

                    // Get end doc
                    Document endDoc = startIterable.skip(partitionSize - 1).limit(1).first();
                    if (endDoc == null) {
                        // this means we have reached the end of the doc
                        endDoc = col.find()
                                .projection(new Document("_id", 1))
                                .sort(new Document("_id", -1))
                                .limit(1)
                                .first();
                        isLastBatch = true;
                    }

                    final Object lteValue = endDoc.get("_id");
                    final String lteClassName = lteValue.getClass().getName();
                    endDocId = lteValue;
                    final String gteValueString = BsonHelper.getPartitionStringFromMongoDBId(gteValue, gteClassName);
                    final String lteValueString = BsonHelper.getPartitionStringFromMongoDBId(lteValue, lteClassName);
                    LOG.debug("Partition of {} : { gte: {} class: {}, lte: {} class {} }", collectionDbName, gteValueString, gteClassName, lteValueString, lteClassName);
                    collectionPartitions.add(
                        PartitionIdentifier
                            .builder()
                            .withPartitionKey(String.format(MONGODB_PARTITION_KEY_FORMAT, collectionDbName, gteValueString, lteValueString, gteClassName, lteClassName))
                            .build());

                    if (isLastBatch) {
                        break;
                    }

                    startIterable = col.find(buildGtQuery(lteValueString, lteClassName, MAX_KEY))
                            .projection(new Document("_id", 1))
                            .sort(new Document("_id", 1))
                            .limit(1);
                }
            }
        } catch (final IllegalArgumentException | MongoClientException e) {
            // IllegalArgumentException is thrown when database or collection name is not valid
            // MongoClientException is thrown for exceptions indicating a failure condition with the MongoClient
            documentDBAggregateMetrics.getExport4xxErrors().increment();
            LOG.error("Client side exception while build partitions.", e);
            throw new RuntimeException(e);
        } catch (final Exception e) {
            documentDBAggregateMetrics.getExport5xxErrors().increment();
            LOG.error("Server side exception while build partitions.", e);
            throw new RuntimeException(e);
        }

        return new PartitionIdentifierBatch(collectionPartitions, isLastBatch, endDocId);
    }

    @Override
    public PartitionIdentifierBatch apply(final ExportPartition exportPartition) {
        return buildPartitions(exportPartition);
    }
}
