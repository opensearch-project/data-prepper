/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.MongoClientException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
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
    private static final Bson ID_PROJECTION = Projections.include("_id");
    private static final Bson ID_ASC = Sorts.ascending("_id");
    private static final Bson ID_DESC = Sorts.descending("_id");

    private final MongoDBSourceConfig sourceConfig;
    private final EnhancedSourceCoordinator enhancedSourceCoordinator;
    private final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics;

    public MongoDBExportPartitionSupplier(final MongoDBSourceConfig sourceConfig,
                                          final EnhancedSourceCoordinator enhancedSourceCoordinator,
                                          final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics) {
        this.sourceConfig = sourceConfig;
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.documentDBAggregateMetrics = documentDBAggregateMetrics;
    }

    /**
     * Detects whether the collection has a uniform _id type by checking the first and last documents.
     * If uniform, we can use a simple Filters.gt() instead of the complex $or query across all BSON types.
     */
    boolean isUniformIdType(final MongoCollection<Document> col) {
        final Document first = col.find().projection(ID_PROJECTION).sort(ID_ASC).limit(1).first();
        final Document last = col.find().projection(ID_PROJECTION).sort(ID_DESC).limit(1).first();
        if (first == null || last == null) {
            return true;
        }
        final String firstType = first.get("_id").getClass().getName();
        final String lastType = last.get("_id").getClass().getName();
        if (BsonHelper.isClassNumber(firstType) && BsonHelper.isClassNumber(lastType)) {
            return true;
        }
        return firstType.equals(lastType);
    }

    private Bson buildNextStartFilter(final Object lastLteValue, final String lteClassName, final boolean uniformType) {
        if (uniformType) {
            return Filters.gt("_id", lastLteValue);
        }
        final String lteValueString = BsonHelper.getPartitionStringFromMongoDBId(lastLteValue, lteClassName);
        return buildGtQuery(lteValueString, lteClassName, MAX_KEY);
    }

    private void addPartition(final List<PartitionIdentifier> partitions, final String collectionDbName,
                              final Object gteValue, final String gteClassName,
                              final Object lteValue, final String lteClassName) {
        final String gteValueString = BsonHelper.getPartitionStringFromMongoDBId(gteValue, gteClassName);
        final String lteValueString = BsonHelper.getPartitionStringFromMongoDBId(lteValue, lteClassName);
        LOG.debug("Partition of {} : { gte: {} class: {}, lte: {} class {} }",
                collectionDbName, gteValueString, gteClassName, lteValueString, lteClassName);
        partitions.add(PartitionIdentifier.builder()
                .withPartitionKey(String.format(MONGODB_PARTITION_KEY_FORMAT,
                        collectionDbName, gteValueString, lteValueString, gteClassName, lteClassName))
                .build());
    }

    private PartitionIdentifierBatch buildPartitions(final ExportPartition exportPartition) {
        documentDBAggregateMetrics.getExportApiInvocations().increment();
        final List<PartitionIdentifier> collectionPartitions = new ArrayList<>();
        final String collectionDbName = exportPartition.getCollection();
        final List<String> collection = List.of(collectionDbName.split(COLLECTION_SPLITTER));
        if (collection.size() < 2) {
            documentDBAggregateMetrics.getExport4xxErrors().increment();
            throw new IllegalArgumentException("Invalid Collection Name. Must be in db.collection format");
        }

        final Optional<ExportProgressState> exportProgressStateOptional = exportPartition.getProgressState();
        final Object lastEndDocId = exportProgressStateOptional.map(ExportProgressState::getLastEndDocId).orElse(null);
        boolean isLastBatch = false;
        Object endDocId = lastEndDocId;

        try (MongoClient mongoClient = MongoDBConnection.getMongoClient(sourceConfig)) {
            final MongoDatabase db = mongoClient.getDatabase(collection.get(0));
            final MongoCollection<Document> col = db.getCollection(
                    collectionDbName.substring(collection.get(0).length() + 1));
            final int partitionSize = exportPartition.getPartitionSize();

            final boolean uniformType = isUniformIdType(col);
            LOG.info("Collection {} has {} _id type. Using {} partition query strategy.",
                    collectionDbName, uniformType ? "uniform" : "mixed", uniformType ? "simple $gt" : "$or-based");

            Bson startFilter;
            if (lastEndDocId != null) {
                startFilter = Filters.gt("_id", lastEndDocId);
            } else {
                startFilter = new Document();
            }

            while (!Thread.currentThread().isInterrupted()) {
                final Document startDoc = col.find(startFilter)
                        .projection(ID_PROJECTION)
                        .sort(ID_ASC)
                        .limit(1)
                        .first();

                if (startDoc == null) {
                    LOG.info("No records to process or has reached end of the export partition.");
                    isLastBatch = true;
                    break;
                }

                final Object gteValue = startDoc.get("_id");
                final String gteClassName = gteValue.getClass().getName();

                final Document endOfPageDoc = col.find(Filters.gte("_id", gteValue))
                        .projection(ID_PROJECTION)
                        .sort(ID_ASC)
                        .skip(partitionSize - 1)
                        .limit(1)
                        .first();

                final Object lteValue;
                final String lteClassName;

                if (endOfPageDoc == null) {
                    final Document lastDoc = col.find()
                            .projection(ID_PROJECTION)
                            .sort(ID_DESC)
                            .limit(1)
                            .first();
                    if (lastDoc == null) {
                        isLastBatch = true;
                        break;
                    }
                    lteValue = lastDoc.get("_id");
                    lteClassName = lteValue.getClass().getName();
                    isLastBatch = true;
                } else {
                    lteValue = endOfPageDoc.get("_id");
                    lteClassName = lteValue.getClass().getName();
                }

                endDocId = lteValue;
                addPartition(collectionPartitions, collectionDbName, gteValue, gteClassName, lteValue, lteClassName);
                documentDBAggregateMetrics.getExportPartitionQueryCount().increment();

                if (isLastBatch) {
                    break;
                }

                // extend the ownership of the partition
                enhancedSourceCoordinator.saveProgressStateForPartition(exportPartition, null);

                startFilter = buildNextStartFilter(lteValue, lteClassName, uniformType);
            }
        } catch (final IllegalArgumentException | MongoClientException e) {
            // IllegalArgumentException is thrown when database or collection name is not valid
            // MongoClientException is thrown for exceptions indicating a failure condition with the MongoClient
            documentDBAggregateMetrics.getExport4xxErrors().increment();
            LOG.error("Client side exception while building partitions.", e);
            throw new RuntimeException(e);
        } catch (final Exception e) {
            documentDBAggregateMetrics.getExport5xxErrors().increment();
            LOG.error("Server side exception while building partitions.", e);
            throw new RuntimeException(e);
        }

        return new PartitionIdentifierBatch(collectionPartitions, isLastBatch, endDocId);
    }

    @Override
    public PartitionIdentifierBatch apply(final ExportPartition exportPartition) {
        return buildPartitions(exportPartition);
    }
}
