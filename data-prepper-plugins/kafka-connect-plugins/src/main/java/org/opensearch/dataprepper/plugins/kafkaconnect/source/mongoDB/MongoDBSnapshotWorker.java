/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public class MongoDBSnapshotWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSnapshotWorker.class);
    private static final Duration BACKOFF_ON_EXCEPTION = Duration.ofSeconds(60);
    private static final Duration BACKOFF_ON_EMPTY_PARTITION = Duration.ofSeconds(60);
    private final SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator;
    private final MongoDBPartitionCreationSupplier mongoDBPartitionCreationSupplier;

    public MongoDBSnapshotWorker(final SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator,
                                 final MongoDBPartitionCreationSupplier mongoDBPartitionCreationSupplier) {
        this.sourceCoordinator = sourceCoordinator;
        this.mongoDBPartitionCreationSupplier = mongoDBPartitionCreationSupplier;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final Optional<SourcePartition<MongoDBSnapshotProgressState>> snapshotPartition = sourceCoordinator.getNextPartition(mongoDBPartitionCreationSupplier);
                if (snapshotPartition.isEmpty()) {
                    try {
                        LOG.info("get empty partition");
                        Thread.sleep(BACKOFF_ON_EMPTY_PARTITION.toMillis());
                        continue;
                    } catch (final InterruptedException e) {
                        LOG.info("The PitWorker was interrupted while sleeping after acquiring no indices to process, stopping processing");
                        return;
                    }
                }
//        MongoDatabase testDB = mongoClient.getDatabase("test");
//        MongoCollection<Document> numbersCollection = testDB.getCollection("orders");
//
//        MongoCursor<Document> cursor = numbersCollection.find().iterator();
//        final int timeout = 5_000;
//        try {
//            while (cursor.hasNext()) {
//                System.out.println(cursor.next().toJson());
//                buffer.write(getEventRecordFromData(cursor.next().toJson()), timeout);
//            }
//        } catch (TimeoutException e) {
//            throw new RuntimeException(e);
//        } finally {
//            cursor.close();
//        }
                LOG.info("get partition success {}", snapshotPartition.get().getPartitionKey());
                try {
                    Thread.sleep(BACKOFF_ON_EMPTY_PARTITION.toMillis());
                    continue;
                } catch (final InterruptedException e) {
                    LOG.info("The PitWorker was interrupted while sleeping after acquiring no indices to process, stopping processing");
                    return;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while trying to snapshot documentDB, backing off and retrying", e);
                try {
                    Thread.sleep(BACKOFF_ON_EXCEPTION.toMillis());
                } catch (final InterruptedException ex) {
                    LOG.info("The DocumentDBSnapshotWorker was interrupted before backing off and retrying, stopping processing");
                    return;
                }
            }
        }
    }
}
