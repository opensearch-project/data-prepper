/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;

import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This service class contains logic for sending data to Prometheus Endpoints
 */
public class PrometheusSinkService {

    private final Lock reentrantLock;

    private final PrometheusSinkConfiguration prometheusSinkConfiguration;

    public PrometheusSinkService(final PrometheusSinkConfiguration prometheusSinkConfiguration){
        this.prometheusSinkConfiguration = prometheusSinkConfiguration;
        this.reentrantLock = new ReentrantLock();
    }

    /**
     * This method process buffer records and send to Prometheus End points based on configured codec
     * @param records Collection of Event
     */
    public void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        try {
            records.forEach(record -> {
                final Event event = record.getData();

                //TODO: call buildRemoteWriteRequest()
                //TODO: compress Remote.WriteRequest to byte[]
                //TODO: write to currentBuffer

                // TODO: threshold check
                // TODO: push current buffer data to prometheus endpoint
                // TODO: implement retry mechanism
                // TODO: push failed Data to DLQ
                });
        }finally {
            reentrantLock.unlock();
        }
    }
}