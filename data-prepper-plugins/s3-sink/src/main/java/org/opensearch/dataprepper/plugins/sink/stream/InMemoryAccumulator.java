/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.stream;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.S3ObjectIndex;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.S3SinkService;
import org.opensearch.dataprepper.plugins.sink.SinkAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;
/**
 * Accumulates data from buffer and store into in memory
 */
public class InMemoryAccumulator implements SinkAccumulator {

	private static final Logger LOG = LoggerFactory.getLogger(InMemoryAccumulator.class);
	final Map<Integer, HashSet<Event>> inMemoryEventMap;
	private final int numStreams;
	private final S3SinkService s3SinkService;
	private final S3SinkConfig s3SinkConfig;
	private boolean retry = Boolean.FALSE;
	private static final int MAX_RETRY = 3;
	private static final int PART_SIZE_COUNT = 5;

	/**
	 * 
	 * @param inMemoryEventMap
	 * @param numStreams
	 * @param s3SinkService
	 * @param s3SinkConfig
	 */
	public InMemoryAccumulator(final Map<Integer, HashSet<Event>> inMemoryEventMap, final int numStreams,
			final S3SinkService s3SinkService, final S3SinkConfig s3SinkConfig) {
		this.inMemoryEventMap = inMemoryEventMap;
		this.numStreams = numStreams;
		this.s3SinkService = s3SinkService;
		this.s3SinkConfig = s3SinkConfig;
	}

	@Override
	public void doAccumulate() {

		String bucket = s3SinkConfig.getBucketName();
		String path = s3SinkConfig.getKeyPathPrefix();
		String index = S3ObjectIndex.getIndexAliasWithDate(s3SinkConfig.getObjectOptions().getFilePattern());
		String key = path + "/" + index;

		S3Client client = s3SinkService.getS3Client();
		int retryCount = MAX_RETRY;
		
		LOG.info("S3-Sink will caeate Amazon S3 object : {}", key);

		do {
			try {
				// Setting up
				final StreamTransferManager manager = new StreamTransferManager(bucket, key, client, path)
						.numStreams(numStreams).numUploadThreads(2).queueCapacity(2).partSize(PART_SIZE_COUNT);
				final List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();

				ExecutorService pool = Executors.newFixedThreadPool(numStreams);
				for (int streamsInput = 0; streamsInput < numStreams; streamsInput++) {
					final int streamIndex = streamsInput;
					HashSet<Event> eventSet = inMemoryEventMap.get(streamsInput);
					pool.submit(new Runnable() {
						public void run() {
							try {
								MultiPartOutputStream outputStream = streams.get(streamIndex);
								if (eventSet != null) {
									for (Event event : eventSet) {
										outputStream.write(event.toJsonString().getBytes());
									}
								}
								// The stream must be closed once all the data has been written
								outputStream.close();
							} catch (Exception e) {
								// Aborts all uploads
								retry = Boolean.TRUE;
								manager.abort(e);
								LOG.error("Aborts all uploads = {}", manager, e);
							}
						}
					});
				}
				pool.shutdown();
				sleep(pool);
				// Finishing off
				manager.complete();

			} catch (Exception e) {
				retry = Boolean.TRUE;
				LOG.error("Issue with the streaming recoreds via s3 client : \n Error message {} \n Exception cause {}", e.getMessage(), e.getCause(), e);
			}

			if (retryCount == 0) {
				retry = Boolean.FALSE;
				LOG.warn("Maximum retry count 3 reached, Unable to store {} into Amazon S3", key);
			}
			if (retry) {
				LOG.info("Retry : {}", (MAX_RETRY - --retryCount));
				sleep(null);
			}
		} while (retry);
	}

	private void sleep(ExecutorService pool) {
		try {
			if (pool != null) {
				pool.awaitTermination(5, TimeUnit.SECONDS);
			} else {
				Thread.sleep(5000);
			}

		} catch (InterruptedException e) {
			LOG.error("InterruptedException - \n Error message {} \n Exception cause {}", e.getMessage(), e.getCause());
		}
	}

}
