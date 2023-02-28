/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import java.util.HashMap;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.stream.InMemoryAccumulator;
import org.opensearch.dataprepper.plugins.sink.stream.LocalFileAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-order to process bulk records, records splits into numStreams &
 * eventsPerChunk. numStreams & eventsPerChunk depends on numEvents provided by
 * user in pipelines.yaml eventsPerChunk will be always 20, only numStreams will
 * be vary based on numEvents.
 * 
 * numEvents(event_count) must be always divided by 100 completely without any
 * remnant.
 * 
 * Ex. 1) if numEvents = 100  then numStreams = 2 and eventsPerChunk = 50 
 *     2) if numEvents = 1000 then numStreams = 20 and eventsPerChunk = 50
 */
public class S3SinkWorker implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(S3SinkWorker.class);
	private static final float LOAD_FACTOR = 0.02f;
	private static final String IN_MEMORY = "in_memory";
	private static final String LOCAL_FILE = "local_file";
	private final int numEvents;
	private int numStreams;
	private final int eventsPerChunk;
	private final S3SinkService s3SinkService;
	private final S3SinkConfig s3SinkConfig;
	private final Codec codec;

	/**
	 * 
	 * @param s3SinkService
	 * @param s3SinkConfig
	 */
	public S3SinkWorker(S3SinkService s3SinkService, S3SinkConfig s3SinkConfig, Codec codec) {
		this.numEvents = s3SinkConfig.getThresholdOptions().getEeventCount();
		this.numStreams = (int) (numEvents * LOAD_FACTOR);
		this.eventsPerChunk = numEvents / numStreams;
		this.s3SinkService = s3SinkService;
		this.s3SinkConfig = s3SinkConfig;
		this.codec = codec;
	}

	@Override
	public void run() {
		try {
			while (!S3Sink.isStopRequested()) {
				if (s3SinkConfig.getTemporaryStorage().equalsIgnoreCase(IN_MEMORY)) {
					inMemmoryAccumulator();
				} else {
					localFileAccumulator();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Exception in S3SinkWorker : \n Error message {} \n Exception cause {}", e.getMessage(),
					e.getCause(), e);
		}
	}

	/**
	 * Accumulates data from buffer and store into in memory
	 */
	public void inMemmoryAccumulator() {
		HashSet<Event> inMemoryEventSet = null;
		HashMap<Integer, HashSet<Event>> inMemoryEventMap = null;
		try {
			StopWatch watch = new StopWatch();
			watch.start();
			int streamCount = 0;
			int byteCount = 0;
			int eventCount = 0;
			long eventCollectionDuration = 0;
			inMemoryEventMap = new HashMap<>(numStreams);
			for (int stream = 0; stream < numStreams; stream++) {
				inMemoryEventSet = new HashSet<>(eventsPerChunk);
				boolean flag = Boolean.FALSE;
				for (int data = 0; data < eventsPerChunk
						&& thresholdsCheck(eventCount, watch, byteCount); data++, eventCount++) {
					Event event = S3Sink.getEventQueue().take();
					inMemoryEventSet.add(event);
					byteCount += event.toJsonString().getBytes().length;
					flag = Boolean.TRUE;
					eventCollectionDuration = watch.getTime(TimeUnit.SECONDS);
				}
				if (flag) {
					inMemoryEventMap.put(stream, inMemoryEventSet);
					streamCount++;
				} else {
					// Once threshold reached then No more streaming required per snapshot, hence
					// terminate the streaming(outer) loop
					break;
				}
			}

			LOG.info(
					"In-Memory snapshot info : Byte_count = {} Bytes \t Event_count = {} Records \t Event_collection_duration = {} sec & \t Number of stream {}",
					byteCount, eventCount, eventCollectionDuration, streamCount);

			new InMemoryAccumulator(inMemoryEventMap, streamCount, s3SinkService, s3SinkConfig).doAccumulate();
		} catch (Exception e) {
			LOG.error("Exception while storing recoreds into In-Memory", e);
		}
	}

	/**
	 * Accumulates data from buffer and store in local file
	 */
	public void localFileAccumulator() {
		DB db = null;
		NavigableSet<String> localFileEventSet = null;
		int byteCount = 0;
		int eventCount = 0;
		long eventCollectionDuration = 0;
		try {
			StopWatch watch = new StopWatch();
			watch.start();
			db = DBMaker.memoryDB().make();
			localFileEventSet = db.treeSet("mySet").serializer(Serializer.STRING).createOrOpen();
			for (int data = 0; thresholdsCheck(data, watch, byteCount); data++) {
				String event = S3Sink.getEventQueue().take().toJsonString();
				byteCount += event.getBytes().length;
				localFileEventSet.add(event);
				eventCount++;
				eventCollectionDuration = watch.getTime(TimeUnit.SECONDS);
			}
			db.commit();
			LOG.info(
					"Local-File snapshot info : Byte_count = {} Bytes, \t Event_count = {} Records  \n & Event_collection_duration = {} Sec",
					byteCount, eventCount, eventCollectionDuration);

			new LocalFileAccumulator(localFileEventSet, s3SinkService, s3SinkConfig).doAccumulate();

		} catch (Exception e) {
			LOG.error("Exception while storing recoreds into Local-file", e);
		} finally {
			if (db !=null && !db.isClosed()) {
				db.close();
			}
		}
	}

	/**
	 * Bunch of events based on thresholds set in the configuration. The Thresholds
	 * such as events count, bytes capacity and data collection duration.
	 * 
	 * @param i
	 * @param watch
	 * @param byteCount
	 * @return
	 */
	private boolean thresholdsCheck(int eventCount, StopWatch watch, int byteCount) {
		boolean flag = Boolean.FALSE;
		flag = eventCount < numEvents
				&& watch.getTime(TimeUnit.SECONDS) < s3SinkConfig.getThresholdOptions().getEventCollectionDuration()
				&& byteCount < s3SinkConfig.getThresholdOptions().getByteCapacity();
		return flag;
	}
}
