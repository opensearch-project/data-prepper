/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryAccumulator;
import org.opensearch.dataprepper.plugins.sink.accumulator.LocalFileAccumulator;
import org.opensearch.dataprepper.plugins.sink.accumulator.SinkAccumulator;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
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
 * {@code LOAD_FACTOR} required to divide collections of records (numEvents)
 * into streams
 * 
 * Ex. 1) if numEvents = 100 then numStreams = 2 and eventsPerChunk = 50 
 *     2) if numEvents = 1000 then numStreams = 20 and eventsPerChunk = 50
 */
public class S3SinkWorker {

	private static final Logger LOG = LoggerFactory.getLogger(S3SinkWorker.class);
	/**
	 * {@code LOAD_FACTOR} required to divide collections of records into streams
	 */
	private static final float LOAD_FACTOR = 0.02f;
	private final S3SinkService s3SinkService;
	private final S3SinkConfig s3SinkConfig;
	private final Codec codec;
	private SinkAccumulator accumulator;
	private final int numEvents;
	private final ByteCount byteCapacity;
	private final long duration;
	private final int numStreams;
	private final int eventsPerChunk;
	
	/**
	 * @param s3SinkService
	 * @param s3SinkConfig
	 */
	public S3SinkWorker(final S3SinkService s3SinkService, final S3SinkConfig s3SinkConfig, final Codec codec) {
		this.s3SinkService = s3SinkService;
		this.s3SinkConfig = s3SinkConfig;
		this.codec = codec;
		numEvents = s3SinkConfig.getThresholdOptions().getEventCount();
		byteCapacity = s3SinkConfig.getThresholdOptions().getByteCapacity();
		duration = s3SinkConfig.getThresholdOptions().getEventCollectionDuration().getSeconds();
		
		numStreams = (int) (numEvents * LOAD_FACTOR);
		eventsPerChunk = numEvents / numStreams;
	}

	/**
	 * Accumulates data from buffer and store into in memory
	 */
	public SinkAccumulator inMemmoryAccumulator() {
		HashSet<String> inMemoryEventSet = null;
		HashMap<Integer, HashSet<String>> inMemoryEventMap = null;
		int streamCount = 0;
		try {
			StopWatch watch = new StopWatch();
			watch.start();
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
					OutputStream outPutStream = new ByteArrayOutputStream();
					codec.parse(outPutStream, event);
					inMemoryEventSet.add(outPutStream.toString());
					byteCount += event.toJsonString().getBytes().length;
					flag = Boolean.TRUE;
					eventCollectionDuration = watch.getTime(TimeUnit.SECONDS);
				}
				if (flag) {
					inMemoryEventMap.put(stream, inMemoryEventSet);
					streamCount++;
				} else {
					// Once threshold reached then No more streaming required per snapshot, hence
					// stop the streaming(outer) loop
					break;
				}
			}

			LOG.info(
					"In-Memory snapshot info : Byte_count = {} Bytes "
					+ "\t Event_count = {} Records "
					+ "\t Event_collection_duration = {} sec & "
					+ "\t Number of stream {}",
					byteCount, eventCount, eventCollectionDuration, streamCount);

			//accumulator = new InMemoryAccumulator(inMemoryEventMap, streamCount, s3SinkService, s3SinkConfig);
		} catch (Exception e) {
			LOG.error("Exception while storing recoreds into In-Memory", e);
		}

		return accumulator;
	}

	/**
	 * Accumulates data from buffer and store in local file
	 */
	public SinkAccumulator localFileAccumulator() {
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
				Event event = S3Sink.getEventQueue().take();
				OutputStream outPutStream = new ByteArrayOutputStream();
				codec.parse(outPutStream, event);
				byteCount += event.toJsonString().getBytes().length;
				localFileEventSet.add(outPutStream.toString());
				eventCount++;
				eventCollectionDuration = watch.getTime(TimeUnit.SECONDS);
			}
			db.commit();
			LOG.info(
					"Local-File snapshot info : Byte_count = {} Bytes, "
					+ "\t Event_count = {} Records  "
					+ "\t & Event_collection_duration = {} Sec",
					byteCount, eventCount, eventCollectionDuration);
			//accumulator = new LocalFileAccumulator(localFileEventSet, s3SinkService, s3SinkConfig, db);
		} catch (Exception e) {
			LOG.error("Exception while storing recoreds into Local-file", e);
		}
		return accumulator;
	}

	/**
	 * Bunch of events based on thresholds set in the configuration. The Thresholds
	 * such as events count, bytes capacity and data collection duration.
	 * 
	 * @param eventCount
	 * @param watch
	 * @param byteCount
	 * @return
	 */
	private boolean thresholdsCheck(int eventCount, StopWatch watch, int byteCount) {
		boolean flag = Boolean.FALSE;
		flag = eventCount < numEvents
				&& watch.getTime(TimeUnit.SECONDS) < duration
				&& byteCount < byteCapacity.getBytes();
		return flag;
	}
}
