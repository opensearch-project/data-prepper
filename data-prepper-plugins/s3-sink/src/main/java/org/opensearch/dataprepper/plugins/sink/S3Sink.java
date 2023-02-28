/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implementation class of s3-sink plugin
 *
 */
@DataPrepperPlugin(name = "s3", pluginType = Sink.class, pluginConfigurationType = S3SinkConfig.class)
public class S3Sink extends AbstractSink<Record<Event>> {

	private static final Logger LOG = LoggerFactory.getLogger(S3Sink.class);
	private static final int EVENT_QUEUE_SIZE = 100000;

	private final S3SinkConfig s3SinkConfig;
	private volatile boolean initialized;
	private static BlockingQueue<Event> eventQueue;
	private static boolean isStopRequested;
	
	private final Codec codec;
	private final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * 
	 * @param pluginSetting
	 * @param s3SinkConfig
	 * @param pluginFactory
	 */
	@DataPrepperPluginConstructor
	public S3Sink(PluginSetting pluginSetting, final S3SinkConfig s3SinkConfig, final PluginFactory pluginFactory) {
		super(pluginSetting);
		this.s3SinkConfig = s3SinkConfig;
		final PluginModel codecConfiguration = s3SinkConfig.getCodec();
		final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
		codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
		initialized = Boolean.FALSE;
	}

	@Override
	public boolean isReady() {
		return initialized;
	}

	@Override
	public void doInitialize() {
		try {
			doInitializeInternal();
		} catch (InvalidPluginConfigurationException e) {
			LOG.error("Failed to initialize S3-Sink.");
			this.shutdown();
			throw new RuntimeException(e.getMessage(), e);
		} catch (Exception e) {
			LOG.warn("Failed to initialize S3-Sink, retrying. Error - {} \n {}", e.getMessage(), e.getCause());
		}
	}

	private void doInitializeInternal() {
		eventQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_SIZE);
		S3SinkService s3SinkService = new S3SinkService(s3SinkConfig);
		S3SinkWorker worker = new S3SinkWorker(s3SinkService, s3SinkConfig, codec);
		new Thread(worker).start();
		initialized = Boolean.TRUE;
	}

	@Override
	public void doOutput(final Collection<Record<Event>> records) {
		LOG.debug("Records size : {}", records.size());
		if (records.isEmpty()) {
			return;
		}
		
		for (final Record<Event> recordData : records) {
			
			Event event = recordData.getData();
			getEventQueue().add(event);
			
		}
	}

	@Override
	public void shutdown() {
		super.shutdown();
		isStopRequested = Boolean.TRUE;
		LOG.info("s3-sink sutdonwn completed");
	}

	public static BlockingQueue<Event> getEventQueue() {
		return eventQueue;
	}

	public static boolean isStopRequested() {
		return isStopRequested;
	}
}
