/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.configuration.EntryConfig;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.EnrichFailedException;
import org.opensearch.dataprepper.plugins.processor.extension.GeoIPProcessorService;
import org.opensearch.dataprepper.plugins.processor.extension.GeoIpConfigSupplier;
import org.opensearch.dataprepper.plugins.processor.utils.IPValidationCheck;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Implementation class of geoIP-processor plugin. It is responsible for enrichment of
 * attributes for the public IPs. Supports both IPV4 and IPV6
 */
@DataPrepperPlugin(name = "geoip", pluginType = Processor.class, pluginConfigurationType = GeoIPProcessorConfig.class)
public class GeoIPProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

  private static final Logger LOG = LoggerFactory.getLogger(GeoIPProcessor.class);
  static final String GEO_IP_EVENTS_PROCESSED = "eventsProcessed";
  static final String GEO_IP_EVENTS_FAILED_LOOKUP = "eventsFailedLookup";
  static final String GEO_IP_EVENTS_FAILED_ENGINE_EXCEPTION = "eventsFailedEngineException";
  private final Counter geoIpEventsProcessed;
  private final Counter geoIpEventsFailedLookup;
  private final Counter geoIpEventsFailedEngineException;
  private final GeoIPProcessorConfig geoIPProcessorConfig;
  private final List<String> tagsOnFailure;
  private final GeoIPProcessorService geoIPProcessorService;
  private final ExpressionEvaluator expressionEvaluator;

  /**
   * GeoIPProcessor constructor for initialization of required attributes
   * @param pluginMetrics pluginMetrics
   * @param geoIPProcessorConfig geoIPProcessorConfig
   * @param geoIpConfigSupplier geoIpConfigSupplier
   */
  @DataPrepperPluginConstructor
  public GeoIPProcessor(final PluginMetrics pluginMetrics,
                        final GeoIPProcessorConfig geoIPProcessorConfig,
                        final GeoIpConfigSupplier geoIpConfigSupplier,
                        final ExpressionEvaluator expressionEvaluator) {
    super(pluginMetrics);
    this.geoIPProcessorConfig = geoIPProcessorConfig;
    this.geoIPProcessorService = geoIpConfigSupplier.getGeoIPProcessorService();
    this.tagsOnFailure = geoIPProcessorConfig.getTagsOnFailure();
    this.expressionEvaluator = expressionEvaluator;
    this.geoIpEventsProcessed = pluginMetrics.counter(GEO_IP_EVENTS_PROCESSED);
    this.geoIpEventsFailedLookup = pluginMetrics.counter(GEO_IP_EVENTS_FAILED_LOOKUP);
    //TODO: Use the exception metric for exceptions from service
    this.geoIpEventsFailedEngineException = pluginMetrics.counter(GEO_IP_EVENTS_FAILED_ENGINE_EXCEPTION);
  }

  /**
   * Get the enriched data from the maxmind database
   * @param records Input records
   * @return collection of record events
   */
  @Override
  public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
    Map<String, Object> geoData;

    for (final Record<Event> eventRecord : records) {
      final Event event = eventRecord.getData();
      boolean isEventProcessed = false;
      boolean isEventFailedLookup = false;

      for (final EntryConfig entry : geoIPProcessorConfig.getEntries()) {
        final String whenCondition = entry.getWhenCondition();
        if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
          continue;
        }
        isEventProcessed = true;
        final String source = entry.getSource();
        final List<String> attributes = entry.getFields();
        final String ipAddress = event.get(source, String.class);

        //Lookup from DB
        if (ipAddress != null && !ipAddress.isEmpty()) {
          try {
            if (IPValidationCheck.isPublicIpAddress(ipAddress)) {
              geoData = geoIPProcessorService.getGeoData(InetAddress.getByName(ipAddress), attributes);
              eventRecord.getData().put(entry.getTarget(), geoData);
            } else {
              isEventFailedLookup = true;
            }
          } catch (final IOException | EnrichFailedException ex) {
            isEventFailedLookup = true;
            LOG.error(DataPrepperMarkers.EVENT, "Failed to get Geo data for event: [{}] for the IP address [{}]", event, ipAddress, ex);
          }
        } else {
          //No Enrichment.
          isEventFailedLookup = true;
        }
      }
      if (isEventProcessed) {
        geoIpEventsProcessed.increment();
      }
      if (isEventFailedLookup) {
        geoIpEventsFailedLookup.increment();
        event.getMetadata().addTags(tagsOnFailure);
      }
    }
    return records;
  }

  @Override
  public void prepareForShutdown() {
  }

  @Override
  public boolean isReadyForShutdown() {
    return true;
  }

  @Override
  public void shutdown() {
    //TODO: delete mmdb files
    LOG.info("GeoIP plugin Shutdown");
  }
}