/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;
import org.opensearch.dataprepper.plugins.geoip.exception.EngineFailureException;
import org.opensearch.dataprepper.plugins.geoip.exception.EnrichFailedException;
import org.opensearch.dataprepper.plugins.geoip.extension.GeoIPProcessorService;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIpConfigSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation class of geoIP-processor plugin. It is responsible for enrichment of
 * attributes for the public IPs. Supports both IPV4 and IPV6
 */
@DataPrepperPlugin(name = "geoip", pluginType = Processor.class, pluginConfigurationType = GeoIPProcessorConfig.class)
public class GeoIPProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

  private static final Logger LOG = LoggerFactory.getLogger(GeoIPProcessor.class);
  static final String GEO_IP_EVENTS_PROCESSED = "eventsProcessed";
  static final String GEO_IP_EVENTS_SUCCEEDED = "eventsSucceeded";
  static final String GEO_IP_EVENTS_FAILED = "eventsFailed";
  static final String GEO_IP_EVENTS_FAILED_ENGINE_EXCEPTION = "eventsFailedEngineException";
  static final String GEO_IP_EVENTS_FAILED_IP_NOT_FOUND = "eventsFailedLocationNotFound";
  private final Counter geoIpEventsProcessed;
  private final Counter geoIpEventsSucceeded;
  private final Counter geoIpEventsFailed;
  private final Counter geoIpEventsFailedEngineException;
  private final Counter geoIpEventsFailedIPNotFound;
  private final GeoIPProcessorConfig geoIPProcessorConfig;
  private final List<String> tagsOnEngineFailure;
  private final List<String> tagsOnIPNotFound;
  private final List<String> tagsOnInvalidIP;
  private final GeoIPProcessorService geoIPProcessorService;
  private final ExpressionEvaluator expressionEvaluator;
  private final Map<EntryConfig, Collection<GeoIPField>> entryFieldsMap;
  final Map<EntryConfig, Collection<GeoIPDatabase>> entryDatabaseMap;

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

    if (geoIPProcessorConfig.getWhenCondition() != null &&
            (!expressionEvaluator.isValidExpressionStatement(geoIPProcessorConfig.getWhenCondition()))) {
      throw new InvalidPluginConfigurationException("geoip_when {} is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax");
    }

    this.geoIPProcessorService = geoIpConfigSupplier.getGeoIPProcessorService().orElseThrow(() ->
            new IllegalStateException("geoip_service configuration is required when using geoip processor."));
    this.geoIPProcessorConfig = geoIPProcessorConfig;
    this.tagsOnEngineFailure = geoIPProcessorConfig.getTagsOnEngineFailure();
    this.tagsOnIPNotFound = geoIPProcessorConfig.getTagsOnIPNotFound();
    this.tagsOnInvalidIP = geoIPProcessorConfig.getTagsOnNoValidIp();
    this.expressionEvaluator = expressionEvaluator;
    this.geoIpEventsProcessed = pluginMetrics.counter(GEO_IP_EVENTS_PROCESSED);
    this.geoIpEventsSucceeded = pluginMetrics.counter(GEO_IP_EVENTS_SUCCEEDED);
    this.geoIpEventsFailed = pluginMetrics.counter(GEO_IP_EVENTS_FAILED);
    this.geoIpEventsFailedEngineException = pluginMetrics.counter(GEO_IP_EVENTS_FAILED_ENGINE_EXCEPTION);
    this.geoIpEventsFailedIPNotFound = pluginMetrics.counter(GEO_IP_EVENTS_FAILED_IP_NOT_FOUND);

    this.entryFieldsMap = populateGeoIPFields();
    this.entryDatabaseMap = populateGeoIPDatabases();
  }

  /**
   * Get the enriched data from the maxmind database
   * @param records Input records
   * @return collection of record events
   */
  @Override
  public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
    try (final GeoIPDatabaseReader geoIPDatabaseReader = BatchGeoIPDatabaseReader.decorate(geoIPProcessorService.getGeoIPDatabaseReader())) {
       processRecords(records, geoIPDatabaseReader);
    } catch (final Exception e) {
      LOG.error("Encountered exception in geoip processor.", e);
    }
    return records;
  }

  private void processRecords(Collection<Record<Event>> records, GeoIPDatabaseReader geoIPDatabaseReader) {
    Map<String, Object> geoData;
    for (final Record<Event> eventRecord : records) {
      final Event event = eventRecord.getData();
      final String whenCondition = geoIPProcessorConfig.getWhenCondition();
      // continue if when condition is null or is false
      // or if database reader is null or database reader is expired
      if (checkConditionAndDatabaseReader(geoIPDatabaseReader, event, whenCondition))
        continue;

      boolean eventSucceeded = true;
      boolean ipNotFound = false;
      boolean invalidIp = false;
      boolean engineFailure = false;

        for (final EntryConfig entry : geoIPProcessorConfig.getEntries()) {
          final String source = entry.getSource();
          final Collection<GeoIPField> fields = entryFieldsMap.get(entry);
          final Collection<GeoIPDatabase> databases = entryDatabaseMap.get(entry);
          String ipAddress = null;
          try {
            ipAddress = event.get(source, String.class);
          } catch (final Exception e) {
            eventSucceeded = false;
            ipNotFound = true;
            LOG.error(DataPrepperMarkers.EVENT, "Failed to get IP address from [{}] in event: [{}]. Caused by:[{}]",
                    source, event, e.getMessage());
          }

        //Lookup from DB
        if (ipAddress != null && !ipAddress.isEmpty()) {
          try {
            final Optional<InetAddress> optionalInetAddress = GeoInetAddress.usableInetFromString(ipAddress);
            if (optionalInetAddress.isPresent()) {
                geoData = geoIPDatabaseReader.getGeoData(optionalInetAddress.get(), fields, databases);
                if (geoData.isEmpty()) {
                  ipNotFound = true;
                  eventSucceeded = false;
                } else {
                  eventRecord.getData().put(entry.getTarget(), geoData);
                }
            } else {
              // no enrichment if IP is not public
              //ipNotFound = true;
              invalidIp = true;
              eventSucceeded = false;
            }
          } catch (final EnrichFailedException e) {
            ipNotFound = true;
            eventSucceeded = false;
            LOG.error(DataPrepperMarkers.EVENT, "IP address not found in database for IP: [{}] in event: [{}]. Caused by:[{}]",
                    ipAddress, event, e.getMessage());
            LOG.error("IP address not found in database for IP: [{}]. Caused by:[{}]", ipAddress, e.getMessage());
          } catch (final EngineFailureException e) {
            engineFailure = true;
            eventSucceeded = false;
            LOG.error(DataPrepperMarkers.EVENT, "Failed to get Geo data for event: [{}] for the IP address [{}]. Caused by:{}",
                    event, ipAddress, e.getMessage());
            LOG.error("Failed to get Geo data for the IP address [{}]. Caused by:{}", ipAddress, e.getMessage());
          }
        } else {
          //No Enrichment if IP is null or empty
          eventSucceeded = false;
          invalidIp = true;
        }
      }

      updateTagsAndMetrics(event, eventSucceeded, ipNotFound, engineFailure, invalidIp);
    }
  }

  private void updateTagsAndMetrics(final Event event, final boolean eventSucceeded, final boolean ipNotFound, final boolean engineFailure, final boolean invalidIp) {
    if (invalidIp) {
      event.getMetadata().addTags(tagsOnInvalidIP);
    }
    if (ipNotFound) {
      event.getMetadata().addTags(tagsOnIPNotFound);
      geoIpEventsFailedIPNotFound.increment();
    }
    if (engineFailure) {
      event.getMetadata().addTags(tagsOnEngineFailure);
      geoIpEventsFailedEngineException.increment();
    }
    if (eventSucceeded) {
      geoIpEventsSucceeded.increment();
    } else {
      geoIpEventsFailed.increment();
    }
  }

  private boolean checkConditionAndDatabaseReader(final GeoIPDatabaseReader geoIPDatabaseReader, final Event event, final String whenCondition) {
    if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
      return true;
    }
    geoIpEventsProcessed.increment();

    // if database reader is not created or if all database readers are expired
    if (geoIPDatabaseReader == null || geoIPDatabaseReader.isExpired()) {
      event.getMetadata().addTags(tagsOnEngineFailure);
      geoIpEventsFailed.increment();
      return true;
    }
    return false;
  }

  private Map<EntryConfig, Collection<GeoIPField>> populateGeoIPFields() {
    return geoIPProcessorConfig.getEntries()
            .stream()
            .collect(Collectors.toMap(Function.identity(), EntryConfig::getGeoIPFields));
  }

  private Map<EntryConfig, Collection<GeoIPDatabase>> populateGeoIPDatabases() {
    final Map<EntryConfig, Collection<GeoIPDatabase>> entryConfigGeoIPDatabaseMap = new HashMap<>();
    for (final EntryConfig entry : geoIPProcessorConfig.getEntries()) {
      final Collection<GeoIPField> geoIPFields = entryFieldsMap.get(entry);
      final Collection<GeoIPDatabase> geoIPDatabasesToUse = GeoIPDatabase.selectDatabasesForFields(geoIPFields);
      entryConfigGeoIPDatabaseMap.put(entry, geoIPDatabasesToUse);
    }

    if(LOG.isDebugEnabled()) {
      for (final Collection<GeoIPDatabase> geoIPDatabases : entryConfigGeoIPDatabaseMap.values()) {
        LOG.debug("Entry configuration using databases: {}", geoIPDatabases.stream().map(Enum::name).collect(Collectors.joining(", ")));
      }
    }

    return entryConfigGeoIPDatabaseMap;
  }

  @Override
  public void prepareForShutdown() {
  }

  @Override
  public boolean isReadyForShutdown() {
    geoIPProcessorService.shutdown();
    return true;
  }

  @Override
  public void shutdown() {

  }
}