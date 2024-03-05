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
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.processor.exception.EngineFailureException;
import org.opensearch.dataprepper.plugins.processor.exception.EnrichFailedException;
import org.opensearch.dataprepper.plugins.processor.extension.GeoIPProcessorService;
import org.opensearch.dataprepper.plugins.processor.extension.GeoIpConfigSupplier;
import org.opensearch.dataprepper.plugins.processor.utils.IPValidationCheck;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  static final String GEO_IP_EVENTS_FAILED_IP_NOT_FOUND = "eventsFailedIpNotFound";
  private final Counter geoIpEventsProcessed;
  private final Counter geoIpEventsSucceeded;
  private final Counter geoIpEventsFailed;
  private final Counter geoIpEventsFailedEngineException;
  private final Counter geoIpEventsFailedIPNotFound;
  private final GeoIPProcessorConfig geoIPProcessorConfig;
  private final List<String> tagsOnEngineFailure;
  private final List<String> tagsOnIPNotFound;
  private final GeoIPProcessorService geoIPProcessorService;
  private final ExpressionEvaluator expressionEvaluator;
  private final Map<EntryConfig, List<GeoIPField>> entryFieldsMap;
  final Map<EntryConfig, Set<GeoIPDatabase>> entryDatabaseMap;

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
    this.geoIPProcessorService = geoIpConfigSupplier.getGeoIPProcessorService().orElseThrow(() ->
            new IllegalStateException("geoip_service configuration is required when using geoip processor."));
    this.geoIPProcessorConfig = geoIPProcessorConfig;
    this.tagsOnEngineFailure = geoIPProcessorConfig.getTagsOnEngineFailure();
    this.tagsOnIPNotFound = geoIPProcessorConfig.getTagsOnIPNotFound();
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
    Map<String, Object> geoData;
    try (final GeoIPDatabaseReader geoIPDatabaseReader = geoIPProcessorService.getGeoIPDatabaseReader()) {

      for (final Record<Event> eventRecord : records) {
        final Event event = eventRecord.getData();
        final String whenCondition = geoIPProcessorConfig.getWhenCondition();
        // continue if when condition is null or is false
        // or if database reader is null or database reader is expired
        if (checkConditionAndDatabaseReader(geoIPDatabaseReader, event, whenCondition)) continue;

        boolean eventSucceeded = true;
        boolean ipNotFound = false;
        boolean engineFailure = false;

        for (final EntryConfig entry : geoIPProcessorConfig.getEntries()) {
          final String source = entry.getSource();
          final List<GeoIPField> fields = entryFieldsMap.get(entry);
          final Set<GeoIPDatabase> databases = entryDatabaseMap.get(entry);
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
              if (IPValidationCheck.isPublicIpAddress(ipAddress)) {
                geoData = geoIPDatabaseReader.getGeoData(InetAddress.getByName(ipAddress), fields, databases);
                if (geoData.isEmpty()) {
                  ipNotFound = true;
                  eventSucceeded = false;
                } else {
                  eventRecord.getData().put(entry.getTarget(), geoData);
                }
              } else {
                // no enrichment if IP is not public
                ipNotFound = true;
                eventSucceeded = false;
              }
            } catch (final UnknownHostException e) {
              ipNotFound = true;
              eventSucceeded = false;
              LOG.error(DataPrepperMarkers.EVENT, "Failed to validate IP address: [{}] in event: [{}]. Caused by:[{}]",
                      ipAddress, event, e.getMessage());
              LOG.error("Failed to validate IP address: [{}]. Caused by:[{}]", ipAddress, e.getMessage());
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
            ipNotFound = true;
          }
        }

        updateTagsAndMetrics(event, eventSucceeded, ipNotFound, engineFailure);
      }
    } catch (final Exception e) {
      LOG.error("Encountered exception in geoip processor.", e);
    }
    return records;
  }

  private void updateTagsAndMetrics(final Event event, final boolean eventSucceeded, final boolean ipNotFound, final boolean engineFailure) {
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

  private Map<EntryConfig, List<GeoIPField>> populateGeoIPFields() {
    final Map<EntryConfig, List<GeoIPField>> entryConfigFieldsMap = new HashMap<>();
    for (final EntryConfig entry: geoIPProcessorConfig.getEntries()) {
      final List<String> includeFields = entry.getIncludeFields();
      final List<String> excludeFields = entry.getExcludeFields();
      List<GeoIPField> geoIPFields = new ArrayList<>();
      if (includeFields != null && !includeFields.isEmpty()) {
        for (final String field : includeFields) {
          final GeoIPField geoIPField = GeoIPField.findByName(field);
          if (geoIPField != null) {
            geoIPFields.add(geoIPField);
          }
        }
      } else if (excludeFields != null) {
        final List<GeoIPField> excludeGeoIPFields = new ArrayList<>();
        for (final String field : excludeFields) {
          final GeoIPField geoIPField = GeoIPField.findByName(field);
          if (geoIPField != null) {
            excludeGeoIPFields.add(geoIPField);
          }
        }
        geoIPFields = new ArrayList<>(List.of(GeoIPField.values()));
        geoIPFields.removeAll(excludeGeoIPFields);
      }
      entryConfigFieldsMap.put(entry, geoIPFields);
    }
    return entryConfigFieldsMap;
  }

  private Map<EntryConfig, Set<GeoIPDatabase>> populateGeoIPDatabases() {
    final Map<EntryConfig, Set<GeoIPDatabase>> entryConfigGeoIPDatabaseMap = new HashMap<>();
    for (final EntryConfig entry : geoIPProcessorConfig.getEntries()) {
      final List<GeoIPField> geoIPFields = entryFieldsMap.get(entry);
      final Set<GeoIPDatabase> geoIPDatabasesToUse = new HashSet<>();
      for (final GeoIPField geoIPField : geoIPFields) {
        final Set<GeoIPDatabase> geoIPDatabases = geoIPField.getGeoIPDatabases();
        geoIPDatabasesToUse.addAll(geoIPDatabases);
      }
      entryConfigGeoIPDatabaseMap.put(entry, geoIPDatabasesToUse);
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