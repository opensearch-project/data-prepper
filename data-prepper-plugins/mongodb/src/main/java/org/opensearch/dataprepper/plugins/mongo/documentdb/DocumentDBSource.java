package org.opensearch.dataprepper.plugins.mongo.documentdb;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.UsesEnhancedSourceCoordination;
import org.opensearch.dataprepper.plugins.mongo.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;


@DataPrepperPlugin(name = "documentdb", alternateNames = "mongodb", pluginType = Source.class, pluginConfigurationType = MongoDBSourceConfig.class)

public class DocumentDBSource implements Source<Record<Event>>, UsesEnhancedSourceCoordination {
    private static final Logger LOG = LoggerFactory.getLogger(DocumentDBSource.class);

    private final PluginMetrics pluginMetrics;
    private final MongoDBSourceConfig sourceConfig;
    private final PluginConfigObservable pluginConfigObservable;
    private EnhancedSourceCoordinator sourceCoordinator;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private DocumentDBService documentDBService;

    private final boolean acknowledgementsEnabled;

    @DataPrepperPluginConstructor
    public DocumentDBSource(final PluginMetrics pluginMetrics,
                            final MongoDBSourceConfig sourceConfig,
                            final AcknowledgementSetManager acknowledgementSetManager,
                            final PluginConfigObservable pluginConfigObservable) {
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginConfigObservable = pluginConfigObservable;
        this.acknowledgementsEnabled = sourceConfig.isAcknowledgmentsEnabled();

        sourceConfig.validateAwsConfigWithUsernameAndPassword();
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        Objects.requireNonNull(sourceCoordinator);
        sourceCoordinator.createPartition(new LeaderPartition());

        documentDBService = new DocumentDBService(sourceCoordinator, sourceConfig, pluginMetrics,
                acknowledgementSetManager, pluginConfigObservable);

        LOG.info("Start DocumentDB service");
        documentDBService.start(buffer);
    }


    @Override
    public void stop() {
        LOG.info("Stop DocumentDB service");
        if (Objects.nonNull(documentDBService)) {
            documentDBService.shutdown();
        }
    }

    @Override
    public void setEnhancedSourceCoordinator(EnhancedSourceCoordinator sourceCoordinator) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceCoordinator.initialize();
    }

    @Override
    public Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory() {
        return new PartitionFactory();
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }
}
