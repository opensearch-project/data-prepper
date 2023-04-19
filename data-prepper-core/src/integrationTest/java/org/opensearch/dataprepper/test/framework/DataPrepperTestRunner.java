/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.framework;

import org.opensearch.dataprepper.AbstractContextManager;
import org.opensearch.dataprepper.DataPrepper;
import org.opensearch.dataprepper.parser.config.FileStructurePathProvider;
import org.opensearch.dataprepper.plugins.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.InMemorySourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.annotation.Nullable;

/**
 * Provides the ability to run a Data Prepper test instance. Each of these will run
 * the Data Prepper core application context. This test instance will thus be very
 * similar to a real Data Prepper instance.
 * <p>
 * Test suites can create a new Data Prepper test {@link Builder} class.
 */
public class DataPrepperTestRunner {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperTestRunner.class);
    private static final String BASE_PATH = "src/integrationTest/resources/org/opensearch/dataprepper";
    private final String dataPrepperConfigFile;
    private final String pipelinesDirectoryOrFile;
    private final InMemorySourceAccessor inMemorySourceAccessor;
    private final InMemorySinkAccessor inMemorySinkAccessor;
    private final TestContextManager contextManager;

    private DataPrepperTestRunner(final Builder builder) {
        dataPrepperConfigFile = builder.dataPrepperConfigFile;
        pipelinesDirectoryOrFile = builder.pipelinesDirectoryOrFile;

        inMemorySourceAccessor = new InMemorySourceAccessor();
        inMemorySinkAccessor = new InMemorySinkAccessor();
        LOG.info("created in memory source accessor {}", inMemorySourceAccessor);

        contextManager = new TestContextManager();
        LOG.info("Started Data Prepper Application context for testing.");
    }

    /**
     * Constructs a new {@link Builder} for creating a new test.
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Starts the Data Prepper test instance.
     */
    public void start() {
        contextManager.getDataPrepperBean()
                .execute();
    }

    /**
     * Stops the Data Prepper test instance.
     */
    public void stop() {
        final DataPrepper dataPrepper = contextManager.getDataPrepperBean();
        dataPrepper.shutdown();
    }

    /**
     * Gets the {@link InMemorySourceAccessor} used by this test Data Prepper instance.
     * Test suites can use this object to write data to any in_memory source running
     * in this Data Prepper test instance.
     * @return The {@link InMemorySourceAccessor}
     */
    public InMemorySourceAccessor getInMemorySourceAccessor() {
        return inMemorySourceAccessor;
    }

    /**
     * Gets the {@link InMemorySinkAccessor} used by this test Data Prepper instance.
     * Test suites can use this object to read data from any in_memory source running
     * in this Data Prepper test instance.
     * @return the {@link InMemorySinkAccessor}
     */
    public InMemorySinkAccessor getInMemorySinkAccessor() {
        return inMemorySinkAccessor;
    }

    private class TestFileStructurePathProvider implements FileStructurePathProvider {

        @Override
        public String getPipelineConfigFileLocation() {
            return BASE_PATH + "/pipeline/" + pipelinesDirectoryOrFile;
        }

        @Nullable
        @Override
        public String getDataPrepperConfigFileLocation() {
            return BASE_PATH + "/configuration/" + dataPrepperConfigFile;
        }
    }

    public static class Builder {
        private String pipelinesDirectoryOrFile;
        private String dataPrepperConfigFile = "data-prepper-config.yaml";

        Builder() {
        }

        public Builder withPipelinesDirectoryOrFile(final String pipelinesDirectoryOrFile) {
            this.pipelinesDirectoryOrFile = pipelinesDirectoryOrFile;
            return this;
        }

        public Builder withDataPrepperConfigFile(final String dataPrepperConfig) {
            this.dataPrepperConfigFile = dataPrepperConfig;
            return this;
        }

        public DataPrepperTestRunner build() {
            return new DataPrepperTestRunner(this);
        }
    }

    private class TestContextManager extends AbstractContextManager {

        protected void preRefreshPublicApplicationContext(final AnnotationConfigApplicationContext publicApplicationContext) {
            publicApplicationContext.registerBean(InMemorySourceAccessor.class, () -> inMemorySourceAccessor);
            publicApplicationContext.registerBean(InMemorySinkAccessor.class, () -> inMemorySinkAccessor);

        }

        protected void preRefreshCoreApplicationContext(final AnnotationConfigApplicationContext coreApplicationContext) {
            coreApplicationContext.registerBean(FileStructurePathProvider.class, TestFileStructurePathProvider::new);
        }
    }
}
