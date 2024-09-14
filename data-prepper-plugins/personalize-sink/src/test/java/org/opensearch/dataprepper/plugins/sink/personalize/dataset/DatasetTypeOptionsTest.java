package org.opensearch.dataprepper.plugins.sink.personalize.dataset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class DatasetTypeOptionsTest {
    @Test
    void notNull_test() {
        assertNotNull(DatasetTypeOptions.ITEMS);
    }

    @Test
    void fromOptionValue_users_test() {
        DatasetTypeOptions datasetTypeOptions = DatasetTypeOptions.fromOptionValue("users");
        assertNotNull(datasetTypeOptions);
        assertThat(datasetTypeOptions.toString(), equalTo("USERS"));
    }

    @Test
    void fromOptionValue_items_test() {
        DatasetTypeOptions datasetTypeOptions = DatasetTypeOptions.fromOptionValue("items");
        assertNotNull(datasetTypeOptions);
        assertThat(datasetTypeOptions.toString(), equalTo("ITEMS"));
    }

    @Test
    void fromOptionValue_interactions_test() {
        DatasetTypeOptions datasetTypeOptions = DatasetTypeOptions.fromOptionValue("interactions");
        assertNotNull(datasetTypeOptions);
        assertThat(datasetTypeOptions.toString(), equalTo("INTERACTIONS"));
    }
}
