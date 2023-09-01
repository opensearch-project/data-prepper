package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperStringContextualDeserializerTest {
    @Mock
    private BeanProperty beanProperty;

    @Mock
    private DeserializationContext deserializationContext;

    @Mock
    private PluginConfigValueTranslator pluginConfigValueTranslator;

    @Mock
    private SupportSecretString annotation;

    private DataPrepperStringContextualDeserializer objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new DataPrepperStringContextualDeserializer(pluginConfigValueTranslator);
    }

    @Test
    void testCreateContextual_with_support_secret_string_annotated_bean_property() {
        when(beanProperty.getAnnotation(eq(SupportSecretString.class))).thenReturn(annotation);
        final JsonDeserializer<String> returnedJsonStringDeserializer = objectUnderTest.createContextual(
                deserializationContext, beanProperty);
        assertThat(returnedJsonStringDeserializer, is(objectUnderTest));
    }

    @Test
    void testCreateContextual_with_normal_string_bean_property() {
        when(beanProperty.getAnnotation(eq(SupportSecretString.class))).thenReturn(null);
        final JsonDeserializer<String> returnedJsonStringDeserializer = objectUnderTest.createContextual(
                deserializationContext, beanProperty);
        assertThat(returnedJsonStringDeserializer, instanceOf(StringDeserializer.class));
    }
}