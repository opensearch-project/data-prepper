package org.opensearch.dataprepper.plugins.source.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class S3ServiceTest {

    @Mock
    private S3ObjectHandler s3ObjectHandler;

    private S3Service createObjectUnderTest() {
        return new S3Service(s3ObjectHandler);
    }

    @Test
    void addS3Object_calls_parseS3Object_on_S3ObjectHandler() throws IOException {
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        final S3ObjectReference s3ObjectReference = mock(S3ObjectReference.class);

        doNothing().when(s3ObjectHandler).parseS3Object(eq(s3ObjectReference), eq(acknowledgementSet), eq(null), eq(null));

        final S3Service objectUnderTest = createObjectUnderTest();

        objectUnderTest.addS3Object(s3ObjectReference, acknowledgementSet);

        verify(s3ObjectHandler).parseS3Object(s3ObjectReference, acknowledgementSet, null, null);
    }

    @Test
    void deleteS3Object_calls_deleteS3Object_on_s3ObjectHandler() throws IOException {
        final S3ObjectReference s3ObjectReference = mock(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).deleteS3Object(s3ObjectReference);

        final S3Service s3Service = createObjectUnderTest();

        s3Service.deleteS3Object(s3ObjectReference);
        verify(s3ObjectHandler).deleteS3Object(s3ObjectReference);

    }
}
