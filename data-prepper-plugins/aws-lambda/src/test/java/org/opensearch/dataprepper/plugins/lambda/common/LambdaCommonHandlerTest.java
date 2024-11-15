package org.opensearch.dataprepper.plugins.lambda.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LambdaCommonHandlerTest {

    @Test
    public void testCheckStatusCode_Success() {
        // Arrange
        InvokeResponse response = Mockito.mock(InvokeResponse.class);
        Mockito.when(response.statusCode()).thenReturn(200);

        // Act
        boolean result = LambdaCommonHandler.checkStatusCode(response);

        // Assert
        assertTrue(result, "Expected checkStatusCode to return true for status code 200");
    }

    @Test
    public void testCheckStatusCode_ClientError() {
        // Arrange
        InvokeResponse response = Mockito.mock(InvokeResponse.class);
        Mockito.when(response.statusCode()).thenReturn(400);

        // Act
        boolean result = LambdaCommonHandler.checkStatusCode(response);

        // Assert
        assertFalse(result, "Expected checkStatusCode to return false for status code 400");
    }


    @Test
    public void testWaitForFutures_AllCompleteSuccessfully() {
        // Arrange
        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> future2 = CompletableFuture.completedFuture(null);
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        futureList.add(future1);
        futureList.add(future2);

        // Act
        LambdaCommonHandler.waitForFutures(futureList);

        // Assert
        assertTrue(futureList.isEmpty(), "Expected futureList to be cleared after completion");
    }

    @Test
    public void testWaitForFutures_WithExceptions() {
        // Arrange
        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> future2 = new CompletableFuture<>();
        future2.completeExceptionally(new RuntimeException("Test exception"));
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        futureList.add(future1);
        futureList.add(future2);

        // Act
        LambdaCommonHandler.waitForFutures(futureList);

        // Assert
        assertTrue(futureList.isEmpty(), "Expected futureList to be cleared even after exceptions");
    }

}
