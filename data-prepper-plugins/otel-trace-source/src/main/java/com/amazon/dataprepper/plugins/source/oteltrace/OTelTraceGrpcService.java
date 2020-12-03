package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import java.util.concurrent.TimeoutException;


public class OTelTraceGrpcService extends TraceServiceGrpc.TraceServiceImplBase {
    private final int bufferWriteTimeoutInMillis;
    private final Buffer<Record<ExportTraceServiceRequest>> buffer;

    public OTelTraceGrpcService(int bufferWriteTimeoutInMillis, final Buffer<Record<ExportTraceServiceRequest>> buffer) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.buffer = buffer;
    }

    @Override
    public void export(ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> responseObserver) {
        try {
            buffer.write(new Record<>(request), bufferWriteTimeoutInMillis);
            responseObserver.onNext(ExportTraceServiceResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (TimeoutException e) {
            responseObserver
                    .onError(Status.RESOURCE_EXHAUSTED.withDescription("Buffer is full, request timed out.")
                            .asException());
        }
    }
}
