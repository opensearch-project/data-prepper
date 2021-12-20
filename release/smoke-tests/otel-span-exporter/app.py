from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from grpc import ssl_channel_credentials

resource = Resource(attributes={
    "service.name": "PythonService"
})

trace_provider = TracerProvider(resource=resource, )

otlp_exporter = OTLPSpanExporter(
    endpoint="otel-collector:4317",
)

trace_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

trace.set_tracer_provider(trace_provider)

span = trace.get_current_span()
span.set_attribute("user_id", "9999")

def run():
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("http-handler"):
        with tracer.start_as_current_span("my-cool-function"):
            print("I'm doing something!")

if __name__ == "__main__":
    run()
