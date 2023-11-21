from opentelemetry import trace
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor


def example_observability_setup_tracing():
    # Create a resource object
    resource = Resource(attributes={SERVICE_NAME: "momento_requests_counter"})

    # Create a tracer provider
    tracer_provider = TracerProvider(resource=resource)

    # Create a Zipkin exporter
    zipkin_exporter = ZipkinExporter()

    # Create a span processor and add the exporter
    span_processor = SimpleSpanProcessor(zipkin_exporter)
    tracer_provider.add_span_processor(span_processor)

    # Register the tracer provider
    trace.set_tracer_provider(tracer_provider)

    # Register the gRPC instrumentation
    GrpcInstrumentorClient().instrument()
