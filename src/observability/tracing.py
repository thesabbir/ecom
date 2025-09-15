from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode
from contextlib import contextmanager
from typing import Optional, Dict, Any
import time


def setup_tracing(service_name: str = "ecommerce-crawler", jaeger_host: str = "localhost", jaeger_port: int = 6831):
    """Initialize OpenTelemetry tracing"""

    try:
        # Create a resource identifying the service
        resource = Resource.create({
            "service.name": service_name,
            "service.version": "1.0.0",
            "deployment.environment": "production"
        })

        # Create tracer provider
        provider = TracerProvider(resource=resource)

        # Only add Jaeger exporter if we can connect
        try:
            jaeger_exporter = JaegerExporter(
                agent_host_name=jaeger_host,
                agent_port=jaeger_port,
                udp_split_oversized_batches=True
            )

            # Add batch span processor with smaller batch size
            provider.add_span_processor(
                BatchSpanProcessor(jaeger_exporter, max_queue_size=512, max_export_batch_size=32)
            )
        except Exception as e:
            # If Jaeger is not available, just continue without it
            print(f"Warning: Could not connect to Jaeger: {e}")

        # Set global tracer provider
        trace.set_tracer_provider(provider)

        # Get tracer
        tracer = trace.get_tracer(__name__)

        return tracer
    except Exception as e:
        print(f"Warning: Tracing setup failed: {e}")
        # Return a no-op tracer if setup fails
        return trace.get_tracer(__name__)


def instrument_app(app):
    """Instrument FastAPI app for automatic tracing"""
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()


@contextmanager
def trace_operation(operation_name: str, attributes: Optional[Dict[str, Any]] = None):
    """Context manager for tracing operations"""
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span(operation_name) as span:
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, str(value))

        start_time = time.time()

        try:
            yield span
            span.set_status(Status(StatusCode.OK))
            span.set_attribute("duration_ms", (time.time() - start_time) * 1000)
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.set_attribute("error", str(e))
            span.set_attribute("duration_ms", (time.time() - start_time) * 1000)
            raise


class CrawlTracer:
    """Specialized tracer for crawl operations"""

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.tracer = trace.get_tracer(__name__)

    @contextmanager
    def trace_crawl_job(self, url: str, site_name: str):
        with self.tracer.start_as_current_span("crawl_job") as span:
            span.set_attribute("job.id", self.job_id)
            span.set_attribute("crawl.url", url)
            span.set_attribute("crawl.site_name", site_name)

            try:
                yield span
                span.set_status(Status(StatusCode.OK))
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    @contextmanager
    def trace_page_fetch(self, url: str):
        with self.tracer.start_as_current_span("fetch_page") as span:
            span.set_attribute("http.url", url)
            span.set_attribute("job.id", self.job_id)

            start_time = time.time()

            try:
                yield span
                span.set_attribute("http.status_code", 200)
                span.set_attribute("duration_ms", (time.time() - start_time) * 1000)
                span.set_status(Status(StatusCode.OK))
            except Exception as e:
                span.set_attribute("http.status_code", 500)
                span.set_attribute("error", str(e))
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    @contextmanager
    def trace_product_extraction(self, url: str):
        with self.tracer.start_as_current_span("extract_product") as span:
            span.set_attribute("product.url", url)
            span.set_attribute("job.id", self.job_id)

            try:
                yield span
                span.set_status(Status(StatusCode.OK))
            except Exception as e:
                span.set_attribute("error", str(e))
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """Add an event to the current span"""
        current_span = trace.get_current_span()
        if current_span:
            current_span.add_event(name, attributes or {})