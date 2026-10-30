"""Tests for the OpenTelemetry metrics recorder and configuration helpers."""

import logging
import os
import types

import pytest
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter

from threadmill import metrics


class RecordingInstrument:
    """Record instrument operations without an SDK pipeline."""

    def __init__(self, kind: str, name: str, **options) -> None:
        self.kind = kind
        self.name = name
        self.options = options
        self.readings: list[tuple[str, float, dict]] = []

    def add(self, amount: float, attributes: dict) -> None:
        self.readings.append(("add", amount, attributes))

    def record(self, amount: float, attributes: dict) -> None:
        self.readings.append(("record", amount, attributes))

    def set(self, amount: float, attributes: dict) -> None:
        self.readings.append(("set", amount, attributes))


class RecordingMeter:
    """Build recording instruments for every instrument factory."""

    def __init__(self) -> None:
        self.instruments: dict[str, RecordingInstrument] = {}

    def _create(self, kind: str, name: str, **options) -> RecordingInstrument:
        instrument = RecordingInstrument(kind, name, **options)
        self.instruments[name] = instrument
        return instrument

    def create_counter(self, name: str, **options) -> RecordingInstrument:
        return self._create("counter", name, **options)

    def create_histogram(self, name: str, **options) -> RecordingInstrument:
        return self._create("histogram", name, **options)

    def create_gauge(self, name: str, **options) -> RecordingInstrument:
        return self._create("gauge", name, **options)


class RecordingMeterProvider:
    """Serve one recording meter and record shutdown."""

    def __init__(self) -> None:
        self.meter = RecordingMeter()
        self.meter_name = None
        self.shutdown_called = False

    def get_meter(self, name: str) -> RecordingMeter:
        self.meter_name = name
        return self.meter

    def shutdown(self) -> None:
        self.shutdown_called = True


@pytest.fixture
def provider() -> RecordingMeterProvider:
    """Build a meter provider double."""
    return RecordingMeterProvider()


@pytest.fixture
def recorder(provider: RecordingMeterProvider) -> metrics.OpenTelemetryMetrics:
    """Build the recorder against the provider double."""
    return metrics.OpenTelemetryMetrics(provider)


class TestMetrics:
    """Tests for the no-op recorder."""

    def test_recording_methods__discard_every_call(self) -> None:
        """Accept every recorder call without external state."""
        recorder = metrics.Metrics()
        recorder.record_task_started(queue_name="default", task_module_path="a.b")
        recorder.record_task_finished(
            queue_name="default",
            task_module_path="a.b",
            status="successful",
            duration_s=0.5,
        )
        recorder.record_busy_threads(busy_threads=1, thread_count=2)
        recorder.record_queue_tasks(queue_name="default", status="ready", count=3)
        recorder.record_queue_backlog(queue_name="default", count=3)
        recorder.record_queue_ingress(queue_name="default")
        recorder.record_queue_egress(queue_name="default")
        recorder.shutdown()


class TestOpenTelemetryMetrics:
    """Tests for the SDK-backed recorder."""

    def test_init__creates_instruments_on_threadmill_meter(
        self, provider: RecordingMeterProvider, recorder: metrics.OpenTelemetryMetrics
    ) -> None:
        """Create every instrument on the threadmill meter."""
        assert provider.meter_name == "threadmill"
        assert set(provider.meter.instruments) == {
            "threadmill.task.started",
            "threadmill.task.finished",
            "threadmill.task.duration",
            "threadmill.worker.busy_threads",
            "threadmill.queue.tasks",
            "threadmill.queue.backlog",
            "threadmill.queue.ingress",
            "threadmill.queue.egress",
        }
        histogram = provider.meter.instruments["threadmill.task.duration"]
        assert histogram.kind == "histogram"
        assert histogram.options["unit"] == "s"
        assert (
            histogram.options["explicit_bucket_boundaries_advisory"]
            == metrics.TASK_DURATION_BUCKETS_S
        )
        assert provider.meter.instruments["threadmill.task.started"].kind == "counter"
        assert provider.meter.instruments["threadmill.queue.backlog"].kind == "gauge"
        assert recorder.meter_provider is provider

    def test_record_task_started__adds_one_with_queue_and_module(
        self, provider: RecordingMeterProvider, recorder: metrics.OpenTelemetryMetrics
    ) -> None:
        """Count one attempt with its queue and task module path."""
        recorder.record_task_started(
            queue_name="default", task_module_path="tests.testapp.tasks.echo"
        )
        instrument = provider.meter.instruments["threadmill.task.started"]
        assert instrument.readings == [
            (
                "add",
                1,
                {
                    "threadmill.queue.name": "default",
                    "threadmill.task.module_path": "tests.testapp.tasks.echo",
                },
            )
        ]

    def test_record_task_finished__counts_and_observes_duration(
        self, provider: RecordingMeterProvider, recorder: metrics.OpenTelemetryMetrics
    ) -> None:
        """Count the outcome and record its duration with the same attributes."""
        recorder.record_task_finished(
            queue_name="default",
            task_module_path="tests.testapp.tasks.echo",
            status="successful",
            duration_s=0.25,
        )
        attributes = {
            "threadmill.queue.name": "default",
            "threadmill.task.module_path": "tests.testapp.tasks.echo",
            "threadmill.task.status": "successful",
        }
        finished = provider.meter.instruments["threadmill.task.finished"]
        duration = provider.meter.instruments["threadmill.task.duration"]
        assert finished.readings == [("add", 1, attributes)]
        assert duration.readings == [("record", 0.25, attributes)]

    def test_record_busy_threads__sets_gauge_with_pool_size(
        self, provider: RecordingMeterProvider, recorder: metrics.OpenTelemetryMetrics
    ) -> None:
        """Set the occupied-slot gauge with the pool size attribute."""
        recorder.record_busy_threads(busy_threads=2, thread_count=4)
        instrument = provider.meter.instruments["threadmill.worker.busy_threads"]
        assert instrument.readings == [
            ("set", 2, {"threadmill.worker.thread_count": 4})
        ]

    def test_record_queue_tasks__sets_segment_gauge(
        self, provider: RecordingMeterProvider, recorder: metrics.OpenTelemetryMetrics
    ) -> None:
        """Set the segment gauge with queue and segment attributes."""
        recorder.record_queue_tasks(queue_name="default", status="deferred", count=7)
        instrument = provider.meter.instruments["threadmill.queue.tasks"]
        assert instrument.readings == [
            (
                "set",
                7,
                {
                    "threadmill.queue.name": "default",
                    "threadmill.queue.segment": "deferred",
                },
            )
        ]

    def test_record_queue_backlog__sets_backlog_gauge(
        self, provider: RecordingMeterProvider, recorder: metrics.OpenTelemetryMetrics
    ) -> None:
        """Set the backlog gauge with the queue attribute."""
        recorder.record_queue_backlog(queue_name="default", count=9)
        instrument = provider.meter.instruments["threadmill.queue.backlog"]
        assert instrument.readings == [("set", 9, {"threadmill.queue.name": "default"})]

    def test_record_queue_ingress_and_egress__count_events(
        self, provider: RecordingMeterProvider, recorder: metrics.OpenTelemetryMetrics
    ) -> None:
        """Count incoming and outgoing events with the queue attribute."""
        recorder.record_queue_ingress(queue_name="default")
        recorder.record_queue_egress(queue_name="default")
        ingress = provider.meter.instruments["threadmill.queue.ingress"]
        egress = provider.meter.instruments["threadmill.queue.egress"]
        assert ingress.readings == [("add", 1, {"threadmill.queue.name": "default"})]
        assert egress.readings == [("add", 1, {"threadmill.queue.name": "default"})]

    def test_shutdown__shuts_down_provider(
        self, provider: RecordingMeterProvider, recorder: metrics.OpenTelemetryMetrics
    ) -> None:
        """Flush through the provider shutdown."""
        recorder.shutdown()
        assert provider.shutdown_called


class TestIsMetricsSdkAvailable:
    """Tests for the optional dependency probe."""

    def test_is_metrics_sdk_available__true_with_sdk_installed(self) -> None:
        """Report the installed SDK as available."""
        assert metrics.is_metrics_sdk_available() is True

    def test_is_metrics_sdk_available__false_when_module_missing(
        self, monkeypatch
    ) -> None:
        """Report a missing module as unavailable."""
        monkeypatch.setattr(metrics.importlib.util, "find_spec", lambda name: None)
        assert metrics.is_metrics_sdk_available() is False

    def test_is_metrics_sdk_available__false_when_parent_package_missing(
        self, monkeypatch
    ) -> None:
        """Report a missing parent package as unavailable."""

        def raise_module_not_found(name):
            raise ModuleNotFoundError(name)

        monkeypatch.setattr(metrics.importlib.util, "find_spec", raise_module_not_found)
        assert metrics.is_metrics_sdk_available() is False


class TestIsMetricsExportEnabled:
    """Tests for the environment switch."""

    @pytest.mark.parametrize(
        ("environment", "expected"),
        [
            ({"OTEL_METRICS_EXPORTER": None}, True),
            ({"OTEL_METRICS_EXPORTER": "otlp"}, True),
            ({"OTEL_METRICS_EXPORTER": "console"}, True),
            ({"OTEL_METRICS_EXPORTER": "none"}, False),
            ({"OTEL_METRICS_EXPORTER": "NONE"}, False),
            ({"OTEL_SDK_DISABLED": "true"}, False),
            ({"OTEL_SDK_DISABLED": "TRUE"}, False),
            ({"OTEL_SDK_DISABLED": "false"}, True),
        ],
    )
    def test_is_metrics_export_enabled__reads_environment(
        self, monkeypatch, environment, expected
    ) -> None:
        """Combine the SDK disable switch with the exporter name."""
        for name, value in environment.items():
            if value is None:
                monkeypatch.delenv(name, raising=False)
            else:
                monkeypatch.setenv(name, value)
        if (
            "OTEL_METRICS_EXPORTER" not in environment
            and "OTEL_SDK_DISABLED" in environment
        ):
            monkeypatch.delenv("OTEL_METRICS_EXPORTER", raising=False)
        assert metrics.is_metrics_export_enabled() is expected


class TestCreateMetricExporter:
    """Tests for the exporter selection."""

    def test_create_metric_exporter__console(self, monkeypatch) -> None:
        """Build the console exporter on request."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        assert isinstance(metrics.create_metric_exporter(), ConsoleMetricExporter)

    def test_create_metric_exporter__otlp_http_default(self, monkeypatch) -> None:
        """Build the OTLP/HTTP exporter by default."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "otlp")
        assert isinstance(metrics.create_metric_exporter(), OTLPMetricExporter)

    def test_create_metric_exporter__otlp_metrics_protocol_wins(
        self, monkeypatch
    ) -> None:
        """Prefer the metrics-specific protocol and select the gRPC module."""
        calls = []

        def import_module(name):
            calls.append(name)
            return types.SimpleNamespace(OTLPMetricExporter=lambda: "grpc-exporter")

        monkeypatch.setattr(metrics.importlib, "import_module", import_module)
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "otlp")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        assert metrics.create_metric_exporter() == "grpc-exporter"
        assert calls == ["opentelemetry.exporter.otlp.proto.grpc.metric_exporter"]

    def test_create_metric_exporter__otlp_shared_protocol(self, monkeypatch) -> None:
        """Fall back to the shared protocol for OTLP selection."""
        calls = []

        def import_module(name):
            calls.append(name)
            return types.SimpleNamespace(OTLPMetricExporter=lambda: "http-exporter")

        monkeypatch.setattr(metrics.importlib, "import_module", import_module)
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "otlp")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        assert metrics.create_metric_exporter() == "http-exporter"
        assert calls == ["opentelemetry.exporter.otlp.proto.http.metric_exporter"]

    def test_create_metric_exporter__rejects_unknown_protocol(
        self, monkeypatch
    ) -> None:
        """Reject an unknown OTLP protocol with the offending value."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "otlp")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "carrier-pigeon")
        with pytest.raises(
            ValueError, match="Unsupported OTLP protocol: 'carrier-pigeon'"
        ):
            metrics.create_metric_exporter()

    def test_create_metric_exporter__rejects_unknown_exporter(
        self, monkeypatch
    ) -> None:
        """Reject an unknown exporter with the offending value."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "jaeger")
        with pytest.raises(
            ValueError, match="Unsupported OTEL_METRICS_EXPORTER: 'jaeger'"
        ):
            metrics.create_metric_exporter()


class TestReadExportIntervalMs:
    """Tests for the export interval fallback."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, metrics.DEFAULT_EXPORT_INTERVAL_MS),
            ("5000", 5000),
            ("30000", 30000),
            ("0", metrics.DEFAULT_EXPORT_INTERVAL_MS),
            ("-5", metrics.DEFAULT_EXPORT_INTERVAL_MS),
            ("abc", metrics.DEFAULT_EXPORT_INTERVAL_MS),
        ],
    )
    def test_read_export_interval_ms__falls_back_for_invalid_values(
        self, monkeypatch, value, expected
    ) -> None:
        """Honor positive values and fall back for unset, invalid, or non-positive ones."""
        if value is None:
            monkeypatch.delenv("OTEL_METRIC_EXPORT_INTERVAL", raising=False)
        else:
            monkeypatch.setenv("OTEL_METRIC_EXPORT_INTERVAL", value)
        assert metrics.read_export_interval_ms() == expected


class TestConfigureMetrics:
    """Tests for recorder construction."""

    def test_configure_metrics__returns_noop_when_export_disabled(
        self, monkeypatch
    ) -> None:
        """Return the no-op recorder when configuration disables export."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
        assert type(metrics.configure_metrics()) is metrics.Metrics

    def test_configure_metrics__warns_when_sdk_missing(
        self, monkeypatch, caplog
    ) -> None:
        """Warn and fall back when export is enabled without the SDK."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "otlp")
        monkeypatch.setattr(metrics.importlib.util, "find_spec", lambda name: None)
        with caplog.at_level(logging.WARNING, logger="threadmill.metrics"):
            recorder = metrics.configure_metrics()
        assert type(recorder) is metrics.Metrics
        assert "OpenTelemetry SDK is missing" in caplog.text

    @pytest.mark.parametrize(
        ("alias", "expected_alias"), [(None, None), ("default", "default")]
    )
    def test_configure_metrics__builds_sdk_provider(
        self, monkeypatch, alias, expected_alias
    ) -> None:
        """Build a provider with the service, pid, and optional backend alias."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        monkeypatch.setenv("OTEL_METRIC_EXPORT_INTERVAL", "60000")
        monkeypatch.setenv("OTEL_SERVICE_NAME", "threadmill-tests")
        recorder = metrics.configure_metrics(backend_alias=alias)
        try:
            assert isinstance(recorder, metrics.OpenTelemetryMetrics)
            assert isinstance(recorder.meter_provider, MeterProvider)
            attributes = recorder.meter_provider._sdk_config.resource.attributes
            assert attributes["service.name"] == "threadmill-tests"
            assert attributes["process.pid"] == os.getpid()
            assert attributes.get("threadmill.backend.alias") == expected_alias
        finally:
            recorder.shutdown()
