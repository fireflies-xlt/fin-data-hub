from opentelemetry import metrics, trace
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor

# 指标相关导入
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from prometheus_client import start_http_server

from fin_data_hub.config import config

# 全局指标对象
def setup_telemetry(app, service_name=config.service_name, metrics_port=config.telemetry_metrics_port):
    """设置OpenTelemetry"""
    # 创建资源信息
    resource = Resource(attributes={SERVICE_NAME: service_name})

    # --- 设置追踪 (Tracing) ---
    tracer_provider = TracerProvider(resource=resource)

    # 添加控制台导出器(可以换成其他后端)
    tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    # 设置全局追踪提供者
    trace.set_tracer_provider(tracer_provider)

    # --- 设置指标 (Metrics) ---
    # 启动Prometheus指标服务器
    start_http_server(metrics_port)

    # 创建Prometheus指标读取器
    prometheus_reader = PrometheusMetricReader()

    # 创建指标提供者
    metrics_provider = MeterProvider(
        resource=resource, metric_readers=[prometheus_reader]
    )

    # 设置全局指标提供者
    metrics.set_meter_provider(metrics_provider)

    # --- 自动检测 ---
    # 自动检测FastAPI
    FastAPIInstrumentor.instrument_app(app)

    # 自动检测MongoDB
    # PymongoInstrumentor().instrument()
    
    # 自动检测系统指标 (CPU, 内存, 磁盘, 网络等)
    SystemMetricsInstrumentor().instrument()


def get_service_meter(service_name=config.service_name):
    return metrics.get_meter(service_name)
