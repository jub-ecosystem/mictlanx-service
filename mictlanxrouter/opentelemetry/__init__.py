from opentelemetry.sdk.trace.export import ConsoleSpanExporter

class NoOpSpanExporter(ConsoleSpanExporter):
    def export(self, spans):
        pass