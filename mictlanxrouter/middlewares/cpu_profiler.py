
import os
import time
from starlette.types import ASGIApp, Receive, Scope, Send
from pyinstrument import Profiler
# from pyinstrument.renderers.html import HTMLRenderer

class CPUProfilerMiddleware:
    def __init__(
        self, 
        app: ASGIApp, 
        output_dir: str = "profiles",
        enable_by_default: bool = False
    ):
        self.app = app
        self.output_dir = output_dir
        self.enable_by_default = enable_by_default
        
        # Ensure output directory exists
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Check for query param ?profile=true
        query_string = scope.get("query_string", b"").decode()
        should_profile = "profile=true" in query_string.lower() or self.enable_by_default

        if not should_profile:
            await self.app(scope, receive, send)
            return

        # Start Profiling this specific request
        profiler = Profiler(interval=0.001, async_mode="enabled")
        profiler.start()

        # Wrap the 'send' function to stop profiling when response finishes
        async def wrapped_send(message):
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                # Request finished
                profiler.stop()
                self._save_profile(profiler, scope)
            
            await send(message)

        try:
            await self.app(scope, receive, wrapped_send)
        except Exception:
            profiler.stop()
            self._save_profile(profiler, scope)
            raise

    def _save_profile(self, profiler, scope):
        try:
            # Generate a filename: GET_api_v4_buckets_timestamp.html
            path = scope.get("path", "unknown").strip("/").replace("/", "_")
            method = scope.get("method", "REQ")
            timestamp = int(time.time() * 1000)
            filename = f"{method}_{path}_{timestamp}.html"
            filepath = os.path.join(self.output_dir, filename)

            html = profiler.output_html()
            
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html)
            
            # Print to stdout so you know where to look
            print(f"[PROFILER] Saved report to: {filepath}")
            
        except Exception as e:
            print(f"[PROFILER] Failed to save report: {e}")