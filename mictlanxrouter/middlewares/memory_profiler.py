import os
import time
import memray
from memray import FileDestination
from starlette.types import ASGIApp, Receive, Scope, Send
import subprocess

class MemoryProfilerMiddleware:
    def __init__(
        self, 
        app: ASGIApp, 
        output_dir: str = "profiles",
        enable_by_default: bool = False,
        report_args:str = "",
        cleanup_bin: bool = True,
        trace_python_allocators: bool = True,
        # show_leaks: bool = False
    ):
        self.app = app
        self.output_dir = output_dir
        self.enable_by_default = enable_by_default
        self.report_args = report_args
        self.cleanup_bin = cleanup_bin
        self.trace_python_allocators = trace_python_allocators
        # self.show_leaks = show_leaks
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Check for query param ?profile_memory=true
        query_string   = scope.get("query_string", b"").decode()
        should_profile = "profile_memory=true" in query_string.lower() or self.enable_by_default

        if not should_profile:
            await self.app(scope, receive, send)
            return

        # Prepare filenames
        path      = scope.get("path", "unknown").strip("/").replace("/", "_")
        method    = scope.get("method", "REQ")
        timestamp = int(time.time() * 1000)
        
        # Memray needs a binary file first
        bin_filename = f"MEM_{method}_{path}_{timestamp}.bin"
        html_filename = f"MEM_{method}_{path}_{timestamp}.html"
        
        bin_path = os.path.join(self.output_dir, bin_filename)
        html_path = os.path.join(self.output_dir, html_filename)

        print(f"[MEMRAY] Starting memory tracking for {path}...")

        # --- START TRACKING ---
        try:
            with memray.Tracker(destination=FileDestination(bin_path, overwrite=True),trace_python_allocators=self.trace_python_allocators):
                await self.app(scope, receive, send)
        except Exception as e:
            print(f"[MEMRAY] Error during profiling: {e}")
            raise
        
        # --- GENERATE HTML REPORT ---
        try:
            self._generate_html(bin_path, html_path)
            print(f"[MEMRAY] Report generated: {html_path}")
            
            # Clean up the binary file to save space (optional)
            if self.cleanup_bin and os.path.exists(bin_path):
                os.remove(bin_path)
                print(f"[MEMRAY] Deleted temporary binary: {bin_filename}")

                
        except Exception as e:
            print(f"[MEMRAY] Failed to convert to HTML: {e}")

    def _generate_html(self, bin_path, html_path):
        """
        Programmatically convert the bin dump to an HTML flamegraph
        using Memray's CLI interface (more stable than internal API).
        """
        if not os.path.exists(bin_path):
            return

        # Build command: memray flamegraph -f [-l] -o output.html input.bin
        cmd = ["memray", "flamegraph"]

        # Handle your custom parameter
        if self.report_args:
            cmd.extend(self.report_args.split())
        cmd.extend(["-o", html_path, bin_path])

        try:
            # Run the command and capture output so it doesn't leak to stdout
            subprocess.run(
                cmd, 
                check=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode() if e.stderr else str(e)
            print(f"[MEMRAY] CLI conversion failed: {error_msg}")
            raise RuntimeError(f"Memray CLI failed: {error_msg}")