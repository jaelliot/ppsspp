#!/usr/bin/env python3
"""Loopback regression for wsdbg's ticketed replies after graceful disconnect.

Only executes a local WebSocket mock, not PPSSPP or target code.
Usage: python probe_wsdbg_disconnect.py PATH_TO_WSDBG
"""

import asyncio
import json
import sys

from websockets.asyncio.server import serve


async def probe(executable: str, mode: str, close_stdin: bool) -> None:
    observed = []

    async def handler(ws):
        async for frame in ws:
            request = json.loads(frame)
            event = request["event"]
            observed.append(event)
            ticket = request.get("ticket")
            if event in ("client.config.set", "broadcast.config.set", "version"):
                await ws.send(json.dumps({"event": event, "ticket": ticket}))
            elif event == "gpu.stats.get":
                await ws.send(
                    json.dumps(
                        {
                            "event": "deferred",
                            "for": event,
                            "replyFollows": True,
                            "ticket": ticket,
                        }
                    )
                )
                if mode == "success":
                    await ws.send(json.dumps({"event": event, "ticket": ticket}))
                elif mode == "error":
                    await ws.send(
                        json.dumps(
                            {
                                "event": "error",
                                "ticket": ticket,
                                "message": "injected failure",
                            }
                        )
                    )
                await asyncio.sleep(0.15)
                await ws.close()
                break

    async with serve(
        handler, "127.0.0.1", 0, subprotocols=["debugger.ppsspp.org"]
    ) as server:
        port = server.sockets[0].getsockname()[1]
        proc = await asyncio.create_subprocess_exec(
            executable,
            str(port),
            "--compact",
            "--quiet",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert proc.stdin is not None
        proc.stdin.write(b":nowait gpu.stats.get\n")
        await proc.stdin.drain()
        if close_stdin:
            proc.stdin.close()
        try:
            if close_stdin:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
            else:
                await asyncio.wait_for(proc.wait(), timeout=15)
                stdout, stderr = await proc.stdout.read(), await proc.stderr.read()
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise AssertionError(f"wsdbg timed out: {mode=}, {close_stdin=}")

        expected_nonzero = mode != "success"
        assert "gpu.stats.get" in observed, f"No GPU request received: {observed}"
        assert (proc.returncode != 0) == expected_nonzero, (
            f"{mode=}, {close_stdin=}: exit={proc.returncode}, "
            f"stdout={stdout.decode(errors='replace')}, "
            f"stderr={stderr.decode(errors='replace')}"
        )
        print(f"PASS {mode=} {close_stdin=}: exit={proc.returncode}", flush=True)


async def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: probe_wsdbg_disconnect.py PATH_TO_WSDBG")
    for mode in ("success", "error", "missing"):
        for close_stdin in (True, False):
            await probe(sys.argv[1], mode, close_stdin)


if __name__ == "__main__":
    asyncio.run(main())
