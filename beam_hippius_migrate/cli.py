#!/usr/bin/env python3
"""beam-hippius-migrate — Migrate data through Beam (SN105) using Hippius (SN75) storage.

A CLI tool that demonstrates Bittensor cross-subnet composability:
  - Hippius provides S3-compatible encrypted object storage
  - Beam provides verified bandwidth transport with PRISM-scored throughput proofs

The tool uses Beam's own Python SDK (beam-network-sdk) with its native Hippius
connector — no custom transport code, no reimplemented APIs. It submits a
provider transfer through BeamCore, watches it complete, and prints a summary
with the transfer receipt.

Requirements:
    pip install beam-network-sdk

Usage:
    beam-hippius-migrate \\
        --source-bucket my-bucket --source-key datasets/file.bin \\
        --dest-bucket archive --dest-key imports/file.bin \\
        --hippius-token $HIPPIUS_API_TOKEN \\
        --beam-api-key $BEAM_API_KEY

    # Multiple files (batch mode):
    beam-hippius-migrate --batch manifest.json \\
        --hippius-token $HIPPIUS_API_TOKEN --beam-api-key $BEAM_API_KEY

Environment variables (alternative to flags):
    BEAM_API_KEY     — BeamCore API key (b1m_...)
    HIPPIUS_API_TOKEN — Hippius API token (hip_...)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any

# -- SDK imports (graceful if not installed) ---------------------------------

try:
    from beam_network_sdk import (
        BeamSDK,
        BeamError,
        BeamTransferError,
        BeamTransferFailedError,
        BeamTimeoutError,
        S3CompatibleProviderSource,
        S3CompatibleProviderDestination,
    )
except ImportError:
    print(
        "ERROR: beam-network-sdk is not installed.\n"
        "       Install it with:  pip install beam-network-sdk[s3]\n",
        file=sys.stderr,
    )
    sys.exit(2)


__version__ = "1.0.0"

# -- ANSI --------------------------------------------------------------------

_IS_TTY = sys.stderr.isatty()

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _IS_TTY else text

def dim(t: str) -> str:    return _c("2;90", t)
def bold(t: str) -> str:   return _c("1", t)
def green(t: str) -> str:  return _c("1;32", t)
def red(t: str) -> str:    return _c("1;31", t)
def yellow(t: str) -> str: return _c("1;33", t)
def cyan(t: str) -> str:   return _c("1;36", t)


# -- Helpers -----------------------------------------------------------------

def fmt_bytes(n: int) -> str:
    """Human-readable byte count."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n = int(n / 1024)
    return f"{n:.1f} PiB"


def fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s"


def fmt_throughput(bytes_done: int, elapsed_s: float) -> str:
    if elapsed_s <= 0:
        return "—"
    return f"{fmt_bytes(int(bytes_done / elapsed_s))}/s"


# -- Banner ------------------------------------------------------------------

BANNER = r"""
  ╦  ╦╔═╗╦  ╦╔╦╗╔═╗╦═╗╦╔╗╦╔═╗╦ ╦
  ║  ║╠═╝║  ║ ║ ║  ╠╦╝║╚╩║╠═╣║║║
  ╩═╝╩╩  ╩═╝╩ ╩ ╚═╝╩╚═╩╩ ╩╩ ╩╚═╝
  Hippius SN75 × Beam SN105 — verified data migration
"""


def print_banner() -> None:
    lines = BANNER.strip("\n").split("\n")
    for line in lines:
        print(dim(line), file=sys.stderr)
    print(file=sys.stderr)


# -- Progress ----------------------------------------------------------------

@dataclass
class ProgressState:
    """Tracks transfer progress for display."""
    transfer_id: str = ""
    status: str = "pending"
    bytes_total: int = 0
    bytes_done: int = 0
    tasks_total: int = 0
    tasks_done: int = 0
    started_at: float = field(default_factory=time.monotonic)
    last_print: float = 0.0

    def update(self, info: Any) -> None:
        self.status = getattr(info, "status", self.status)
        self.bytes_total = getattr(info, "source_bytes_total", 0) or self.bytes_total
        self.bytes_done = getattr(info, "delivery_bytes_completed", 0) or self.bytes_done
        self.tasks_total = getattr(info, "delivery_tasks_total", 0) or self.tasks_total
        self.tasks_done = getattr(info, "delivery_tasks_completed", 0) or self.tasks_done

    def render(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self.last_print) < 2.0:
            return
        self.last_print = now
        elapsed = now - self.started_at

        pct = ""
        if self.bytes_total > 0:
            pct = f"  {self.bytes_done / self.bytes_total * 100:5.1f}%"
        elif self.tasks_total > 0:
            pct = f"  {self.tasks_done}/{self.tasks_total} tasks"

        tp = fmt_throughput(self.bytes_done, elapsed)
        bar_w = 28
        filled = 0
        if self.bytes_total > 0:
            filled = int(bar_w * min(1.0, self.bytes_done / self.bytes_total))
        elif self.tasks_total > 0:
            filled = int(bar_w * min(1.0, self.tasks_done / max(1, self.tasks_total)))

        bar = "█" * filled + "░" * (bar_w - filled)
        print(
            f"\r  {dim('│')} {cyan(bar)}{dim('│')}{pct}  {dim(tp)}  {dim(self.status)}",
            end="",
            file=sys.stderr,
            flush=True,
        )

    def finish(self) -> None:
        self.render(force=True)
        print(file=sys.stderr)


# -- Diagnostics callback ----------------------------------------------------

def make_diagnostics_cb(state: ProgressState) -> Any:
    """Returns an async callback for BeamSDK(on_diagnostics=...)."""
    async def _cb(diag: dict[str, Any]) -> None:
        # Diagnostics carry internal SDK events; we surface progress via status polling.
        pass
    return _cb


# -- Core migration logic ----------------------------------------------------

async def migrate_one(
    beam: BeamSDK,
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_endpoint: str,
    s3_region: str,
    name: str | None,
    wait_timeout: float,
    poll_interval: float,
) -> dict[str, Any]:
    """Submit a single Hippius→Hippius transfer through Beam and wait for completion.

    Hippius is S3-compatible, so we use the S3-compatible provider path which
    sends the full resource metadata BeamCore requires.
    """

    src = S3CompatibleProviderSource(
        provider="hippius",
        bucket=source_bucket,
        key=source_key,
        endpoint_url=s3_endpoint,
        region=s3_region,
        access_key_id=s3_access_key,
        secret_access_key=s3_secret_key,
        force_path_style=True,
    )
    dst = S3CompatibleProviderDestination(
        provider="hippius",
        bucket=dest_bucket,
        key=dest_key,
        endpoint_url=s3_endpoint,
        region=s3_region,
        access_key_id=s3_access_key,
        secret_access_key=s3_secret_key,
        force_path_style=True,
    )

    label = name or f"hippius-migrate:{source_bucket}/{source_key}"
    print(f"  {bold('▸')} Submitting transfer  {dim(label)}", file=sys.stderr)

    result = await beam.transfers.prepare_provider_transfer(
        sources=[src],
        destinations=[dst],
        name=label,
        distribute=True,
    )

    if not result.success:
        raise BeamTransferError(
            f"Transfer preparation failed: {result.error or result.message}"
        )

    transfer_id = result.transfer_id
    total_size = result.total_size
    total_chunks = result.total_chunks

    print(
        f"  {bold('▸')} Transfer accepted   "
        f"{dim('id=')}{cyan(transfer_id)}  "
        f"{dim('size=')}{fmt_bytes(total_size)}  "
        f"{dim('chunks=')}{total_chunks}",
        file=sys.stderr,
    )

    # Watch progress
    state = ProgressState(transfer_id=transfer_id, bytes_total=total_size)
    state.render(force=True)

    terminal = await beam.transfers.open_terminal_signal_waiter(transfer_id)

    while True:
        # Poll status for progress display
        try:
            info = await beam.transfers.status(transfer_id)
            state.update(info)
            state.render()
        except Exception:
            pass

        # Check terminal signal (non-blocking-ish, short timeout)
        event = await terminal.wait(timeout=poll_interval)
        if event is not None:
            state.status = event.status
            state.finish()
            break

        elapsed = time.monotonic() - state.started_at
        if elapsed > wait_timeout:
            await terminal.close()
            state.finish()
            raise BeamTimeoutError(
                f"Transfer {transfer_id} timed out after {fmt_duration(elapsed)}"
            )

    await terminal.close()

    # Final status
    final = await beam.transfers.status(transfer_id)
    return {
        "transfer_id": transfer_id,
        "status": final.status,
        "source": f"{source_bucket}/{source_key}",
        "destination": f"{dest_bucket}/{dest_key}",
        "total_bytes": final.source_bytes_total,
        "delivered_bytes": final.delivery_bytes_completed,
        "tasks_total": final.delivery_tasks_total,
        "tasks_completed": final.delivery_tasks_completed,
        "elapsed_ms": getattr(final, "performance", None)
        and final.performance.elapsed_ms
        or None,
        "name": label,
    }


async def migrate_batch(
    beam: BeamSDK,
    manifest_path: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_endpoint: str,
    s3_region: str,
    wait_timeout: float,
    poll_interval: float,
) -> list[dict[str, Any]]:
    """Run a batch migration from a JSON manifest."""
    with open(manifest_path) as f:
        manifest = json.load(f)

    if not isinstance(manifest, list):
        raise ValueError("Batch manifest must be a JSON array of transfer objects")

    results: list[dict[str, Any]] = []
    total = len(manifest)

    for i, item in enumerate(manifest, 1):
        print(
            f"\n  {bold(f'[{i}/{total}]')} {dim(item.get('source_key', '?'))} "
            f"→ {dim(item.get('dest_key', '?'))}",
            file=sys.stderr,
        )
        result = await migrate_one(
            beam=beam,
            source_bucket=item["source_bucket"],
            source_key=item["source_key"],
            dest_bucket=item["dest_bucket"],
            dest_key=item["dest_key"],
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_endpoint=s3_endpoint,
            s3_region=s3_region,
            name=item.get("name"),
            wait_timeout=wait_timeout,
            poll_interval=poll_interval,
        )
        results.append(result)

    return results


# -- Result rendering --------------------------------------------------------

def print_result(result: dict[str, Any]) -> None:
    status = result["status"]
    status_str = green(f"✓ {status}") if status == "completed" else red(f"✗ {status}")

    print(f"\n  {'─' * 52}", file=sys.stderr)
    print(f"  {bold('Transfer receipt')}", file=sys.stderr)
    print(f"  {'─' * 52}", file=sys.stderr)
    print(f"  {dim('ID            ')} {cyan(result['transfer_id'])}", file=sys.stderr)
    print(f"  {dim('Status         ')} {status_str}", file=sys.stderr)
    print(f"  {dim('Source         ')} {result['source']}", file=sys.stderr)
    print(f"  {dim('Destination    ')} {result['destination']}", file=sys.stderr)
    print(
        f"  {dim('Delivered      ')} {fmt_bytes(result['delivered_bytes'])}"
        f" / {fmt_bytes(result['total_bytes'])}",
        file=sys.stderr,
    )
    print(
        f"  {dim('Tasks          ')} {result['tasks_completed']}/{result['tasks_total']}",
        file=sys.stderr,
    )
    if result.get("elapsed_ms"):
        elapsed_s = result["elapsed_ms"] / 1000
        tp = fmt_throughput(result["delivered_bytes"], elapsed_s)
        print(
            f"  {dim('Elapsed        ')} {fmt_duration(elapsed_s)}  {dim(f'({tp})')}",
            file=sys.stderr,
        )
    print(f"  {'─' * 52}\n", file=sys.stderr)


def print_batch_summary(results: list[dict[str, Any]]) -> None:
    completed = sum(1 for r in results if r["status"] == "completed")
    failed = len(results) - completed

    print(f"\n  {'═' * 52}", file=sys.stderr)
    print(f"  {bold('Batch summary')}", file=sys.stderr)
    print(f"  {'═' * 52}", file=sys.stderr)
    print(f"  {dim('Total    ')} {len(results)}", file=sys.stderr)
    print(f"  {dim('Completed')} {green(str(completed))}", file=sys.stderr)
    if failed:
        print(f"  {dim('Failed   ')} {red(str(failed))}", file=sys.stderr)
    total_bytes = sum(r["delivered_bytes"] for r in results if r["status"] == "completed")
    print(f"  {dim('Moved    ')} {fmt_bytes(total_bytes)}", file=sys.stderr)
    print(f"  {'═' * 52}\n", file=sys.stderr)

    for r in results:
        icon = green("✓") if r["status"] == "completed" else red("✗")
        print(f"  {icon} {dim(r['source'])} → {dim(r['destination'])}", file=sys.stderr)
    print(file=sys.stderr)


# -- CLI ---------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="beam-hippius-migrate",
        description=(
            "Migrate data through Beam (SN105) using Hippius (SN75) storage.\n"
            "Demonstrates Bittensor cross-subnet composability with verified transport."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Single file migration:\n"
            "  beam-hippius-migrate \\\n"
            "    --source-bucket datasets --source-key file.bin \\\n"
            "    --dest-bucket archive --dest-key imports/file.bin \\\n"
            "    --s3-access-key $HIPPIUS_S3_ACCESS_KEY \\\n"
            "    --s3-secret-key $HIPPIUS_S3_SECRET_KEY \\\n"
            "    --beam-api-key $BEAM_API_KEY\n\n"
            "  # Batch mode (JSON manifest):\n"
            "  beam-hippius-migrate --batch manifest.json \\\n"
            "    --s3-access-key $HIPPIUS_S3_ACCESS_KEY \\\n"
            "    --s3-secret-key $HIPPIUS_S3_SECRET_KEY \\\n"
            "    --beam-api-key $BEAM_API_KEY\n\n"
            "  manifest.json format:\n"
            "  [\n"
            '    {"source_bucket":"a","source_key":"f1","dest_bucket":"b","dest_key":"f1"},\n'
            '    {"source_bucket":"a","source_key":"f2","dest_bucket":"b","dest_key":"f2"}\n'
            "  ]\n"
        ),
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    # Source
    src = p.add_argument_group("source (Hippius)")
    src.add_argument("--source-bucket", help="Source Hippius bucket")
    src.add_argument("--source-key", help="Source object key")

    # Destination
    dst = p.add_argument_group("destination (Hippius)")
    dst.add_argument("--dest-bucket", help="Destination Hippius bucket")
    dst.add_argument("--dest-key", help="Destination object key")

    # Batch
    p.add_argument("--batch", help="JSON manifest file for batch mode")

    # Credentials
    cred = p.add_argument_group("credentials")
    cred.add_argument(
        "--beam-api-key",
        default=os.environ.get("BEAM_API_KEY"),
        help="BeamCore API key (env: BEAM_API_KEY)",
    )
    cred.add_argument(
        "--s3-access-key",
        default=os.environ.get("HIPPIUS_S3_ACCESS_KEY"),
        help="Hippius S3 access key id (env: HIPPIUS_S3_ACCESS_KEY)",
    )
    cred.add_argument(
        "--s3-secret-key",
        default=os.environ.get("HIPPIUS_S3_SECRET_KEY"),
        help="Hippius S3 secret access key (env: HIPPIUS_S3_SECRET_KEY)",
    )

    # Options
    opt = p.add_argument_group("options")
    opt.add_argument(
        "--s3-endpoint",
        default="https://s3.hippius.com",
        help="Hippius S3 endpoint (default: https://s3.hippius.com)",
    )
    opt.add_argument(
        "--s3-region",
        default="decentralized",
        help="Hippius S3 region (default: decentralized)",
    )
    opt.add_argument(
        "--beam-env",
        default="prod",
        help="Beam environment: 'prod' or 'dev' (default: prod)",
    )
    opt.add_argument(
        "--nats-url",
        default=None,
        help="Override NATS URL (default: tls://orch-gateway.b1m.ai:4222 in prod)",
    )
    opt.add_argument("--name", help="Transfer name (for identification)")
    opt.add_argument(
        "--timeout",
        type=float,
        default=3600.0,
        help="Per-transfer timeout in seconds (default: 3600)",
    )
    opt.add_argument(
        "--poll-interval",
        type=float,
        default=10.0,
        help="Status poll interval in seconds (default: 10)",
    )
    opt.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON to stdout (for scripting)",
    )
    opt.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate arguments and exit (no transfer submitted)",
    )

    return p


def validate_args(args: argparse.Namespace) -> None:
    """Validate argument combinations."""
    if args.batch:
        return  # Batch mode — source/dest come from manifest

    missing = []
    if not args.source_bucket:
        missing.append("--source-bucket")
    if not args.source_key:
        missing.append("--source-key")
    if not args.dest_bucket:
        missing.append("--dest-bucket")
    if not args.dest_key:
        missing.append("--dest-key")

    if missing:
        print(
            f"ERROR: Missing required arguments: {', '.join(missing)}\n"
            "       (or use --batch for batch mode)",
            file=sys.stderr,
        )
        sys.exit(2)

    if not args.beam_api_key:
        print(
            "ERROR: Beam API key required (--beam-api-key or BEAM_API_KEY env)",
            file=sys.stderr,
        )
        sys.exit(2)

    if not args.s3_access_key:
        print(
            "ERROR: Hippius S3 access key required (--s3-access-key or HIPPIUS_S3_ACCESS_KEY env)",
            file=sys.stderr,
        )
        sys.exit(2)

    if not args.s3_secret_key:
        print(
            "ERROR: Hippius S3 secret key required (--s3-secret-key or HIPPIUS_S3_SECRET_KEY env)",
            file=sys.stderr,
        )
        sys.exit(2)


async def run(args: argparse.Namespace) -> int:
    """Main async entry point."""
    validate_args(args)

    if not args.json:
        print_banner()

    if args.dry_run:
        if not args.json:
            print(f"  {yellow('◎')} Dry run — arguments valid, no transfer submitted.", file=sys.stderr)
            if args.batch:
                with open(args.batch) as f:
                    manifest = json.load(f)
                print(f"  {dim('Batch entries:')} {len(manifest)}", file=sys.stderr)
            else:
                print(
                    f"  {dim('Source:      ')} {args.source_bucket}/{args.source_key}",
                    file=sys.stderr,
                )
                print(
                    f"  {dim('Destination: ')} {args.dest_bucket}/{args.dest_key}",
                    file=sys.stderr,
                )
            print(f"  {dim('Beam env:    ')} {args.beam_env}", file=sys.stderr)
            print(f"  {dim('NATS URL:    ')} {args.nats_url or '(default for env)'}", file=sys.stderr)
            print(f"  {dim('S3 endpoint: ')} {args.s3_endpoint}", file=sys.stderr)
            print(f"  {dim('S3 region:   ')} {args.s3_region}", file=sys.stderr)
        return 0

    if not args.json:
        print(
            f"  {bold('▸')} Connecting to Beam  {dim('env=' + args.beam_env)}",
            file=sys.stderr,
        )

    beam = BeamSDK(
        api_key=args.beam_api_key,
        environment=args.beam_env,
        nats_url=args.nats_url,
        timeout=60.0,
        on_diagnostics=make_diagnostics_cb(ProgressState()),
    )

    exit_code = 0

    try:
        if args.batch:
            if not args.json:
                print(f"  {bold('▸')} Batch mode  {dim(args.batch)}", file=sys.stderr)
            results = await migrate_batch(
                beam=beam,
                manifest_path=args.batch,
                s3_access_key=args.s3_access_key,
                s3_secret_key=args.s3_secret_key,
                s3_endpoint=args.s3_endpoint,
                s3_region=args.s3_region,
                wait_timeout=args.timeout,
                poll_interval=args.poll_interval,
            )
            if args.json:
                print(json.dumps(results, indent=2))
            else:
                print_batch_summary(results)
            if any(r["status"] != "completed" for r in results):
                exit_code = 1
        else:
            result = await migrate_one(
                beam=beam,
                source_bucket=args.source_bucket,
                source_key=args.source_key,
                dest_bucket=args.dest_bucket,
                dest_key=args.dest_key,
                s3_access_key=args.s3_access_key,
                s3_secret_key=args.s3_secret_key,
                s3_endpoint=args.s3_endpoint,
                s3_region=args.s3_region,
                name=args.name,
                wait_timeout=args.timeout,
                poll_interval=args.poll_interval,
            )
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                print_result(result)
            if result["status"] != "completed":
                exit_code = 1

    except BeamTransferFailedError as e:
        if not args.json:
            print(f"\n  {red('✗ Transfer failed')}: {e}", file=sys.stderr)
        else:
            print(json.dumps({"error": str(e), "type": "transfer_failed"}, indent=2))
        exit_code = 1
    except BeamTimeoutError as e:
        if not args.json:
            print(f"\n  {red('✗ Timeout')}: {e}", file=sys.stderr)
        else:
            print(json.dumps({"error": str(e), "type": "timeout"}, indent=2))
        exit_code = 1
    except BeamError as e:
        if not args.json:
            print(f"\n  {red('✗ Beam error')}: {e}", file=sys.stderr)
        else:
            print(json.dumps({"error": str(e), "type": "beam_error"}, indent=2))
        exit_code = 1
    except Exception as e:
        if not args.json:
            print(f"\n  {red('✗ Error')}: {e}", file=sys.stderr)
        else:
            print(json.dumps({"error": str(e), "type": "error"}, indent=2))
        exit_code = 1

    finally:
        await beam.close()

    return exit_code


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = asyncio.run(run(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
