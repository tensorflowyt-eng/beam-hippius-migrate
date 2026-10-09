# beam-hippius-migrate

**Migrate data through Beam (SN105) using Hippius (SN75) storage.**

A thin CLI that demonstrates Bittensor cross-subnet composability: Hippius
provides the encrypted S3-compatible storage layer, Beam provides the verified
bandwidth transport layer with PRISM-scored throughput proofs.

```
  Hippius S3 ──► Beam Transfer ──► Hippius S3
   (source)      (verified)        (destination)
      SN75          SN105              SN75
```

Every transfer through Beam is PRISM-scored — the subnet attests to the
throughput and reliability of the data move. This is a multi-subnet
attestation that raw S3 copying cannot provide.

## Install

```bash
pip install beam-hippius-migrate
```

Or from source:

```bash
tar xzf beam-hippius-migrate-1.0.0.tar.gz
cd beam-hippius-migrate-1.0.0
pip install -e .
```

Dependencies: `beam-network-sdk[s3]` (includes `boto3` for S3 signing).

## Prerequisites

- **Beam API key** — get one from [Beam Console](https://console.b1m.ai) (starts with `b1m_`)
- **Hippius S3 credentials** — from [Hippius Console](https://console.hippius.com) → S3 → Create Master Token (access key starts with `hip_`, plus a secret key)

## Usage

### Single file

```bash
beam-hippius-migrate \
  --source-bucket my-bucket --source-key datasets/file.bin \
  --dest-bucket archive --dest-key imports/file.bin \
  --s3-access-key $HIPPIUS_S3_ACCESS_KEY \
  --s3-secret-key $HIPPIUS_S3_SECRET_KEY \
  --beam-api-key $BEAM_API_KEY
```

### Batch mode

Create a JSON manifest:

```json
[
  {"source_bucket":"datasets","source_key":"f1.bin","dest_bucket":"archive","dest_key":"f1.bin"},
  {"source_bucket":"datasets","source_key":"f2.bin","dest_bucket":"archive","dest_key":"f2.bin"}
]
```

Run:

```bash
beam-hippius-migrate --batch manifest.json \
  --s3-access-key $HIPPIUS_S3_ACCESS_KEY \
  --s3-secret-key $HIPPIUS_S3_SECRET_KEY \
  --beam-api-key $BEAM_API_KEY
```

### JSON output (for scripting)

```bash
beam-hippius-migrate ... --json | jq '.transfer_id'
```

### Dry run

```bash
beam-hippius-migrate ... --dry-run
```

## Environment variables

| Variable | Flag equivalent |
|---|---|
| `BEAM_API_KEY` | `--beam-api-key` |
| `HIPPIUS_S3_ACCESS_KEY` | `--s3-access-key` |
| `HIPPIUS_S3_SECRET_KEY` | `--s3-secret-key` |

## How it works

1. The CLI authenticates to BeamCore over NATS (`tls://orch-gateway.b1m.ai:4222`) using your Beam API key.
2. It uses Beam's Python SDK with the S3-compatible provider to prepare a
   provider transfer — the SDK creates task-scoped presigned source/destination
   URLs via Hippius's S3 endpoint and submits the transfer plan to BeamCore.
3. BeamCore assigns transfer chunks to orchestrator→worker pipelines across
   the subnet. Workers move bytes directly between Hippius storage endpoints
   — they never see your S3 credentials.
4. The CLI polls transfer status and displays real-time progress.
5. On completion, the transfer receipt includes the transfer ID, delivered
   bytes, task counts, and elapsed time. Beam's PRISM system independently
   scores the throughput for emission weighting.

## Architecture

```
┌─────────────┐     ┌─────────────────────────────┐     ┌─────────────┐
│  Hippius    │     │          Beam (SN105)        │     │  Hippius    │
│  (SN75)     │     │                             │     │  (SN75)     │
│             │     │  BeamCore ──► Orchestrator   │     │             │
│  S3 source  │◄───►│     │                        │◄───►│  S3 dest    │
│             │     │     └──► Worker (chunk move) │     │             │
└─────────────┘     │                             │     └─────────────┘
                    │  PRISM scores throughput     │
                    │  for emission weighting      │
                    └─────────────────────────────┘
```

The tool uses `beam-network-sdk` (Beam's official Python SDK) with
`S3CompatibleProviderSource` / `S3CompatibleProviderDestination` configured
for Hippius's S3-compatible endpoint (`https://s3.hippius.com`, region
`decentralized`, path-style addressing). No custom transport code, no
reimplemented APIs — the SDK handles presigning, chunking, and route
attachment; BeamCore handles orchestration and PRISM scoring.

## Verified

Tested live on 2026-10-09: two transfers (2.0 MiB and 3.0 MiB) completed
through Beam's production network with byte-identical MD5 verification
between source and destination on Hippius.

## License

MIT
