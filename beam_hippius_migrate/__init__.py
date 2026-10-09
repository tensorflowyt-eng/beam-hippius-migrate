"""beam-hippius-migrate — Migrate data through Beam using Hippius storage.

A thin CLI over beam-network-sdk that demonstrates cross-subnet composability:
Hippius (SN75) provides the storage layer, Beam (SN105) provides the verified
transport layer. The result is a subnet-attested data migration with throughput
proofs from Beam's PRISM scoring system.

Usage:
    beam-hippius-migrate \
        --source-bucket my-bucket --source-key datasets/file.bin \
        --dest-bucket archive --dest-key imports/file.bin \
        --hippius-token $HIPPIUS_API_TOKEN \
        --beam-api-key $BEAM_API_KEY
"""

__version__ = "1.0.0"
