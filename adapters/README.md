# Adapter data

Each directory contains a schema-v1 data-only starting manifest. Harness releases change
their flags and event shapes independently, so `ahrb doctor` validates executable/version
compatibility before a run. The `mock` adapter is the normative, fully exercised reference;
the nine external entries deliberately contain no harness-specific Rust code.

