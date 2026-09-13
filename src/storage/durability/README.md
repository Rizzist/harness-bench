S2 Darwin collector
===================

`build.rs` compiles two C libraries with the system compiler and embeds them in
AHRB. The control library imports the OS functions independently; the interposer
must observe exactly three successful fsyncs, two successful F_FULLFSYNCs and two
EBADF failures of each (including cancellation variants) in every process-image initialization. Where the runtime
exports fdatasync, its success and EBADF failure are also required. These calls use a
disposable file outside the measured profile, are tagged self_test and excluded
from the measured intervals. The control also exercises all three fcntl argument
forms to catch a broken variadic forwarding ABI.

The arm64 fcntl trampolines forward non-F_FULLFSYNC calls without reading an
optional argument. fsync and fcntl cancellation variants are hooked; nested calls
have a thread-local guard. The SDK does not expose fdatasync in unistd.h, but some Darwin runtimes export
it. A weak import and runtime control determine its applicability; available
fdatasync calls are intercepted, never silently treated as inapplicable. The estimate is always the comparison assumption 4 ms/call.
It is neither measured latency nor a hardware or crash-safety claim.

Each image stream has PID/start-time, ordered records, an independently verified
control, executable-path receipt, and a clean end/drop counter. Process spawning
records its monotonic interval and requires a distinct subsequent child PID/start
identity control. PID reuse, earlier images and multiple exec images cannot stand
in for a missing spawned process. The runner cross-checks sampled owned identities.
An exec needs the successor image's load receipt to replace the old image's end.
A fork currently emits an explicit coverage gap: the collector cannot safely run
its loader/control in a fork-only child of a multithreaded program. Missing image
receipts, fork gaps and lost records after preflight are ERROR. Raw records remain
in the bundle even if typed validation fails. Direct arm64 supervisor instructions
in application images (including loaded libraries) cause a coverage gap. The
system library ABI supplies the intercepted primitives; system library internals
are not instrumented as separate calls.

Preflight runs the resolved executable with constructor-only control and an empty
HOME. When injection is denied, the manifest's version arguments are the only
normal CLI stimulus. A missing handshake, launch status and stderr are recorded;
no zero count or reason such as hardened runtime is guessed from environment
presence. No permissions, signature or SIP setting is changed.

Current platform limits: only Darwin arm64 is built here. Linux probes strace
availability; unavailable tracing is os-limited, while an installed tracer with
an unimplemented/unchecked backend is an explicit collector error. A Linux
owned-tree tracing implementation and execution are still outstanding. Do not
claim parity from the Darwin tests.

Reference ABI documentation:
https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html
https://developer.apple.com/library/archive/documentation/DeveloperTools/Conceptual/DynamicLibraries/100-Articles/UsingDynamicLibraries.html
