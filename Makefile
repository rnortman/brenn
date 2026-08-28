.PHONY: setup-hooks scrub-selfcheck scrub-tree check bazel-check bazel-dsl-coherence bazel-release bazel-release-dir bazel-policy-parity xtask-deny build run-artifacts clean launchdev stopdev npm-audit e2e
# Delete partially-written targets on recipe failure. Without this, a failing
# recipe leaves the target file with a fresh mtime, causing subsequent
# incremental builds to skip it entirely.
.DELETE_ON_ERROR:

# Build identifier shared between the Rust binary (env! in
# brenn/src/build_info.rs) and the JS bundle (esbuild --define).
# Both ends must match for the stale-tab force-refresh handshake to
# green-light a WS connect. `bazel/workspace_status.sh` reads this and refuses
# to stamp an unidentifiable build; CI and the deploy pipeline set it from the
# resolved version, local dev falls back to the short git SHA so same-checkout
# builds handshake cleanly.
BRENN_BUILD_ID ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown-dev)
export BRENN_BUILD_ID

# Extra flags for every bazel invocation. Public CI sets --config=ci, the
# private deploy pipeline --config=cd; locally the .bazelrc defaults apply.
BAZEL_CONFIG ?=

# Where bazel plants its convenience symlinks (--symlink_prefix in .bazelrc).
# Every path below that names a build output goes through this, because the
# configuration segment of a real output path is Bazel's to choose.
BAZEL_BIN := .bazel-bin

# The Bazel gate.
#
# Three invocations, not five. The host lane is one: the clippy and rustfmt
# aspects apply to the same `//...` in the same configuration the tests build
# in, so requesting them alongside `test` loads and analyzes that graph once and
# schedules one action pool instead of three back-to-back. Action keys are what
# the disk cache stores, not invocation shapes, so the verdict and the cached
# results are identical either way.
#
# The two wasm lanes stay separate because they are not the same graph: both
# wasm trees are reached through a platform transition (the component rule's,
# the wasm-bindgen rule's), and an aspect only applies to top-level targets, so
# clippy needs its own request under the wasm32 platform for each.
bazel-check: bazel-dsl-coherence
	bazel test $(BAZEL_CONFIG) --config=clippy --config=rustfmt //...
	bazel build $(BAZEL_CONFIG) --config=clippy --platforms=//bazel/platforms:wasm32 //brenn-wasm/components/...
	bazel build $(BAZEL_CONFIG) --config=clippy --platforms=//bazel/platforms:wasm32 //surface/...

# What //brenn-dsl links out of the fltk module, asserted over the resolved
# dependency graph.
#
# Two things a test target cannot see. First, pyo3: fltk ships two flavors of
# each runtime crate and only the `:no_python` ones may appear here — a
# pyo3-flavor label compiles fine and would ship unflagged, so nothing else
# would catch it. Second, serde: `fltk-serde-core` takes its serde from a
# label_flag that `.bazelrc` points at `@crates//:serde`, and with the flag
# unset the crate silently links fltk's module-private hub instead. That one
# does fail the build, as a wall of "two different versions of crate `serde`" —
# this turns it into a named policy failure.
#
# Every reachability question is asked in labels, not in the text of labels.
# Repo names are bzlmod's to mangle, so a grep over raw cquery output either
# invents failures on a rules_rust bump or, for the negative assertions, quietly
# stops guarding anything. `somepath` resolves through Bazel's own label
# machinery instead, and the serde question gets its real form: not "is fltk's
# hub serde absent" but "does fltk-serde-core reach ours", which is the seam
# itself. The pyo3 question is the exception and is a text `filter`, because the
# flavor it looks for is a property of the crate name in whatever hub supplies
# it, not a label this workspace can name.
#
# The cquery carries the same config flags as the `bazel test` above. Bazel keys
# its analysis cache on the build options, so alternating two option sets in one
# server discards it and re-analyzes `//...` — on a path that runs at every local
# commit.
#
# The pyo3 question is asked of `//brenn-dsl:all`, not of the library alone:
# `brennfmt` names fltk's fmt-cli and every test target names the runtime crates
# directly, so a flavor mix-up in any of those deps lists is outside a graph
# rooted at the library.
#
# `cq` writes to a file and aborts the recipe when the query itself fails, rather
# than being read through `$(...)`: a failed query produces empty stdout, which
# the negative assertion would read as "nothing found" and pass. That is the one
# assertion nothing else catches, so it must not fail open, and a command
# substitution cannot abort the recipe from inside.
#
# A step of `bazel-check` rather than a lane of its own, so every pipeline that
# already runs that verb inherits it with no workflow edit.
bazel-dsl-coherence:
	@set -e; \
	out="$$(mktemp)"; err="$$(mktemp)"; trap 'rm -f "$$out" "$$err"' EXIT; \
	cq() { bazel cquery $(BAZEL_CONFIG) --config=clippy --config=rustfmt "$$1" >"$$out" 2>"$$err" \
	    || { echo "FAIL: bazel-dsl-coherence broken: the cquery failed: $$1"; cat "$$err"; exit 1; }; }; \
	cq 'somepath(//brenn-dsl:brenn-dsl, @fltk//crates/fltk-cst-core:no_python)'; \
	test -s "$$out" \
	    || { echo "FAIL: bazel-dsl-coherence broken: //brenn-dsl does not reach fltk's runtime crates"; exit 1; }; \
	cq 'filter("pyo3", deps(//brenn-dsl:all))'; \
	test ! -s "$$out" \
	    || { echo "FAIL: pyo3 is in //brenn-dsl's graph; a pyo3-flavor fltk target crept in"; cat "$$out"; exit 1; }; \
	cq 'somepath(//brenn-dsl:brenn-dsl, @crates//:serde)'; \
	test -s "$$out" \
	    || { echo "FAIL: bazel-dsl-coherence broken: //brenn-dsl links no serde from this workspace's hub"; exit 1; }; \
	cq 'somepath(@fltk//crates/fltk-serde-core:no_python, @crates//:serde)'; \
	test -s "$$out" \
	    || { echo "FAIL: fltk-serde-core does not reach @crates//:serde; the .bazelrc serde flag is not reaching it"; exit 1; }; \
	echo "bazel-dsl-coherence: no pyo3, one serde (@crates//:serde)"

# The deploy tarball's staged tree, and the gates on it.
#
# Needs BRENN_BUILD_ID: the release config stamps, and the workspace status
# command refuses to emit an unidentifiable build.
bazel-release:
	bazel build $(BAZEL_CONFIG) --config=release-package //deploy:release_package
	bazel test $(BAZEL_CONFIG) --config=release-package //deploy:all

# Prints the absolute path of the tree `bazel-release` staged, and nothing else,
# so the deploying repo can tar it. Derived rather than spelled out: the
# configuration segment of an output path is Bazel's to choose, and the
# convenience symlinks point at whichever configuration built last — a lane that
# also runs `bazel-check` would find a dev-configuration package there.
#
# Fail-closed, because the consumer tars whatever this prints. A command
# substitution that fails inside a `printf` leaves the enclosing recipe exiting
# 0 with a truncated path — `/` or the bare execroot, which exists and holds the
# mirrored source tree, so the tar step would succeed and ship a tarball with no
# bin/.
bazel-release-dir:
	@set -e; \
	root=$$(bazel info $(BAZEL_CONFIG) --config=release-package execution_root); \
	rel=$$(bazel cquery $(BAZEL_CONFIG) --config=release-package --output=files //deploy:release_package); \
	test -n "$$root" || { echo "bazel info printed no execution_root" >&2; exit 1; }; \
	test -n "$$rel" || { echo "bazel cquery printed no output path for //deploy:release_package" >&2; exit 1; }; \
	test -d "$$root/$$rel" || { echo "$$root/$$rel is not a directory; run bazel-release first" >&2; exit 1; }; \
	printf '%s/%s\n' "$$root" "$$rel"

# The policy scan's file set against the tracked tree, in both directions.
#
# Not a lane of `bazel-check`, and deliberately: the Bazel side is a filesystem
# glob, so every untracked scratch file in a working tree is a difference. On a
# clean checkout — CI's tree — that is exactly the signal wanted, which is where
# this runs. Two invocations because the comparison needs the manifest built
# first and then a binary run against it with the workspace still in view.
bazel-policy-parity:
	bazel build $(BAZEL_CONFIG) //:policy_manifest
	bazel run $(BAZEL_CONFIG) //xtask -- policy-parity --root $(CURDIR) --manifest $(CURDIR)/$(BAZEL_BIN)/policy_manifest.txt

# The advisory gate over both Cargo workspaces. Outside `bazel-check` because
# cargo-deny fetches an advisory database over the network, which a sandboxed
# test target has no business doing. `--root` is explicit: `bazel run` starts
# the binary in its runfiles tree, not the workspace.
# Install with: cargo install --locked cargo-deny
xtask-deny:
	bazel run $(BAZEL_CONFIG) //xtask -- deny --root $(CURDIR)

# Full pre-commit check suite: the Bazel graph, the policy scan's file-set
# parity against git, the advisory gates over both dependency ecosystems, and
# the scrub gate's own liveness. Each step is independently invocable and
# streams its own output.
#
# `npm-audit` and `scrub-selfcheck` are here because nothing else runs them: the
# JS/TS advisory gate has no CI job, and the stale-installed-scrubber check is
# local by nature — it needs neither cargo nor Bazel, and CI never runs
# `make check`.
check: bazel-check bazel-policy-parity xtask-deny npm-audit scrub-selfcheck
	@echo "check: all steps passed (bazel-check bazel-policy-parity xtask-deny npm-audit scrub-selfcheck)"

build:
	bazel build $(BAZEL_CONFIG) //...

# What a running server reads: the binary, the two asset trees the configs name,
# and the guest components. `build` stays the everything verb; this is the
# subset `launchdev` and `e2e` need, so starting a dev server does not first
# compile and link every test binary in the graph.
RUN_TARGETS := //brenn:brenn //frontend:dist //surface:dist //brenn-wasm:components //brenn-wasm:install_tree

run-artifacts:
	bazel build $(BAZEL_CONFIG) $(RUN_TARGETS)

# Install the scrub binary and point git at the tracked hooks. Idempotent.
# Asserts the scrub gate is actually live on this clone: template shims in
# sync, git hooks activated, installed binary not stale. See scrub/selfcheck.sh.
scrub-selfcheck:
	@./scrub/selfcheck.sh

# Canonical release-gate sweep: the tree must be free of disallowed strings.
# Invokes the installed brenn-scrub (the same binary the git hooks use;
# scrub-selfcheck asserts it is not stale). The exclude list is now empty: the
# private instance-config TOMLs and the CI workflow that carried private
# git-dependency URLs have moved to the brenn-ops annex, and the git deps are
# repointed to public sources. A prefix that matches no tracked file panics, so
# a stale exclude fails this target loudly rather than silently passing.
# TODO(scrub-tree-auto-gate): nothing runs this automatically, so the green-tree
# invariant and the stale-exclude panic only fire when someone remembers to
# invoke it. Wiring it into `check` needs a decision on the CI binary (CI does
# not install brenn-scrub) or a hermetic invocation.
scrub-tree:
	brenn-scrub tree

# Developer-machine setup, not a build lane: the scrub binary the git hooks
# exec has to exist on PATH, and cargo is how a Rust binary gets installed
# there.
setup-hooks:
	cargo install --path scrub
	git config core.hooksPath .githooks
	@rm -f .git/hooks/pre-commit
	@command -v gitleaks >/dev/null 2>&1 || { \
	    echo "gitleaks not found on PATH."; \
	    echo "Install the pinned release from https://github.com/gitleaks/gitleaks/releases"; \
	    echo "(version pin: see PINNED_VERSION in scrub/src/gitleaks.rs)"; \
	}
	@echo "setup-hooks: done."

e2e/node_modules: e2e/package-lock.json
	cd e2e && npm ci

# How pnpm is reached: the binary rules_js vendors, run through bazel. Pinned
# by MODULE.bazel and MODULE.bazel.lock like every other tool the build
# executes, and nothing has to be installed on the machine. `--dir` is absolute
# in the recipe below because `bazel run` starts the binary in its runfiles
# tree, not the workspace.
PNPM := bazel run $(BAZEL_CONFIG) -- @pnpm//:pnpm

# Fail the build on known advisories in all three npm trees (full audit, dev
# deps included). `frontend` and `surface` are audited through pnpm because
# `pnpm-lock.yaml` is the lockfile the build installs from — auditing what
# actually ships. `e2e` drives real browsers, sits outside the build graph, and
# has only `package-lock.json`, so it keeps `npm audit`.
npm-audit: e2e/node_modules
	$(PNPM) --dir $(CURDIR)/frontend audit
	$(PNPM) --dir $(CURDIR)/surface audit
	cd e2e && npm audit

DEV_PIDFILE := .dev-server.pid

# Hermetic e2e config server settings, mirrored from brenn.e2e.brenn.
E2E_BIN := $(BAZEL_BIN)/brenn/brenn
E2E_BASE_URL := http://127.0.0.1:3100

# Browser-level end-to-end tests (Playwright). Deliberately NOT part of
# `make check`: needs installed Playwright browsers and a live server. Fresh DB
# every run (rm -rf target/e2e) keeps it hermetic. Mints an invite via the
# built binary, starts the hermetic server backgrounded (launchdev pattern —
# no output redirection, which would hang the pipe against the backgrounded
# process), polls the login page until ready (checking server liveness first
# each iteration so a bind failure is reported, not masked by a foreign
# listener), runs the specs with the base URL and invite exported, and always
# stops the server (trap), propagating the
# test exit code. Chromium is the one genuine one-time setup step; a missing
# browser fails fast with the install command before any server starts.
#
# TODO(e2e-in-ci): no gate runs this suite, so it can rot red indefinitely —
# blocked on chromium provisioning on the runner and on the port-3100 server
# this target boots.
e2e: run-artifacts e2e/node_modules
	@cd e2e && node -e "const{chromium}=require('@playwright/test');const fs=require('fs');const p=chromium.executablePath();if(!fs.existsSync(p)){console.error('ERROR: Playwright chromium browser not installed. Run: cd e2e && npx playwright install chromium');process.exit(1);}"
	@rm -rf target/e2e
	@mkdir -p target/e2e
	@set -e; \
	if curl -sf -o /dev/null $(E2E_BASE_URL)/auth/login 2>/dev/null; then \
	    echo "ERROR: $(E2E_BASE_URL) is already serving before we started — a leaked e2e server or a port clash on 3100. Kill it before running make e2e."; exit 1; \
	fi; \
	invite=$$($(E2E_BIN) --config brenn.e2e.brenn --modules config/specs invite); \
	$(E2E_BIN) --config brenn.e2e.brenn --modules config/specs serve & \
	srv=$$!; \
	trap 'kill $$srv 2>/dev/null || true' EXIT INT TERM; \
	echo "e2e: server PID $$srv; polling $(E2E_BASE_URL)/auth/login ..."; \
	ready=0; \
	for i in $$(seq 1 60); do \
	    kill -0 $$srv 2>/dev/null || { echo "ERROR: e2e server exited before becoming ready"; exit 1; }; \
	    if curl -sf -o /dev/null $(E2E_BASE_URL)/auth/login; then ready=1; break; fi; \
	    sleep 1; \
	done; \
	[ "$$ready" -eq 1 ] || { echo "ERROR: e2e server not ready within 60s"; exit 1; }; \
	cd e2e && BRENN_E2E_BASE_URL=$(E2E_BASE_URL) BRENN_E2E_INVITE=$$invite npx playwright test

# Start the dev server in the background. Builds what the server reads, then
# execs the built binary from the workspace root so the relative paths in
# brenn.dev.brenn — the database, the log dir, the asset trees — resolve where
# they always did.
launchdev: run-artifacts
	@if [ -f $(DEV_PIDFILE) ] && kill -0 $$(cat $(DEV_PIDFILE)) 2>/dev/null; then \
		echo "Dev server already running (PID $$(cat $(DEV_PIDFILE)))"; \
	else \
		$(BAZEL_BIN)/brenn/brenn --config brenn.dev.brenn --modules config/specs serve & \
		echo $$! > $(DEV_PIDFILE); \
		echo "Dev server started (PID $$!)"; \
		sleep 1; \
	fi

# Stop the dev server.
stopdev:
	@if [ -f $(DEV_PIDFILE) ]; then \
		kill $$(cat $(DEV_PIDFILE)) 2>/dev/null && echo "Dev server stopped" || echo "Dev server not running"; \
		rm -f $(DEV_PIDFILE); \
	else \
		echo "No PID file found"; \
	fi

clean:
	bazel clean
	rm -rf target/e2e
