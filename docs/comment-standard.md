# Comment Standard

Rules for code comments in Brenn (and pfin/graf). Written to be enforceable by
a review agent with minimal subjectivity. Each rule has a test the reviewer can
apply mechanically or near-mechanically.

The default disposition is **delete**. A comment must earn its place by saying
something the code cannot say. When in doubt, no comment.

---

## Rule 1 — No references to ephemeral or out-of-tree documents

**Banned:** references to design docs, ADRs, requirements docs, review notes,
plans, chat context, or any document not present in the public repo tree at a
stable path. This includes section symbols pointing at such documents
(`§7.7`, "design doc section 3"), and contextless phrases like "per the plan",
"as discussed", "see the design".

**Allowed:** references to stable external specifications: RFCs, published
protocol specs (MQTT, WebSocket), language/stdlib documentation, W3C specs.
Also references to documents *in the repo tree* at stable paths (e.g.
`docs/security-posture.md`).

**Reviewer test:** does the referent exist in the public tree or as a stable
published spec? If the reviewer cannot resolve the reference from the repo
plus public standards, it is a finding. A `§` glyph is a strong signal but not
the rule — "per the design" without a `§` is equally banned; "RFC 6455 §5.2"
is fine.

```rust
// BAD: refers to a design doc that isn't in the tree
/// Falls back to the global default (design §7.7 — second tier).

// BAD: same rot, no section symbol
// As discussed in the review, we retry here.

// GOOD: stable external spec
// Close code 1009 per RFC 6455 §7.4.1 (message too big).

// GOOD: stable in-tree doc
// Rejection is logged for fail2ban; see docs/security-posture.md.
```

## Rule 2 — No narration

**Banned:** comments that restate what the adjacent code visibly does.

**Reviewer test (the delete test):** delete the comment and reread the code.
If no information is lost, the comment is narration. Finding: delete it.

```rust
// BAD: narration
// Iterate over the sessions and close each one.
for session in sessions {
    session.close();
}

// BAD: narration with extra words (common LLM output)
// First, we acquire the lock to ensure exclusive access.
let guard = state.lock();

// GOOD: not narration — the code cannot say this
// close() is idempotent; double-close during shutdown races is fine.
for session in sessions {
    session.close();
}
```

Section-header comments ("// ---- helpers ----") are narration of file
structure. One or two in a long file are tolerable; more than that is a
finding (consider splitting the file instead).

## Rule 3 — No descriptions of remote implementation

**Banned:** comments describing how code elsewhere behaves, when that behavior
is not part of the remote code's documented contract. These are the highest-rot
comments in the codebase: the remote code changes, the comment silently lies.

**Required instead**, in preference order:

1. **Restate as a local obligation or assumption.** An observation about
   remote code becomes a contract statement about this code. If the remote
   code later changes, an observation-comment is silently wrong; an
   obligation-comment is a documented broken contract — a bug report, not a
   lie.
2. **Move the fact to the side that owns it.** If this code depends on a
   behavior of a remote function, that behavior belongs in the remote item's
   doc comment (see Rule 4). The local comment may then cite the documented
   contract, which is allowed.
3. **Promote the invariant into the types** where practical (newtype, enum
   state machine, `#[must_use]`). Rot-proof; prefer it when the invariant is
   load-bearing and the change is small.
4. **Residual coupling:** if the coupling is real, non-contractual, and not
   worth a type, write it as coupling-with-an-address and a tripwire:
   `// Coupled to reconnect handling in session.rs; revisit if that changes.`
   This is honest and greppable from the other side. Use rarely.

**Reviewer test:** for any comment referencing behavior of code outside the
current item: does the referenced item's doc comment actually promise that
behavior? If yes → allowed (it cites a contract). If no → finding; the fix is
to document the contract where it lives (or restate locally as an assumption),
not to describe internals from afar.

```rust
// BAD: describes remote internals; rots when SessionManager changes
// SessionManager retries sends on reconnect, so errors here are rare.

// GOOD (form 1): local obligation, self-contained
// The session layer retries on reconnect; this handler must be idempotent.

// GOOD (form 2): remote side documents its contract...
/// Delivers `msg` at-least-once: redelivery occurs after reconnect.
/// Handlers must therefore be idempotent.
pub fn deliver(&self, msg: Msg) { ... }
// ...and the consuming side needs no comment at all, or at most:
// deliver() is at-least-once (see its docs); dedupe by msg id.
```

## Rule 4 — Doc comments on public items: what it is, not how it works

**Required:** every public item (fn, struct, trait, module) has a doc comment.
One line is usually enough: what it *is* or *guarantees*, not how it works.
Preconditions, postconditions, and invariants callers may rely on belong here
— this is where Rule 3's contracts live. Struct fields and non-obvious
parameters get a brief definition; obvious ones (`name: &str` on a thing with
a name) need none.

**Reviewer test:** public item with no doc comment → finding. Doc comment that
walks through the implementation → finding (it will rot; trim to the
contract).

```rust
// BAD: describes the implementation, will rot
/// Loops over the retained-message map, cloning each entry into a Vec,
/// then sorts by topic and returns it.
pub fn retained(&self) -> Vec<Retained> { ... }

// GOOD: the contract, one line
/// All currently retained messages, sorted by topic.
pub fn retained(&self) -> Vec<Retained> { ... }
```

## Rule 5 — Invariants and assumptions: the highest-value comments

**Encouraged:** comments stating what the local code relies on but cannot
express: lock-ordering, call-ordering, "sorted by construction", "caller has
validated", "must not allocate here". These are the comments the codebase
should have *more* of, even as the total count drops.

**Reviewer test:** none needed — this category is not a finding source, except
when the invariant could cheaply be a type or a `debug_assert!`, in which case
suggest the promotion.

```rust
// GOOD
// Invariant: `routes` is sorted by prefix length, longest first.
// binary_search below depends on it; insert() maintains it.

// BETTER, when cheap: enforce it
debug_assert!(routes.windows(2).all(|w| w[0].prefix.len() >= w[1].prefix.len()));
```

## Rule 6 — Why-comments: reasons, self-contained

**Allowed sparingly:** the reason a piece of code exists or takes a surprising
approach — stated in self-contained terms (a property of the world, a
tradeoff, a past incident), not as a pointer to remote internals (that's
Rule 3) or to an ephemeral doc (that's Rule 1).

**Reviewer test:** would the comment still be true and comprehensible if every
other file in the repo were rewritten? If it leans on "because that module
does X internally", route through Rule 3.

```rust
// GOOD: property of the world, stable
// Brokers commonly drop rapid resubscribes; debounce to one per 500ms.

// GOOD: tradeoff, self-contained
// Linear scan: n is bounded by the component count (~dozens); a map
// isn't worth the indirection.

// BAD: why-as-remote-pointer
// We buffer because the renderer can't handle partial frames.
// (Fix: document "callers must deliver whole frames" on the renderer,
//  cite that.)
```

## Rule 7 — How-comments: rare, only for the irreducible

**Allowed rarely:** explaining how code works, only where the code is
irreducibly non-obvious: bit manipulation, protocol quirks, algorithmic
subtlety, and every `unsafe` block (justification required there — why the
invariants hold).

**Reviewer test:** could the code be made obvious instead (better names,
smaller functions, a type)? If yes, that's the finding, not the comment.

## Rule 8 — No commented-out code, no changelog comments

**Banned:** commented-out code (git remembers), "removed X because Y"
tombstones, "TODO: cleanup" without a slug. TODOs follow the TODO(slug)
system — a `TODO` without a slug and a matching `TODO.md` entry is a finding.

---

## Rule 9 — Generic names in examples, fixtures, and tests

Comments, examples, doc snippets, and unit-test scenarios use generic
identities: `alice`, `bob`, `charlie`; `ACME Co.`; `example.com` /
`example.org`; `10.0.0.1`. Never a real person's name, a real host or domain
you actually run, a real employer, or anything resembling a credential.

**Bad:** `// e.g. the maintainer's own laptop pushes to git.realcorp.test`
**Good:** `// e.g. alice's laptop pushes to git.example.com`

A scrub gate (gitleaks) mechanically rejects the specific strings on a
site-local rule overlay, in write-time hooks and on commit and push. That covers only strings
someone already thought to list. This rule covers the rest — a novel name, a
new host, a real-sounding company — which is why it is a reviewer rule and not
just a regex.

**Reviewer test:** could this identifier be swapped for `alice` /
`example.com` with no loss of meaning? If yes, it should have been. The same
discipline extends to *meta* language: tracked text must not explain the
site-specific motivation for hygiene tooling — describe the mechanism, never
who runs it or what kind of content it guards.

---

## Reviewer quick sheet

| # | Rule | Test | Mechanical? |
|---|------|------|-------------|
| 1 | No ephemeral/out-of-tree refs | Referent resolvable in public tree or published spec? | mostly |
| 2 | No narration | Delete test: information lost? | mostly |
| 3 | No remote internals | Remote doc comment promises it? | yes, given Rule 4 |
| 4 | Public items: contract docs | Present? Contract, not implementation? | mostly |
| 5 | Invariants encouraged | (not a finding source) | — |
| 6 | Why: self-contained reasons | True if rest of repo rewritten? | judgment |
| 7 | How: irreducible only | Could code be made obvious instead? | judgment |
| 8 | No dead code / slugless TODOs | grep | yes |
| 9 | Generic names in examples/fixtures | Swappable for `alice`/`example.com`? | partly (scrub gate) |

Volume calibration: this standard should *reduce* total comment count
substantially. A diff that adds many comments is itself a signal — most new
comments should be Rule 4 contracts or Rule 5 invariants. Narrative density
(a comment every few lines restating the story of the code) is a finding even
if each individual comment is arguably defensible.
