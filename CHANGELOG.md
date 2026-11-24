# Changelog

All notable changes to Brenn are documented here.

## [Unreleased]

- **New for component authors: a component can state a lifetime, ask for
  delivery statuses and name a reply topic on an MQTT publish.**
  `brenn:processor/mqtt@0.1.0` gains a function, `mqtt-publish-tracked`, beside
  `mqtt-publish` and `mqtt-publish-expiring`, whose WIT shapes are unchanged
  (what a call to them does changes: **BREAKING for component authors**,
  below). It takes a required lifetime — a number of seconds, or `forever` —
  and optionally a status port, one of the component's own `out` or `io` ports,
  and a reply topic on the same client, sent as the MQTT 5 Response Topic. It
  returns the message's id, the `message_id` every status about it carries. The
  host names the channel the operator wired to the status port as the publish's
  `status_to`; a port the component does not declare `out` or `io` traps the
  activation, a declared port left unwired names none, and a wired channel the
  bus cannot take is answered `status-port-unusable`. A component that does not
  call the function imports nothing new; one that does needs this brenn or
  later. In `brenn-guest`: `mqtt::publish_tracked` with `mqtt::TrackedPublish`
  and `mqtt::Lifetime`; `delivery::read` reads a status off a received message
  and believes only the host's status sender; `mqtt::connection::read` and
  `mqtt::connection::outcome_will_never_come` settle a `handed` with no outcome
  against the client's connection state. Message ids are
  `brenn_guest::uuid::Uuid` (`brenn_guest::mqtt::MessageId`); use that `uuid`,
  or the Bazel alias `//brenn-wasm/components/guest:uuid`, not a `uuid` of your
  own, whose `Uuid` is a different type. To read the statuses, bind the status
  port `io` to a declared durable channel and list it in the instance's `acl
  publish` (`docs/config-dsl.md`, *Instances and bindings*). A publish draws
  two statuses; a component woken by its own statuses is bounded by its
  activation pacer and the status binding's `push_depth` and `amplification`
  (`docs/security-posture.md` §8.2).
- **Breaking config:** boot now refuses an `[llm_chat] prefix` of
  `mqtt-connection` or `device-location`. Either would place every chat
  channel inside a channel family only the server itself may write, and give
  the chat harness write access to it. A configuration using one must pick
  another prefix.
- **Feature:** MQTT connection state is now on the bus. Every declared
  `mqtt_client` gets a server-written channel, `brenn:mqtt-connection.<client>`,
  whose newest message says whether the client is connected, retrying (with
  the last error) or stopped for good (with the reason), since when, and
  whether the previous server process ended that client's session uncleanly.
  It exists even for a client nothing is bound through. Nobody can read it
  until the operator grants access — `acl subscribe [exact
  "brenn:mqtt-connection.<client>"]` for an agent, an `in` port for a
  component — and no configuration can write to it or resize it
  (`docs/message-bus.md` §2.7, §2.7.3).
- **Feature:** senders learn about crashes. A sender that asks for delivery
  status (`status_to`, below) would be left holding `handed` with no outcome
  when the server dies. After an unclean stop, the next server process first
  sends one `restarted` status to every `status_to` named on a message the
  old process may have handed and the client's egress channel still holds,
  before sending anything new: "nobody knows whether it arrived". A clean stop
  sends none, except one that overlaps a configuration reload
  (`docs/message-bus.md`, *Delivery status*). A `restarted` status has no
  `message_id`, so readers of delivery statuses must check `kind` before
  reading `message_id`.
- **Upgrade note:** the first boot of this version cannot know how the
  previous version stopped, so it sends no `restarted` statuses and its
  connection state reports no unclean end. A sender resolves an old
  `handed` using the state's `found_at_ms` (`docs/message-bus.md`, "What a
  sender may infer").
- **Behaviour change:** every final stop of an MQTT client — the broker
  rejecting its credentials, or ending a connected session for good — now
  raises a `Critical` alert, "MQTT client stopped: <client>". Before, every
  such stop was only a log line. A placeholder client with wrong credentials
  will now alert at every boot.
- **Behaviour change:** MQTT clients now handle a session the broker kept
  (`session_expiry_secs` above 0) correctly on the receiving side. The
  first two items below change every client; the last two change nothing
  for clients left at the default expiry of 0:
  - Messages that reach Brenn at boot before its inbound routing is ready
    are no longer lost: what the broker queued while Brenn was down, and
    the retained values it sends at the first subscribe. Before, they were
    dropped, the queued ones after the broker had already been told they
    were received.
  - Reconnecting no longer fills a channel with copies of values it already
    has. The broker still resends each topic's retained (last-known) value on
    every subscribe, so a value that changed while Brenn was away still
    arrives. But a resent value that matches the newest one the channel
    holds for that topic is not added again. The exception is a payload
    that is not text on a wrapped channel (the default shape), which is
    still added on every reconnect and repeated subscribe
    (`mqtt-retained-replay-repeated-on-resubscribe`). Live messages are
    added as before. A subscriber that relied on getting a new message on every
    reconnect or repeated subscribe no longer gets one.
  - A filter unsubscribed while the client is disconnected is now withdrawn
    at the broker on the next connection that finds the kept session.
    Before, Brenn never sent the withdrawal and the broker went on
    delivering. `MessageUnsubscribe`'s `unsubscribed_pending_reconnect`
    result now means exactly this. Known gap: a restart before that
    connection still leaves the filter in the broker's session, and so does
    a filter a previous process held and this one does not
    (`mqtt-deferred-unsubscribe-not-withdrawn`).
  - An `mqtt_client` removed by a config reload ends its broker session
    when it is connected at that moment, instead of leaving it to queue
    messages until it expires. One with no connection then, or whose
    DISCONNECT is lost on the way (`mqtt-disconnect-lost-to-reset`), leaves
    the session at the broker until it expires. One added by a reload
    starts clean on its first connection, discarding anything the broker
    kept under that slug; a server restart before that first connection
    resumes a kept session, as every server boot does
    (`mqtt-added-client-clean-start-lost-to-restart`).
- **Upgrade note:** the reload status body gains `mqtt_unsubscribe_deferred`,
  which lists unsubscribes waiting for the client's next connection.
  `mqtt_deferred` no longer lists them.
- **Upgrade note:** `mqtt_client { qos = 2 }` no longer loads; set it to 1.
  Brenn's MQTT client library forgets, on every lost connection, which QoS 2
  messages it has received, so subscribing at 2 could deliver a message twice
  or fail the connection on every reconnect. Publishes may still use QoS 2.
- **Behaviour change:** an MQTT client keeps its session state across a lost
  connection. With `session_expiry_secs` above 0, when the broker resumes the
  session, a publish in flight when the connection dropped is completed by
  it: QoS 1 is sent again under its original packet identifier, not as a new
  publish, and QoS 2 is delivered exactly once. The completion is made
  however late the client reconnects, even after the message's lifetime
  (`mqtt-expired-replay-not-stopped`). With the default of 0, or when the
  broker did not keep the session, a QoS 1 publish in flight is sent again as
  a new publish within its lifetime, and a QoS 2 publish, or a QoS 0 one not
  yet written, ends as `unknown`.
- **Behaviour change:** an unsolicited acknowledgement from the broker (one
  for a message the client has no record of) no longer reconnects an MQTT client forever; the client's session is discarded
  once with an alert, and a second in a row stops the client with a critical
  alert.
- **Behaviour change:** an MQTT connection that is closed or reset during the
  TLS handshake is retried like any other failed connect; before, it stopped
  the MQTT client until its `mqtt_client` block was reloaded or the server
  restarted. A certificate the client rejects, or any other refusal by TLS
  itself, still stops it.
- **Behaviour change: one QoS per MQTT topic filter, the client's.** An MQTT
  filter is now always subscribed at its client's configured `qos`, whether a
  config binding or an app's runtime `MessageSubscribe` subscribed it.
  `MessageSubscribe` no longer takes a `qos` argument; a call that passes one
  is refused as an unknown argument. This fixes a panic: subscribing to a
  filter already held at a different QoS crashed the conversation (or tool
  connection) after the subscription had been written.
- **Upgrade note:** on first start, the QoS stored with each app's runtime
  MQTT subscription is dropped. A filter that was held at a different QoS
  than its client's is held at the client's from then on, so its messages
  arrive at no higher QoS than that.
- After a config reload that changes a client's `qos`, the `mqtt_subscribed`
  status lists every filter of that client the reload carried, including
  those held only by runtime subscriptions.
- **Behaviour change:** recurring automation jobs now keep firing with nobody
  attending. Before, every fire used one unit of a conversation's send budget
  (the allowance that only a person's interaction refills), so a schedule
  emptied it within days and was then disabled. Fires no longer draw it. A
  deployment that enlarged `send_budget` or `default_send_budget` for its
  schedules can bring it back down. A `send_message` fire delivered into a
  conversation still costs that conversation one unit for the turn it
  provokes.
- **Behaviour change:** `AutoCreate`, and an `AutoEdit` that changes a job's
  action or trigger or enables it, now draw one unit of the calling
  conversation's send budget and are refused when none remains; a write the
  engine refuses gives its unit back. Renaming, disabling, listing and
  deleting jobs are free.
- **New: `automation { max_fires_per_day_per_app }`**, default 500: the most
  fires of all one app's jobs together in any sliding 24 hours. A fire over it
  is skipped, recorded with the new outcome `app_fire_cap`, and the job fires
  again by itself at its next slot with room; it does not count toward
  `consecutive_failures_to_disable`, so it never disables a job. The first
  such refusal of an app in any 24 hours is reported to the owner's
  conversation and later ones are not; the operator is alerted once per
  process per app. The `automation` section's keys are now documented in
  `docs/config-dsl.md`.
- **Upgrade note:** on first start the `automation_fires` table is rebuilt
  (its stored outcomes no longer include `budget`; existing `budget` rows
  become `action_error` with `error_class` `budget`), and a new table,
  `automation_app_fires`, is created. After that, rolling back to an older
  binary is not supported: an older binary panics when it records a fire on
  an empty send budget.
- **Fixed:** a client holding overlapping MQTT subscription filters (`a/#` and
  `a/b`) received each message on the overlap once per covering filter in each
  covering channel. It now receives it once in each: every message is routed
  by the MQTT 5 Subscription Identifier the broker returns on it. Not so
  where two filters share a Subscription Identifier, which is rare, or where
  the broker sends a message without one and it is routed by topic as before,
  as is every message of a broker that does not support Subscription
  Identifiers; see "Which channels an inbound MQTT message reaches" in
  `docs/message-bus.md`.
- **BREAKING for operators with a non-zero `session_expiry_secs`:** on the
  first start of this version, messages the broker queued for the client while
  the previous version was stopped are dropped, each with a `warn` naming the
  Subscription Identifier it carried. Later restarts deliver queued messages
  to the channel of the filter each was queued for. A deployment with the
  default `session_expiry_secs = 0` is not affected.
- **Behaviour change:** the retained messages a broker replays for a new MQTT
  subscription, and for each filter on reconnect, reach that subscription's
  channel only — a wider channel on the same client no longer receives them.
  A subscriber that relied on the wider channel refreshing that way subscribes
  to the narrower filter itself.
- **Fixed:** a reload that changed the QoS of a held MQTT filter unsubscribed
  it and subscribed it again several commit steps later, and the broker sent
  nothing published on it in between. The reload now sends one SUBSCRIBE at
  the new QoS, and the filter is reported in `mqtt_subscribed` only.
- **BREAKING for operators:** an MQTT subscribe ACL entry whose filter starts
  with `#` or `+` no longer covers a subscription whose first level starts
  with `$` (`$SYS/…`, `$share/…`), as the per-topic publish entries this
  release introduces (below) do not either. Write an entry that spells the
  first level, for example `topic_filter "mqtt:<c>:$SYS/#"`. What you see if
  a subscription is no longer covered: a static subscription or a surface
  binding stops the server at start with a message naming the subscriber and
  the channel; a stored dynamic subscription is kept but goes dormant, with a
  `warn` line "dynamic subscription revoked", and resumes once a covering
  entry is added; a new dynamic subscribe is refused as not permitted. On a
  client with a non-zero `session_expiry_secs`, the broker can resume its
  session with the dormant subscription's filter still in place and keep
  sending on it: each such message is dropped with a `warn` naming the
  Subscription Identifier it carried, unless a filter the client holds shares
  that identifier and covers the topic, in which case that filter's channel
  receives the message a second time (rare; `docs/message-bus.md`, the same
  passage); this lasts until the broker's session for the client ends or a
  covering entry is added.
- **New: an MQTT publish can name a reply topic.** `MqttSend` and an
  automation's `mqtt_publish` action take an optional `reply_to`, written
  `mqtt:<client>:<topic>`, which is sent to the broker as the MQTT 5 Response
  Topic. It must be a concrete topic on the same client as `to`, and the app
  must itself be allowed to publish to it by its `acl publish` entries, so an
  operator who wants an app to ask for replies grants it publish on the reply
  topic too. No Correlation Data is sent.
- **New: `MqttSend` can schedule a publish.** An optional `deliver_after`
  (RFC 3339) commits the message now and publishes it at that time; a past
  time publishes at once. The lifetime counts from `deliver_after`. Until
  then `BrennPendingList` lists it and `BrennMessageCancel` withdraws it,
  including for an app that holds only the MQTT publish grant;
  `BrennMessageEdit` refuses it. A client removed from the configuration
  first reports its scheduled messages `dropped`, `removed`. Each client
  holds at most its egress channel's retain depth (256 by default) of
  scheduled messages, across every sender. An automation's `mqtt_publish`
  action refuses `deliver_after`.
- **New: recurring automations can send MQTT messages.** A scheduled job
  (created with `AutoCreate`, changed with `AutoEdit`) can now publish to an
  MQTT topic on a cron schedule with no LLM involved — for example lighting
  schedules. Use an action of kind `mqtt_publish` with `to` set to
  `mqtt:<client>:<topic>` and a fixed payload (`body` as text or JSON, or
  `body_base64`), plus `content_type`, `qos`, `retain`, a required
  `lifetime_s` (counted from each fire) and an optional `status_to`. It obeys
  the same rules as the `MqttSend` tool: only apps with the MQTT publish
  grant are offered it, and the target is checked when the job is created or
  edited and again on every fire.
- **Upgrade note:** on first start the `automation_jobs` table is migrated
  (its `action_kind` column is dropped). After that, rolling back to an older
  binary cannot create or edit automations.
- **BREAKING for automation callers:** `AutoCreate` and `AutoEdit` now refuse
  unknown keys in `action`, naming the key, instead of ignoring them. A
  `send_message` action aimed at an `mqtt:` address, or with a non-text
  `body`, is refused with a message pointing the right way.
- **BREAKING for MqttSend callers:** a `retain` that is not `true`/`false`
  is refused; it used to be read as `false`.
- **BREAKING for MqttSend callers:** `MqttSend` now commits the message to the
  bus and returns, instead of waiting for the broker. A new argument,
  `lifetime_s`, is required: a number of seconds from 1 to 4294967295, or
  `"forever"`. It is how long Brenn keeps trying to hand the message to the
  broker, and what is left of it at that moment is sent as the MQTT message
  expiry; a message already in flight when the connection is lost can be sent
  after it, when the client reconnects. The answer is `ok` with the message's
  id, the remaining budget and whether the client is connected now; it never
  reports delivery. A client that is not connected is no longer an error: the
  message waits for the connection within its lifetime. Without a
  `status_to` (below), failures go to the operator as `warn` lines and phone
  alerts, not to the caller. At QoS 1 a message may be delivered twice after a
  reconnect. An
  inline payload over `[messaging] max_body_bytes` needs a blob store and is
  refused as `payload_unstorable` without one. The `MqttSend` card's badge
  reads "committed".
- **New: a sender can learn what became of its MQTT message.** `MqttSend`
  takes an optional `status_to`, a `brenn:` channel the app may publish to.
  Brenn publishes JSON statuses there (sender `system:delivery-status`):
  `handed` when the message is handed over to be sent, then one outcome —
  `delivered`, `rejected` (refused by the broker, or too large for it),
  `expired`, `dropped` or `unknown`. `expired` and `dropped` mean the message
  was never sent, and can come with no `handed` before them; `unknown` means
  it may have been sent and will not be sent again, including a QoS 1 message
  refused when re-sent after a lost connection (cause `resend_refused`),
  whose first copy may have arrived. Not every message gets an outcome
  (`docs/message-bus.md`, *What a sender may infer*). A refusal by the broker
  alerts the operator whether or not the caller was told. It usually means
  the app's topic ACL admits what the broker forbids, but in this release
  every refusal by the broker is told and alerted alike: also a Quota
  exceeded answer, which says "not now" and which Brenn does not send again
  (`mqtt-quota-exceeded-told-rejected`), and, at QoS 2, a broker that takes a
  message and then refuses the last step of the exchange, so that a message
  told `rejected` has probably been delivered
  (`mqtt-qos2-completion-refusal-told-rejected`). Components name a status
  port through `mqtt-publish-tracked` (above).
- **New for operators: an MQTT session waits for its broker's answers when it
  stops.** A server shutdown, a reload that changes a client and a reload that
  removes one all stop the client's session the same way: nothing more is
  handed to the broker, the messages already in flight are waited for and
  reported as what they turn out to be, and only then is the DISCONNECT sent.
  How long it waits is a new `mqtt_client` key, `stop_wait_secs` (default 60;
  `0` does not wait); a message the broker has not answered by then is
  `unknown`, `stopped`. A session with nothing in flight, or a broker that
  answers, stops in milliseconds. A shutdown stops every session at once and
  does not exit until each has finished and its statuses are committed, so a
  `handed` with no outcome after a restart means the server crashed or was
  killed, or that the outcome's status could not be published, which the
  operator is told. Keep `stop_wait_secs` shorter than the service manager's
  stop timeout (systemd's `TimeoutStopSec`, 90 s unless set), or a shutdown
  against a silent broker is killed mid-wait. A reload that both removes and
  changes clients, against brokers that do not answer, can wait up to two
  bounds (120 s at the default); allow for that in anything that waits on a
  reload's outcome.
- **Development:** `make check` no longer spends about eleven minutes on the
  `brenn-server` test suite. Three Gmail message-body tests searched for the
  largest body that fits one byte step at a time; they now bracket and bisect,
  and the whole suite runs in about a minute. No runtime behaviour changes.
- **BREAKING for operators:** MQTT publish ACLs are per topic. Until now, an
  app or component allowed to publish through a broker client could publish to
  every topic on it; it is now limited to the topics its ACL names. A publish entry
  that named a client, `acl publish [client "mqtt:<c>"]`, no longer loads; it
  is written `acl publish [topic_filter "mqtt:<c>:<filter>"]`, one entry per
  filter, and a budget tail moves onto any one of a client's entries (it still
  tunes the client's one sink). `topic_filter "mqtt:<c>:#"` reproduces the old
  reach except topics whose first level starts with `$`, which are written out
  (for example `topic_filter "mqtt:<c>:$SYS/#"`). A stamp ceiling's `client`
  publish line is rewritten the same way. A packaged arrangement whose bundle
  still writes the client-only form keeps compiling, and now holds the topics
  its stamp's ceiling states for that client — a stamp whose ceiling states
  none is refused with the line to add. Applies to agents (`MqttSend`) and
  components (`mqtt-publish`) alike: a component's refused topic is the
  existing `not-permitted`, an agent's is an error that says so, and either
  denial names the topic.
- **BREAKING for MqttSend callers:** `MqttSend` can send JSON, and raw bytes
  move to their own argument. `body: { binary_base64, content_type }` becomes
  the top-level arguments `body_base64` and `content_type`; the old object is
  refused with a message naming them, never published. A string `body` is
  still published verbatim. Any other object, an array, a number or a boolean
  `body`, all refused until now, is published as its JSON text (`42`,
  `true`): a serialization of the value, not the caller's spelling. A null
  `body` is still refused; the error now says to send the text `null` as
  base64 (`bnVsbA==`) in `body_base64`. A number with many digits can lose
  some of them, on the way to Brenn or in Brenn's own parse, which holds at
  most a 64-bit integer or an `f64`; to publish a number's exact digits, give
  the base64 of its text in `body_base64`. `content_type` also types a
  `body`, and is refused beside an `attachment`, which publishes its own. An
  automation's `mqtt_publish` action (above) reads `body` the same way.
- **BREAKING for component authors:** a component's `mqtt-publish` and
  `mqtt-publish-expiring` now go through the bus, like `MqttSend`, and never
  wait on the broker. The WIT shapes are unchanged and every existing component
  loads and links; what a call means changes:
  - The call returns once Brenn has decided the publish may be made. Nothing
    has been sent yet: the message is committed to the client's egress
    channel with the activation's other publishes when the activation returns
    ok.
  - A message published before a later `err` or trap is discarded with the
    activation's other publishes. Before, it had already reached the broker.
  - A disconnected, slow or silent broker is no longer an error. The message
    waits on the client's egress channel and is sent when the client
    connects. `broker(..)` is never returned.
  - A broker refusal and a packet over the broker's maximum size go to the
    operator as `warn` lines and phone alerts naming the component, and are
    not told to the component. `broker-rejected(..)` is never returned.
  - `mqtt-publish` is kept until sent, within the client's egress window;
    before, it was attempted once.
  - `mqtt-publish-expiring`'s interval starts when the activation commits, and
    what is left of it when the message is handed to the broker is the
    interval sent. Before, it started when the broker received the message.
  - Each accepted call is a buffered publish, shared with `ports.publish`: at
    most 256 per activation and 4 MiB of payload in total; past either the
    call returns `quota-exceeded`.
  - The payload is also held to the client's `outbound_payload_cap_bytes`, and
    a malformed `content-type` is `invalid-payload`.

  This change rests on a survey of the out-of-tree component repositories
  named in `docs/component-packages.md`, which found one component that
  publishes MQTT and none that relies on the broker's inline answer. A
  component repository not named there was not and could not be surveyed.
  Until this release is tagged the old behaviour can be restored without a new
  contract version; after it, restoring it needs one.
- **For authors of host tests:** the `brenn_wasm` seam for a component's MQTT
  publish is replaced. `MqttPublishFn` was a closure that published and
  returned an `MqttPublishOutcome`; it is now an `Arc<dyn MqttPublisher>`, a
  trait whose two methods decide and never publish: `resolve` parses the
  guest's client and topic into an `MqttTarget`, and `admit` returns an
  `MqttAdmission` — `Admitted { response_topic }`, `NotPermitted`,
  `InvalidPayload`, `ReplyTopicNotPermitted` or `StatusChannelRefused`, whose
  `cause` is a `StatusChannelRefusal`. `MqttPublishOutcome` is gone.
  `MqttPublishProps` states a `lifetime` (`MqttPublishLifetime`), a
  `status_to` and a `reply_topic` in place of `message_expiry_s`, and no
  longer has a `Default`. A test reads what a component published from
  `ProcessorOutcome::Ok`'s `publishes`, where an MQTT entry has `mqtt` set to
  a `ProcessorMqttPublish` (`message_id`, `target`, `props`,
  `response_topic`). The `mqtt` host functions are trappable.
- A component's per-client publish rate limit now counts every publish that
  reaches the client, however the guest spelled the `client` string: `home:a`
  with topic `b` is charged to `home`.
- **Fixed:** after an MQTT session's supervisor recovers from a panic, a
  message in flight on the dead connection is resolved by the rules for a
  lost connection, and is never taken for a later message's acknowledgement.
- An `MqttSend` to a client that a config reload is removing is never
  answered "committed" and then silently lost. It is refused, as for a client
  that is not configured, or it is committed and reported: a message the
  client had already taken, as what became of it; one it had not, as dropped
  with cause `removed` — to the operator, and to its `status_to` when it
  names one. The report does not depend on the client's session still
  running. Any open run of coalesced alerts for the client is closed, with a
  summary that names the removal if it held any back. Still not reported:
  messages left for a client deleted from the config while the server was
  down, and whatever a server shutdown that overlaps the reload cuts off
  (`shutdown-does-not-quiesce-reload`).
- **BREAKING for operators:** `egress` is now a reserved channel name, for
  routing MQTT publishes through the message bus. A declared
  channel named `egress`, `egress.<x>` or `egress/<x>` no longer loads, and no
  publish ACL, component output or surface output may name a channel under
  `brenn:egress/`. The host mints one channel there per `[[mqtt_client]]`,
  `brenn:egress/mqtt/<client>`, which carries that client's publishes, from
  `MqttSend` and from components. Its window (default 256) is the outage it survives; tune every
  client's with `channel at prefix "brenn:egress/mqtt/"` or one client's with
  `channel at "brenn:egress/mqtt/<client>"`, and restart to apply a retune. A
  message dropped because the window overflowed is counted and alerted. A
  component's `in` port may read a declared client's channel; nothing else may
  name it.
- **For component authors:** the SDK's `TransportMeta` gains the variant
  `MqttPublish`, which holds an `MqttPublishTransport` (defined in
  `brenn-envelope`; `brenn-guest` re-exports the enum, not the struct) with
  the fields `client_slug`, `topic`, `qos`, `retain`, `content_type`,
  `expires`, `status_to` and `response_topic`. `expires` is
  `{"at": "<rfc3339>"}` or `"never"`. `status_to` is the `brenn:` channel the
  publish's delivery statuses go to, as its sender named it.
  `response_topic` is the bare reply topic, sent as the MQTT 5 Response
  Topic. `content_type`, `status_to` and `response_topic` are absent on the
  wire when unset. A component that matches the enum exhaustively needs an
  arm when it is rebuilt against this release; one that uses the SDK's
  accessors needs nothing. A component reads it only on a
  `brenn:egress/mqtt/<client>` channel its `in` port binds. The variant
  changes no WIT contract.
- Bumped url from 2.5.8 to 2.5.9, clearing RUSTSEC-2026-0592. A `^` in a URL
  path is now percent-encoded: file links carry `%5E`, `api-request` sends
  `%5E` for it, and a push `data.url`, redirector target or login return path
  holding a raw `^` is refused.
- **For developers:** `make check` and `make build` now keep a timing record
  of each Bazel run: how long it took and which steps were rebuilt rather than
  taken from the cache. `make timing` shows the last run. Set
  `BAZEL_TIMING=off` to turn it off; it is off by default when `BAZEL_CONFIG`
  is set, as it is in CI and deploy builds. See `docs/build-timing.md`.
- The policy check no longer scans a local database's blob store directory
  (`*.db.blobs`) left in the source tree by a dev server.
- An MQTT message arriving from a broker whose topic contains a wildcard
  character (`+` or `#`), which no valid topic can, is now logged and dropped
  instead of possibly being routed to a channel.

## [0.38.0] — 2026-11-21

A component can unsubscribe a mailbox from a mailing list: the new
`one-click-unsubscribe` tool sends the RFC 8058 request, and a provider
component built on `brenn-provider-kit` can now call any tool it is granted,
not only `api-request`. Breaking for provider-kit authors; read the bold
entry below.

- **A new tool, `one-click-unsubscribe`, sends the RFC 8058 one-click
  unsubscribe request.** Called with an `https` URL, it sends one `POST` with
  the body `List-Unsubscribe=One-Click` and no credential or cookie, follows no
  redirect, reads no response body, and answers the status, or `unreachable`
  when nothing answered within 30 seconds, each with the host it parsed and
  dialled. A URL whose host is not a public
  address — a special-purpose IP literal, a reserved name, or a name that
  resolves only to special-purpose addresses — is refused `denied` and nothing
  is sent. The tool is always registered, takes no `allow` clause and is for
  WASM components: a call from any other caller is refused `denied`.
  - **For operators:** an instance holding the grant can make Brenn send a
    fixed-body `POST` to any public `https` URL of up to 2,048 bytes; Brenn
    cannot check where the URL came from, and the URL can carry whatever the
    grantee puts in it. Grant it only to a component trusted with outbound
    data, and always with a `rate_limit`. A component's call has no approval
    step, so the grant is the authorization. Each refused destination writes a
    `ssrf_attempt` security line attributed to the caller (no `ip`, so
    fail2ban ignores it) and raises a `Warning` phone alert naming the caller
    and the host; the host usually comes from a message a third party sent.
    An `unreachable` call's log line carries the cause: resolution,
    connection, TLS or timeout.
- **A provider component can call a tool other than `api-request`.** A
  provider built on `brenn-provider-kit` returns `Call::tool(step, tool, args)`
  to call any tool its instance is granted, such as `one-click-unsubscribe`;
  the call is issued, backlogged and answered as an `api-request` call is, and
  the step reads the tool's answer value itself. Such a call carries no
  attachment. A refused call's failure names the tool and the grant it needs.
  - **BREAKING for authors using `brenn-provider-kit`:** `Call.api` is
    `Call.request`, a `Nested` — `Nested::Api(ApiCall)` or
    `Nested::Tool(ToolCall { tool, args })`; `Call::api(step, api)` builds the
    old shape. `InFlight.target` is an `Outstanding` —
    `Outstanding::Api(Target)` or `Outstanding::Tool(name)` — and a step hands
    the `read_api_*` readers `state.target.api()?`, which fails `internal` for
    a call to another tool. `Host::call` takes `&Nested`. In `testutil`,
    `NestedCall.call` is `NestedCall.request`, read as an `api-request` call
    with `NestedCall::api()`; `api_of(&call)` reads a `Call`'s; and
    `unsubscribe_status(host, status)` and `unsubscribe_unreachable(host)` fake
    `one-click-unsubscribe`'s answers. A backlog or in-flight row written
    before the cut does not parse after it, and none is read: no request
    survives the restart an upgrade is, and the kit deletes the rows of a
    request no longer pending unread. When every call an activation could issue was
    refused, the backlog's failure now reads "the host refused every call …"
    where it read "api-request refused every call …".

## [0.37.0] — 2026-11-21

Bytes move as blobs end to end: Gmail bodies, draft attachments and calendar
writes travel as attachments, provider components build call bodies as blobs,
guest fuel follows the bytes a component is handed, and `MqttSend` publishes
attachments by reference. Several breaking changes for component authors and
operators; read the bold entries below.

- **`MqttSend` payloads are bounded per client, at 4 MiB unless the
  deployment says otherwise.** A new `mqtt_client` key,
  `outbound_payload_cap_bytes` (default 4194304), caps the bytes one
  `MqttSend` call publishes through that client: the string's UTF-8 length,
  the decoded length of a base64 body, or the attachment's size. A call over
  it is refused with `reason` `payload_too_large` and spends no send budget.
  Text and base64 payloads had no bound before; a deployment publishing more
  than 4 MiB in one call raises the key. A component's `mqtt-publish` is not
  held to it and stays capped at `[messaging] max_body_bytes`.
- **`MqttSend` publishes an attachment's bytes by reference.** A new
  `attachment` argument, `{"message_id": …, "name": …}`, given instead of
  `body`, publishes the bytes of an attachment of a message the conversation
  was shown, with the attachment's content type as the MQTT Content Type. No
  byte passes through the conversation: it names the attachment as it would
  forward it with `BrennSend`, under the same naming rule and refusals. A call
  gives exactly one of `body` and `attachment`. The base64 body stays, for
  bytes a conversation composes itself.
- **An MQTT publish larger than the broker accepts is refused before it is
  sent, instead of dropping the client's session.** When the broker
  advertises a maximum packet size, a publish whose packet would exceed it
  is refused on the spot. `MqttSend` answers an error naming the packet's
  size, the client and the broker's limit; a component's `mqtt-publish`
  gets `invalid-payload`. Before, the oversize packet took the client's
  whole session down: every other publish in flight on that client failed
  with it, and a QoS 0 publish was reported sent when it never was. A
  broker that advertises no maximum is unaffected.
- **`MqttSend`'s base64 body is checked strictly.** The object form takes
  `binary_base64` and `content_type` and nothing else: any other key is
  refused, naming it, where it used to be ignored (a misspelt `content_type`
  published untyped bytes). `content_type` must be a string the bus accepts
  as a content type; a non-string used to be read as absent.
- **Existing `calendar-write` instances must add `blobs` to their `grants`;
  `CalendarCreate` and `CalendarUpdate` now take a description of up to 8,000
  characters.** For operators: without the `blobs` grant the configuration is
  refused at load; the instance's clauses are unchanged. Each write now builds
  the event it sends to Google as a blob (a stored byte payload), which stays
  in the blob pool for a while — up to about 76 KB per write. For callers: a
  write's answer no longer echoes `description` or `description_chars`; read
  the description back with `CalendarEvent`. A request larger than the bus
  cap is still refused before the tool sees it: at the default 64 KiB, that
  takes a description of more than about 6,100 characters after JSON escaping
  (a control character takes six bytes) beside every other field at its
  widest.
- **The provider kit drops a late answer unread.** For authors of kit
  components: `Logic::advance` is no longer called for an answer to a request
  the host has already given up on (its caller left or its deadline passed);
  the kit deletes the request's in-flight row without decoding it and logs one
  `Debug` line. The kit's own 240-second cutoff is gone: the host's `pending`
  check is the only cut. No caller sees a change — everything this skips was
  already being dropped. A test harness that tracks no requests and answers
  `pending` true sees every answer advanced, whatever its age. Out-of-tree
  components pick this up when they move their brenn pin.
- **`GmailDraft` attaches files; existing `gmail-compose` instances must
  change two lines.** `GmailDraft`, the Gmail draft-writing tool, now takes
  up to 5 files (10,485,760 bytes together) in the call's `attachments`,
  each attached under the name the call gives it; more is refused before
  anything reaches Google. To make room, the draft is now built as a blob
  (brenn's binary attachment store) and sent to Gmail's upload endpoint
  instead of being encoded into the request text. Upgrading needs:
  - `blobs` added to the instance's `grants` — without it the configuration
    is refused at load.
  - Its `api-request` clause for `POST /gmail/v1/users/me/drafts` replaced
    with one for `POST /upload/gmail/v1/users/me/drafts` — with the old
    clause the instance loads but every draft is refused.

  The guide's instance block shows both. A draft without files carries the
  same message as before, and the body limit stays 20,000 bytes. Drafts with
  files hold blob space for a while (the files, and the built message of up
  to about 14.4 MB), so review `blob_budget_bytes`.
- Bumped wasmtime from 48.0.8 to 48.0.10, clearing RUSTSEC-2026-0566
  through RUSTSEC-2026-0573.
- **Components granted `blobs` get twice the fuel per attachment byte: 256,
  up from 128.** Fuel is the per-activation instruction budget a WebAssembly
  component runs on; the attachment share now covers a component that parses
  a whole attachment in memory, not only one that reads it in ranges. The
  attachment cap doubles with it, to 17,179,869,184. No activation's grant
  fell, so every bound measured before still holds, and none of this is a
  setting. A component that loops while holding large attachments can burn
  up to twice the fuel it could before; the 30-second wall-clock deadline is
  unchanged and still bounds it.
  - **For component authors:** a size bound that the attachment fuel set can
    be measured again, upward. `GmailBody`'s bound rises on it (below).
- **A guest's fuel follows the bytes it is handed.** Fuel is the per-activation
  instruction budget a WebAssembly component runs on. Every component is now
  granted 256 fuel per byte of body on the messages an activation delivers
  new, and a component that provides tools also 256 per byte of the body of
  each request the host holds pending for it, in every activation and not
  only the one the request arrived in; the two together are capped at
  4,294,967,296. A component granted `blobs` that provides tools is also
  granted the attachment rate, 256 a byte, for the attachments of the
  requests the host holds pending for it, within the attachment cap of
  17,179,869,184 (both raised in this release; see the attachment rate's own
  entry). The floor, the per-envelope figure and guest memory are
  unchanged, and none of them is a setting: a guest cannot learn its limits,
  so a component's bounds are compiled in against these constants. No
  activation's grant fell, so a bound measured against the old grant still
  holds and may be re-measured upward. The rate was sized on arguments that
  are strings; arguments that are dense JSON cost more to parse and are
  claimed no margin.
  - **For Rust callers of `brenn-wasm`, `brenn-tool-registry` and
    `brenn-activation`:** `ProvidesHost` gains `pending_load`, which an
    implementation must answer; `ProviderRuntime::begin` takes the call's
    arguments and returns the request body beside its guard, and
    `ProviderRuntime` gains `is_pending`; `PortWindow` gains
    `checked_new_envelopes`, which answers `None` where `new_envelopes`
    panics, for a window whose `new_from` passes its length.
- **`provides` gains `pending`, and the provider kit builds nothing for a
  request that has ended.** `brenn:processor/provides@0.1.0` gains
  `pending(id) -> bool`, whether the host holds `id` pending for the
  component; it changes nothing and draws on no budget, and `brenn-guest`
  reaches it through `provides::pending`. `brenn-provider-kit` asks it before
  it issues or queues a call and before it decodes a backlog entry: for a
  request the host has given up — its caller left, its deadline passed — it
  builds no body, attempts no call and spends neither budget, and it deletes
  the request's backlog entry unread instead of answering it. Every call it
  now drops is one the host already refused, so no caller sees a change.
  - **For authors using `brenn-provider-kit`:** a kit component built from
    this release imports `provides.pending` and so needs a brenn that serves
    it; one built against an earlier kit is untouched. `Host` gains
    `pending`, which an implementation must answer; `MemoryHost` answers
    every request pending until a test calls `end_request`.
  - **For Rust callers of `brenn-wasm`:** `ProvidesHost` gains `is_pending`,
    which an implementation must answer. A test harness that tracks no
    requests answers `true` for the ids it hands a component: a kit component
    built from this release drops every call of a request answered `false`,
    with only a `Debug` log line, so such a harness would see no nested call
    at all. It answers `pending_load` with an empty load.
- **A provider component can build a nested call's body.** A provider built on
  `brenn-provider-kit` can now send a body it builds — a document, a MIME
  message around a request's attachment — as an attachment of its nested
  `api-request` call, instead of as text inside the call's arguments, which
  are capped at 64 KiB.
  - **BREAKING for authors using `brenn-provider-kit`:** `Body` gains
    `Body::Built`, so an exhaustive match on it no longer compiles. A call
    with `Body::Built` has its body built by the new `Logic::body`, when the
    call is issued, in whichever activation issues it; a value an earlier
    answer taught reaches it through `derived`. It is sent as the call's
    attachment `BUILT_BODY_ATTACHMENT`, so the component is served by
    `serve_with_blobs` and its class requires `blobs`. At most `BUILD_BUDGET`
    (4) bodies are built per activation, a failed build included; a call past
    it is queued as a call past `CALL_BUDGET` is, and the backlog drains
    oldest first without skipping an entry. A backlog row holding a built
    call is not read by an older kit. `MemoryHost`'s `NestedCall` gains
    `body_content_type`.
- **What bounds a request to a provided tool, stated correctly.** The
  documents said a request's arguments are up to 64 KiB, so a provider's
  request window holds up to Σ (its request limit) × 64 KiB. The 64 KiB cap
  binds only the arguments a component passes to a nested call, and stays; a
  conversation's call is bounded by `[messaging] max_body_bytes` on the whole
  request, so the window holds up to Σ × `max_body_bytes` — at the Google
  guides' 393,216 bytes, six times the old figure. `docs/config-dsl.md` and the
  Google Workspace and Maps guides now say so. Nothing a deployment runs
  changes.
  - **For Rust callers of `brenn-tool-registry`:** `MAX_ARGS_BYTES`, which
    nothing compared against, is removed.
- **A provider's request attachments stay reachable while the request is
  pending.** A WASM provider component (a tool implemented as a WebAssembly
  component) can now open a request's attachments at any point while the host
  still holds that request pending. Before, it could reach them only while the
  request was among the newest in its `tool-requests` window, so under load a
  tool that uploaded attachments after an earlier step (a session call, a
  previous upload) failed `internal` through no fault of its caller.
  - **For operators:** each pending provided-tool call now holds its
    request's attachment blobs until the call ends (completed, failed, timed
    out or abandoned). The hold is not charged against `blob_budget_bytes`;
    at worst the attachments of Σ `max_concurrency` requests (up to 32 blobs
    each) outlive their request by one bus GC interval. No setting changes.
  - **For WASM component authors:** `brenn:processor/blobs@0.3.0` gains
    `open-request(id, name)` with its own `open-request-error`
    (`not-permitted`, `not-pending`, `not-attached`, `quota-exceeded`). It is
    an addition: artifacts built before it load unchanged. It needs the
    `provides` grant and is not served on a surface, where a component that
    imports it is refused at boot. See `docs/component-packages.md`,
    "Contract evolution".
  - **BREAKING for authors using `brenn-guest`, `brenn-provider-kit` or
    `brenn-provider-kit-guest`:** `provides::open_request_attachment` drops
    its `activation` argument and calls `blobs.open-request`;
    `RequestAttachmentError` is now `NotPermitted` / `NotPending` /
    `NotAttached` / `QuotaExceeded`; the kit's `OpenError::LeftWindow` is
    `OpenError::NotPending`; `MemoryHost::request_attachments_left_window` is
    `MemoryHost::requests_not_pending`. A component served by
    `serve_with_blobs` imports `open-request` whether or not its tools take
    attachments, so release brenn before any bundle built on this kit.
  - **BREAKING for code implementing `brenn_wasm::ProvidesHost`** (e.g. a
    test harness hosting a provider): the trait gains a required method,
    `request_attachment(id, name)`, which must hold the request's attachment
    while the request is pending.
- **`GmailBody`: a Gmail message's body whole, as files.** The
  `google-gmail-read` class gains a sixth tool. `GmailBody` takes a message
  `id` and `forms`, one to three of `text` (`GmailRead`'s text, uncut),
  `html` (the message's HTML as sent, decoded to UTF-8) and `rendered` (that
  HTML read as text), and answers each as an attachment of its result. It
  reads messages up to 1,048,576 bytes of Gmail's answer, more than
  seventeen times `GmailRead`'s 60,000 characters, and refuses a larger one naming that
  bound and the `format=raw` route.
  - **For operators:** grant it with `tool gmail-body {}`. The class needs no
    new capability or clause (`blobs` and `messages.get` are already
    granted). Its Σ `max_concurrency` is 14, so the `retain_depth = 32` the
    guide sets on `brenn:tool-results/gmail-read` still holds it. Each call
    leaves Gmail's answer on that inbox for its window: at most 1,048,576
    bytes when the tool answers, and up to `max_blob_bytes` when it refuses
    a message over its bound, since the answer is fetched before it is
    checked. A success also leaves up to three forms of at most 2,359,296
    bytes each, together at most 7,077,888 bytes, on one row of the
    caller's inbox; `docs/google-workspace.md` says how that fits the blob
    pool and the guest's memory (the tool peaks at about 8.69 MiB). A
    deployment sizing `blob_budget_bytes` from a caller inbox's depth × a
    success's forms sizes it for the figure here, and a body answer leaves
    the `gmail-read` windows about 7.3 MiB of the guest's 16 MiB, less than
    the 12 MiB a full result window can weigh at the guide's settings: a body
    answer beside a result window heavier than that traps, and its caller
    waits out the 240 s ceiling.
  - `GmailRead`'s `text_omitted`, and its `internal` when even the message's
    structure cannot be read, now end "…; GmailBody answers the body as
    files", and `GmailAttachment`'s refusal of a part carried inside the
    message names `GmailBody` for body text.
  - `GmailRead` now checks the type of every field of every part of Gmail's
    answer. An answer malformed below a part listed as an attachment, which
    it used to ignore, is now answered `shape`. Gmail's own answers are not
    affected.
  - The tool descriptions of the four other Google classes no longer list
    attachments among the Gmail endpoints the tools do not cover.
- `chrono` and `chrono-tz` are listed by RustSec as unmaintained
  (RUSTSEC-2026-0543, RUSTSEC-2026-0555), with no fixed release. The
  dependency gate ignores those two advisories by id; a vulnerability filed
  against either crate still fails it. Moving off both crates is tracked as
  `chrono-unmaintained`.

## [0.36.0] — 2026-11-15

A huge release; the headline is binary messages and blob attachments on the
message bus, plus python bindings for the bus.

- **Python programs can attach to the bus: the `brenn_bus` extension
  module.** A Python program attaches to a server's remote route as a remote
  attacher, publishes text and byte bodies, subscribes, reads deliveries, and
  moves blobs, through `brenn-attach-client` and not a second implementation
  of the wire. It speaks attach protocol v6 exactly.
  - **For Python authors:** upload bytes or a file as a blob and name it in a
    publish; open, read by range or download to a file an attachment a
    delivery names, or forward it without reading it. A blocking, thread-safe
    API with a timeout on every wait; publish outcomes are returned as values,
    every failure is a named exception, and a rejected token ends the
    attachment instead of being re-dialled. `wss://` works off loopback and can be told which authority to
    trust (`ca_file`). One build loads under CPython 3.13 or later; it is
    built by Bazel only, here or from another Bazel workspace that pins this
    repository by commit. See `docs/python-binding.md`.
  - **New crates:** `brenn-attach-blocking` (`attach/blocking`), the blocking
    client the module maps, and `brenn-attach-python` (`attach/python`), the
    module.
  - **For binders of `brenn-attach-client` (additive):** `NativeConnection`
    is generic over its stream (`NativeConnection<S>`, defaulting to the stock
    connector's) and gains `NativeConnection::from_stream`, so a connector that
    runs its own websocket handshake reuses the connection's framing;
    `refuse_cleartext_bearer` is the stock connector's cleartext guard, public,
    for such a connector to apply; `Uri` is re-exported beside `HeaderMap`.
    No behaviour changed.
- **A surface page makes and forwards blobs; `brenn:processor/blobs@0.3.0` is
  declared stable.** A component on a page granted `blobs` can create a blob,
  write it by range, seal it and publish a message naming it, and pass along an
  attachment it opened, by reference — on a wire channel and on a page-local
  one.
  - **For operators:** a surface whose instance granted `blobs` has a declared
    output on a wire channel now gets a blob door that uploads; a surface whose
    `blobs` instances only read, or publish only page-locally, keeps a
    read-only door. Nothing new is configured: the door follows from the grant
    and the bindings (`docs/config-dsl.md`). Such an instance may attach blobs
    only on its own declared wire outputs, never on the surface error channel
    unless an output is bound to it. Two new `attach_protocol_violation`s
    (fail2ban signal) on any attach session: `BlobWrite` or `BlobSeal` on a
    door without upload, and a publish naming attachments under an
    attribution, or on a channel, that may not take one. A browser session
    with an upload door draws on the shared blob pool up to the per-session
    cap (64 handles, 4 × `max_blob_bytes` held); see
    `docs/security-posture.md` §6.6. A page holds at most 64 MiB of bytes its
    components made, beside the 64 MiB of fetched bytes; the other page
    bounds are in `docs/message-bus.md` §2.1.2a, "A surface page makes blobs".
  - **For component authors:** a surface serves `create`, the `writer`
    resource and `publish` of `blobs@0.3.0`; only `call-async`,
    `call-async-for` and `complete` are refused there, by name. A page uploads
    a blob it made only after the activation returns `ok`, so the backend
    store's verdict arrives after the call: a refused upload, or a forwarded
    attachment whose message left the server's window, loses the activation's
    wire batch whole — never the message without its attachments — and the
    page announces it on its error channel. On a surface `budget-exhausted` is
    the page's own budget. `limits` on a page reports 1 MiB for writes.
    `processor-blobs-test`'s spec gains an optional `feed` port.
  - **`brenn:processor/blobs@0.3.0` is STABLE**, with the promise
    `brenn:processor@0.2.0` carries: no type or function is changed or
    removed and no case or field is added, while brenn serves it an artifact
    built against the shapes in the file loads and means what it meant, and
    it is not removed without a removal period announced here and in the
    file. An addition is a function no existing component imports, added
    beside the ones the interface has; a host coming to serve a function it
    refuses today is an addition of the same kind. Bounds stay host policy.
    See `brenn-wasm/wit/processor-0.3.wit` and `docs/component-packages.md`.
- **A surface page reads message attachments.** A component on a page granted
  `blobs` can open an attachment of a message in its windows and read its
  bytes, which cross the surface websocket as binary frames — the camera-frame
  case of a `remote` publishing a blob per frame and a `surface` rendering it.
  - **For operators:** `blobs` is now legal on a placed instance. A surface
    `in` binding takes `fetch_bytes`, the largest attachment the page fetches
    on that channel (default 1 MiB on an instance granted `blobs`, 0
    otherwise, at most 64 MiB). Any value above zero makes every reader of
    that channel on the page wait for the fetch, up to a bound set by the
    channel's depth (`⌈(depth + 4) / 4⌉ · 30 s` attached, 60 s at depth 1);
    see `docs/config-dsl.md`, "Instances and bindings", and
    `docs/message-bus.md` §2.1.2a. The surface route moves to attach v6 and
    the bindings document to version 8 together; page and server deploy
    together and a page from an earlier release is refused until it reloads.
    A page reports a refused or failed fetch, an attachment over the
    session's bytes held, and bytes dropped for room on its error channel
    once per kind, per channel, per connection.
  - **For component authors:** a surface serves the read side of
    `brenn:processor/blobs@0.3.0` — `limits`, `open` and a blob's `size`,
    `content-type` and `read` — and, with the entry above, its producing
    side; the surface check now type-checks the `blobs` import.
    **BREAKING, in place:** `read-error` gains `unavailable`, which a page
    answers for bytes it does not hold and the backend never answers. A
    component that calls `blob.read` must be rebuilt against this release's
    `brenn-guest` before it links on the backend, and a `match` on
    `brenn_guest::blobs::ReadError` gains an arm for `Unavailable`. No tagged
    release served the earlier shape, and the interface is declared stable
    with this shape (the entry above). See `docs/component-packages.md`, "Contract evolution".
- **Large files through `api-request`, and a new `GmailAttachment` tool.**
  The host tool `api-request` (the way components and agents call an
  OAuth-protected API) can now send a request body from a blob attachment
  and return a successful response body as an attachment, streaming both,
  instead of squeezing them through JSON size caps. Gmail attachments up to
  50 MB can now be fetched.
  - **BREAKING for operators of `GoogleGmailRead`:** the class now requires
    the `blobs` grant and is refused at load without it. Add `blobs` to the
    `gmail-read` instance's `grants`, and allow
    `GET gmail.googleapis.com/gmail/v1/users/me/messages/*/attachments/*` in
    its `api-request` grant. Each fetch's answer stays in the blob pool for
    the result inbox's window of 32, up to about 2.1 GB at the largest size,
    twice the default 1 GiB `blob_budget_bytes`; raise that or lower
    `max_blob_bytes`. See `docs/google-workspace.md`.
  - **For operators generally:** `api-request` now consumes the blob pool, up
    to `max_blob_bytes` per call and eight calls in flight; attachment
    answers are bounded by `max_blob_bytes`, not `max_response_bytes`. Size
    `blob_budget_bytes` to at least inbox depth × `max_blob_bytes`, summed
    over inboxes receiving attachment answers. A full pool refuses new blobs
    until a window turns over. See `docs/security-posture.md` §7.6 and §9f.
  - **For agents and app authors:** `api-request` takes `body_attachment`
    (send a named attachment as the body) and `response_attachment` (return a
    2xx body as a named attachment). Oversized answers report
    `response_too_large`; a full pool, `blob_store_full`. `GmailRead` now
    lists a `part_id` per attachment, which `GmailAttachment` (`id`,
    `part_id`) uses. See `docs/config-dsl.md`, "The OAuth tools".
  - **BREAKING for component authors using `brenn-provider-kit` /
    `brenn-provider-kit-guest`:** the kit gains attachment support
    (`serve_with_blobs`, blob methods on `Step`,
    `ApiCall::answer_as_attachment`, `read_api_blob`), and several types
    change shape: `Logic::plan`/`advance` take a `Step`, hand-written `Host`s
    implement `Blobs`, `Body` gains an `Attachment` variant, and some structs
    gain fields. Components that keep calling `serve` keep their grants. The
    kit also stops advancing a nested call answer arriving more than 240 s
    after its activation. See `docs/component-packages.md`, "Contract
    evolution" and "A tool provider on the kit".
  - **For WASM component authors:** `response_attachment` is refused to an
    instance without the `blobs` grant. Instances with `blobs` get extra fuel
    (128 per byte) for attachments on newly delivered messages. `brenn-guest`
    gains `provides::open_request_attachment`. No WIT changed.
- **Remote attachers can move blob bytes: attach protocol v6.** A native
  daemon connected over the remote websocket (a "remote attacher") can now
  read, upload and forward blobs — the large binary attachments on bus
  messages, e.g. camera frames — instead of seeing only their references.
  - **For daemon authors:** v6 is v5 plus blob transfer. A session with a
    blob door (`Welcome.blobs`, `AttachmentFacts::blobs()`) opens an
    attachment of a message by (channel, message id, attachment name) and
    reads its bytes by range, uploads a blob of declared size in chunks, and
    publishes messages naming its blobs by session handle — single publishes
    and `PublishBatch` entries alike. Forwarding an attachment copies no byte.
    `brenn-attach-client` gains the `blobs` module (`BlobPlane`), which drives
    the frames and hands the daemon chunk ranges to read and chunks to write;
    the crate docs' "For a daemon author: blobs" section walks the loops,
    handle lifetime and every outcome. v4 and v5 are still served, and a
    daemon that stays on `exactly(5)` is delivered what it was before:
    references as metadata, no blob frames. See `docs/message-bus.md` §2.1.2a
    and §2.1.3.
  - **BREAKING for operators:** a remote's subscribe ACLs now grant its token
    the *bytes* of attachments on those channels (v6), not only their
    references; the tool substrate's channels are excepted. A remote with
    publish authority is a producer against `blob_budget_bytes`, and every
    remote session with the blob door — whether or not the remote may
    publish — can keep up to 4 × `max_blob_bytes` of the pool charged, by
    uploads or by holding opened attachments. No configuration changes;
    review remote subscribe matchers. See `docs/security-posture.md` §9a.
  - **BREAKING for binders of `brenn-attach-client` at the pin bump:**
    `SUPPORTED_VERSIONS` is 4..=6, so a daemon offering the whole range now
    negotiates v6; `ClientFrame` and `ServerFrame` gain variants and
    `PublishOutcome` / `PublishBatchOutcome` gain `AttachmentNotHeld`
    (exhaustive matches break, deliberately); `ClientFrame::Publish`,
    `BatchEntry`, `PublishRequest`, `ServerFrame::Welcome` and
    `AttachmentFacts` gain a field (struct literals break; the constructors do
    not); `OutboxSteps` and `Discarded` gain `released`, and `Discarded` is no
    longer `Copy`; a parked flush naming a blob handle is dropped at a detach
    and reported, and one in flight is forgotten, as every in-flight batch is.
    Additive: `FlushBatch::handles`, `DeferredOpKind::body`,
    `store::DeferOp::body`, `Codec::attachments_charged_bytes`, the `blobs`
    module.
- **Tool calls can carry blobs (binary attachments).** A tool can now be
  handed blobs with its arguments and answer with blobs beside its result.
  - **For agents and app authors:** a tool result with attachments is shown to
    the conversation as `{"result": …, "message_id": "<uuid>", "attachments":
    {<name>: {"size", "content_type"}}}`; `BrennSend`, `brenn-tool save` and
    later tool calls name those attachments by that `message_id` until the
    conversation's context is reset. A result without attachments is the bare
    value, as before. A tool that accepts attachments takes an `attachments`
    argument. See `docs/agent-tool-cli.md`.
  - **For WASM component authors:** `brenn:processor/blobs@0.3.0` gains
    `call-async`, `call-async-for` and `complete`, the attachment-carrying
    forms of the tool-call and provider-answer functions; `brenn-guest` gains
    matching `*_with_attachments` helpers. A provided tool takes attachments
    only if its `provide` statement sets `accepts_attachments = true`
    (default `false`); otherwise such a call is refused
    `attachments-not-accepted`. Using the new functions also requires the
    `tools` or `provides` capability they extend. `brenn-guest`'s
    `ToolResult` and `ToolRequest` gain a public `attachments` field, so code
    building or exhaustively destructuring them must change at its next pin
    move. Components using none of this are unaffected. See
    `docs/component-packages.md`, "Blobs and tool calls".
- **BREAKING for operators: `app-tool-results` is a reserved channel name.**
  Each app granted an asynchronous tool gets a private inbox channel,
  `brenn:app-tool-results/<app-slug>`, holding the attachments of tool results
  handed to its conversations (default depth 16, tunable with a `[[channel]]`
  block; `send_rate` is refused). A configuration declaring a channel named
  `app-tool-results…`, or a matcher reaching into it, is now refused at start.
  Other configurations need no change. See `docs/message-bus.md` §2.7.
- **BREAKING for WASM component authors: `blobs.publish`'s error type
  changed.** It now wraps `ports@0.2.0`'s `publish-error` instead of restating
  its cases, so a component calling `blobs.publish` must be rebuilt (its
  source does not change) and its class must require `ports`, as any class
  that sends already does.
- A watch of the tool channels (`brenn:tools/`, `brenn:tool-requests/`,
  `brenn:tool-results/`) now sees attachment names, sizes and content types,
  but can never save or forward the blobs themselves. See
  `docs/security-posture.md` §7.6.
- **BREAKING for operators who install `brenn-tool` by hand: the tool socket
  protocol is v2.** The server refuses a v1 `brenn-tool` by name
  ("unsupported protocol version"). The binary at `tool_cli_path` must be the
  one built with the server.
- **For agents and app authors: LLM conversations can now forward, save and
  attach blobs** (large binary attachments on bus messages). A conversation
  refers to an attachment by the message id and attachment name it was shown,
  never by a raw blob id, and only for a message it has actually been handed
  since its context was last reset (start, compaction, a `/` command, or a
  server restart) and that is still retained on a channel the app may read;
  refusals say why (`not_read`, `not_retained`, …). `BrennSend` gains
  `attachments`, forwarding by reference with no bytes copied. Two new
  `brenn-tool` commands, run from the conversation's shell: `brenn-tool save`
  writes an attachment or message body to a file (atomically, once every byte
  has arrived), and `brenn-tool send` publishes with local files attached.
  No blob bytes pass through the model's context. Apps that disable `Bash`
  get forwarding only. See `docs/agent-tool-cli.md` and `docs/message-bus.md`
  §2.1.2a.
- **For agents and app authors: a content type on a conversation's text.**
  `BrennSend` and `BrennMessageEdit` take an optional `content_type`, which
  must be a text type (`text/*`, `application/json`, `+json`, with a UTF-8
  or US-ASCII charset if any); binary data goes as an attachment instead. An
  edit replaces body and type together: `body` without `content_type` leaves
  the message untyped, and `content_type` without `body` is refused.
- **For WASM component authors: a `blobs` interface.** A new interface, `brenn:processor/blobs@0.3.0`
  (`brenn-wasm/wit/processor-0.3.wit`), under a new grant word, `blobs`, lets
  a component on the backend host open an attachment of a message in its
  windows by port, index and name, read it by byte range, create a blob by
  positional writes and seal it, and publish a message naming blobs it opened
  or made — forwarding one by reference copies no byte. Publishing with
  attachments needs `ports` as well; `ports.publish` and its siblings still
  publish no attachments, so republishing a body drops what it carried. A
  component names the world `brenn:processor/processor@0.3.0`, the 0.2.0 world
  with `blobs` imported, and is still a 0.2.0 component; `brenn-guest` binds
  that world and adds `brenn_guest::blobs`, `publish_with_attachments` and
  `Message::attachments`. A component that does not call the interface
  imports nothing new. A surface serves the interface too, as the entries
  above say, and it is declared stable. See `docs/component-packages.md`.
- **BREAKING for operators: a blob directory records the database it belongs
  to.** The blob directory now holds a file `owner`, mode 0600, holding the
  store generation of its database (`messaging_store_identity.generation`).
  The first boot over an empty directory writes it; every later boot compares
  it with the database's. A directory whose record names another database,
  one that holds blob files and no record, and one whose record is not a
  single generation line stop the boot, naming both generations, before
  anything is deleted. A fresh database beside an old blob directory used to
  start, its boot sweep deleting the old files; it now refuses: remove the
  directory, or point `messaging { blob_dir }` elsewhere, and the host claims
  a new one. A database restored or moved together with its own directory
  boots unchanged. To repair a lost record beside the database that owns the
  files: `sqlite3 <database> "SELECT generation FROM messaging_store_identity"
  > <blob_dir>/owner`. See `docs/message-bus.md`, the blob section's backup
  and restore paragraph.
- **BREAKING for operators: the database and the blob directory are one unit
  to back up, restore and roll back.** Attachment bytes live in a directory
  beside the database (`messaging { blob_dir }`, default the database path
  with `.blobs` appended), and Brenn refuses to start on a database whose
  messages name a blob that directory no longer holds. Blob files are deleted
  as their messages are evicted, so a snapshot of the database alone,
  restored later, can name files that are gone. Back up and snapshot the two
  together, and prune them together. A component granted `blobs` can
  publish an attachment onto a durable channel, so a deploy or backup script
  must cover the directory from this release on. To
  start a host that refuses: delete the message rows the panic names with
  `sqlite3`. See `docs/message-bus.md`, the blob section's restore paragraph.
- **For operators and WASM component authors: a blob store, and attachments
  on bus messages.** A message can now name large byte objects — blobs — by
  reference, beside its body, which stays capped at `max_body_bytes`. A blob
  lives exactly as long as a message the host holds names it, or host code
  holds it open, and is then deleted: at once when only ring channels ever
  named it, at the next reap (hourly, and after a cancel) when a durable row
  did. Each message carries up to 32 named attachments in a new open-header
  key, `attachments`, a map from name to `{id, size, content_type}`. A 0.2.0
  component sees the full references in its header, on the backend and on a
  page, and so does an attach v5 session; a conversation is shown each
  attachment's name, size and content type without its id; the eviction
  archive records the references but not the bytes; 0.1.0 components and
  attach v4 sessions see nothing, byte-identically. A backend component
  granted `blobs` creates, reads and forwards blobs (the entry below), and a
  conversation forwards, saves and attaches them by name (the entry above);
  remotes and pages see references as metadata only and can neither create
  nor redeem one. Three new keys in the `messaging` section:
  `max_blob_bytes` (default 64 MiB), `blob_budget_bytes` (default 1 GiB) and
  `blob_dir`; the store creates its directory, mode 0700, on first boot, and
  refuses to start if `PRAGMA synchronous` is below `FULL` or the directory
  holds anything that is not a blob file or its owner record. The database gains a nullable
  `attachments` column on `messaging_messages`, two tables
  (`messaging_blobs`, `messaging_blob_refs`) and two triggers on the first
  boot. See `docs/message-bus.md` §2.1.2a and `docs/config-dsl.md`.
- **For Rust authors building `brenn-envelope` types:**
  `MessageEnvelope` and `EnvelopeHeader` gain the `attachments` field, so a
  struct literal needs it (`Default::default()` is the empty map), and the
  shape trait `BodyShape` is renamed `EnvelopeShape` and gains a required
  `serialize_attachments`.

- **For operators and WASM component authors: MQTT and webhook ingress can
  carry the payload itself.** A tuning block for an `mqtt:` or `webhook:`
  channel — exact or prefix — may now state `non_text = true`. Such a channel
  takes the *raw* ingress shape: the body is the payload byte for byte, typed
  with the MQTT publisher's Content-Type or the webhook endpoint's content
  type, and the message carries the transport's metadata (client, topic, QoS
  and receive time; or the endpoint, key id, client address, receive time and
  headers with credentials redacted) in a new open-header key, `transport`.
  Binary MQTT payloads and non-UTF-8 webhook bodies reach a 0.2.0 component
  as bytes; a conversation is shown a non-text body as the usual placeholder
  with the metadata beside it. Without the opt-in nothing changes: both
  wrappers, `MqttEnvelope` and `WebhookEnvelope`, are byte-identical and stay
  the default, so the opt-in is the whole compatibility story — there is no
  dual period and no shim. An `mqtt:` tuning block that is `non_text` may also
  state `untyped_content_type = "<media type>"`, which types payloads the
  broker delivers with no Content-Type (MQTT 3.1.1 publishers, firmware that
  sets none); without it an untyped payload that is not UTF-8 is dropped. A
  raw webhook endpoint must state a `transport_ceiling_bytes` within
  `messaging { max_body_bytes }` and a content type the bus accepts; one with
  a non-text content type such as `application/octet-stream` accepts any
  bytes. Narrowing a raw channel back to the default needs a restart.
  `brenn-guest` gains `mqtt_body()`, `mqtt_transport()` and
  `webhook_transport()` beside `webhook_body()`, which now names the raw shape
  when handed it, and `MqttPayloadBody::text()`. The database gains a nullable
  `transport` column on the first boot. A raw channel's stored messages are
  not readable by an earlier build: rolling back with them stored is not
  supported. See `docs/message-bus.md` §2.7.1 and `docs/config-dsl.md`.
- **For WASM component authors: `brenn:processor@0.2.0` is stable, and the
  string contract `brenn:processor@0.1.0` is being removed.** The 0.2.0
  shapes (`bus`, `ports`, `calls` and the `receive` export) are now frozen:
  a breaking change would be a new version served beside 0.2.0 for a dual
  period. The contract grows only by new interfaces under new grant words
  and by new keys in the open message header, which a guest ignores when it
  does not know them. Nothing changed in the shapes, so 0.2.0 artifacts built
  at an earlier pin need no rebuild.
  The string contract is served by this release and the three after it, and
  is not removed before the out-of-tree components known to us
  (`brenn-fastmail`, `brenn-component-demo` and one private repository) have
  moved. Only the string contract's world and its `types`, `ports@0.1.0` and
  `calls@0.1.0` interfaces go; the other `@0.1.0` interfaces stay. To move,
  rebuild on a current `brenn-guest` pin. Operators running 0.1.0 components
  or surface kinds see a `warn!` naming them at boot and after each reload;
  nothing stops working yet. Surface hosts now also refuse a 0.2.0 kind
  that imports a `ports` or `calls` interface under a near-miss version or
  with an extra function. See `docs/component-packages.md`, *Contract
  evolution*.
- **For operators and WASM component authors: binary bodies on surface pages
  are now tested in a real browser.** Until now, the path that carries
  non-text (binary) message bodies between a surface page and the server had
  run only in a node.js stand-in. `make e2e` now runs it in Chromium
  (`e2e/tests/bytes.spec.ts`): a page-hosted `brenn:processor@0.2.0`
  component stores a binary body on a page-local `io` port and reads it back,
  and publishes one over the page's websocket and receives it, each checked
  byte for byte, including a 3,000,000-byte body. A new `make e2e-measure`
  target runs the browser timing workload on its own; one run's figures are
  in `docs/page-rig-timings.md`. Those figures show small (~48 KB) delivered
  rows arriving slower than 1 MiB ones; the cause is not yet established and
  is tracked as `surface-small-row-delivery-latency` in `TODO.md`. The e2e
  server now reads its own mounts document, which adds the test component;
  the dev server and deployments are unchanged.

- **For operators and WASM component authors: the surface page speaks attach
  protocol v5, and a surface may bind a channel declared `non_text`.** The
  browser page and the surface route move together from attach v4 to v5 and
  only v5; v4 is served to remote daemons only. Every publish and every
  delivery on a surface page, its own bindings document included, is now a
  binary websocket frame, whatever its body — for chrome and every in-tree
  0.1.0 component on text bodies as for a 0.2.0 kind's bytes. `make e2e` runs
  the page's wire in Chromium, text bodies and non-text bodies both (below).
  **Before deploying this release, run `make e2e`**: `e2e/tests/bar.spec.ts`,
  `e2e/tests/bytes.spec.ts` and `e2e/tests/surface-reload.spec.ts` load
  surface pages and are the only tests that run the page's wire in a
  browser.
  - A port of a page-hosted `brenn:processor@0.2.0` kind bound to a channel
    that leaves the page carries typed text with its type, and non-text
    bodies when the channel is declared `non_text`: published, parked,
    edited and delivered byte for byte with the content type. A non-text body
    for a text-only channel is refused to the kind with `text-only-channel`
    and nothing is sent. Shown on the node rig; `e2e/tests/bytes.spec.ts`
    runs the publish and the delivery in Chromium.
  - A surface port that does not state `non_text` may be bound to a channel
    declared `non_text`. The `non_text` key on a surface binding is refused
    in two cases: on a reserved `local:brenn/` plane, and on a declared
    `ephemeral:` or `brenn:` channel that is text-only (reported by
    `config-check`, boot and reload, naming the port and the channel). A
    surface config channel declared `non_text` is still refused: a page reads
    its bindings document as text.
  - A served surface instance whose kind is built against
    `brenn:processor@0.1.0`, bound by input or output to any `non_text`
    channel, page-local or not, is refused at boot and at reload, naming the
    port, the channel and the fix.
  - Reload: changing the declaration of a surface's config, geometry or
    status channel now closes that surface's pages, which reload on the new
    configuration, as a change to a channel it binds already did; changing
    the surface error channel's closes every surface's. A reload that
    redeclares a durable channel text-only while a running surface may
    publish to it (an output bound to it, or one of its own platform
    channels) is refused, naming the channel as the reloaded configuration
    names it; restart instead.
  - On the surface route a non-text body offered for a text-only channel is a
    protocol violation: the session is closed, the
    `attach_protocol_violation` security event is logged and fail2ban sees
    it. A page is told every channel's declaration and never sends one. On
    the remote route it is still answered `TextOnlyChannel`.
  - The surface route's frame cap is v5's, 204 800 bytes at the default body
    cap, down from 532 480.
  - The surface bindings document's version is 7: a page older than its
    server reloads, as at every version move.
  - Limits on a stream delivered to a surface are in `docs/message-bus.md`
    §2.1.4; node-rig and Chromium timings of binary rows over the page's wire
    are in `docs/page-rig-timings.md`.

- **For operators and WASM component authors: `non_text` on a surface's
  page-local channels.** A surface binding tail (`in`, `out`, `io`) may now
  state `non_text = true` where its channel stays on the page — a `local:`
  address outside the reserved `local:brenn/` planes, a free `io` port, or a
  link whose endpoints are all on that surface; the page ring is `non_text`
  when any surface binding on its address says so. A page-hosted
  `brenn:processor@0.2.0` kind then publishes, parks, edits and reads back
  non-text bodies there with their content type — binary state on an `io`
  port with no base64. Shown on the node rig; `e2e/tests/bytes.spec.ts` runs
  the publish and the read-back in Chromium.
  A served surface instance whose kind is built against
  `brenn:processor@0.1.0`, bound by input or output to such a channel, is
  refused at boot and at reload, naming the port and the channel. The key is
  refused on a reserved plane, and on a channel that leaves the page and is
  declared text-only (above). Node-rig timings against 0.1.0's base64 in JSON
  are in `docs/page-rig-timings.md`, with Chromium figures for the binary
  state.
- **For WASM component authors: the surface host serves
  `brenn:processor@0.2.0` beside 0.1.0.** A page-hosted 0.2.0 kind is handed
  each message as a header plus the body's bytes and content type, as on the
  backend. On a page it carries typed text on any channel with its type, and
  any body in a sync call and reply between two 0.2.0 kinds; a non-text body
  is held at rest on any channel declared `non_text`, page-local or not, and
  refused with `text-only-channel` elsewhere (above). The server holds a
  0.2.0 surface kind's artifact to the import names it serves and
  type-checks its `ports` and `calls` imports and its `receive` export: one
  that fails either is withheld under a bundle mount and a boot refusal under
  brenn's own. The page manifest names each kind's contract. A brenn whose
  surfaces instantiate any 0.1.0 kind names those kinds in one `warn!` at
  boot and after each applied reload.
- A page-hosted component's refused publish, edit or call now hands the
  guest the refusal's diagnostic (`invalid-payload`, `invalid-body`), as the
  backend does.
- A page-hosted component's oversize publish is refused with
  `invalid-payload` and no longer terminates the instance. Before, the
  refusal reached a `brenn:processor@0.1.0` guest with no payload, the
  generated glue could not lower it, and the instance failed with
  `expected a string`.
- **BREAKING for component authors: `brenn-guest` binds
  `brenn:processor@0.2.0`.** Every component built on the guest SDK from this
  pin on is a 0.2.0 artifact; every in-tree SDK component and surface kind is
  one, with the same bodies on the same channels as before. The one edit
  every component makes: `Processor::receive` returns
  `Result<Option<Body>, Error>` (a text reply is `Ok(Some(text.into()))`).
  Bodies are `Body` (owned, read as bytes, text or JSON, with its content
  type) and `BodyRef` (borrowed, to publish); `publish`, `publish_deferred`,
  `repark`, `defer_edit` and calls take any body, and a `&str` is still
  untyped text. `PortWindow::new_raw`/`context_raw` become
  `new_messages`/`context_messages` (`Message`: header JSON, `Body`,
  `envelope()`), `DeferredEntry::payload` becomes `body`, `calls::CallError`
  is an SDK enum, and `Error` gains `TextOnlyChannel` and `InvalidBody`. The
  JSON conveniences still publish untyped text. `RetainedState<T, C = Json>`
  takes a `StateCodec`, so a state can be stored as bytes with no JSON and no
  base64, on a channel (or surface binding) declared `non_text`; worked
  example `brenn-wasm/components/processor-sdk-bytes-test`.
  `surface/page-harness` drives 0.2.0 kinds only. 0.2.0 is stable (the entry
  above); to stay on 0.1.0 for the rest of its removal period, keep the older
  pin or write raw 0.1.0 bindings. The full
  before/after list is in `docs/component-packages.md`, *Contract evolution*.
- **BREAKING for component authors: `MessageEnvelope.body` is a
  `MessageBody`.** A message body on the bus is now bytes with an optional
  content type. Under the current component contract every body a component
  is handed is still text, delivered exactly as before. Read it with
  `envelope.text_body()?` or `body.as_text()`; `json_body` and
  `webhook_body` are unchanged. `MessageEnvelope` no longer implements
  `Serialize`; use `to_envelope_json`. Paid at the next `brenn-guest` pin
  bump; no deployed component is affected. See
  `docs/component-packages.md`.
- A conversation shown a bus message with a content type sees
  `content_type` beside the body, and a message whose body is not text sees
  a `{"binary": true, "content_type": …, "size": …}` placeholder instead of
  its bytes. The first thing that can publish such a message is a component
  built against the byte-carrying contract, below; see
  `docs/message-bus.md` §2.1.2.
- **For WASM component authors: a byte-carrying component contract,
  `brenn:processor@0.2.0`.** The backend host now serves it
  beside the string contract `brenn:processor@0.1.0`. A 0.2.0 component is
  handed each message as an open JSON header plus the body's bytes and
  content type, and publishes, parks, edits, calls and replies with bytes and
  an optional content type; no base64 anywhere. The new WIT is
  `brenn-wasm/wit/processor-0.2.wit`, read together with `processor.wit`. New
  refusals a 0.2.0 guest can draw: `invalid-body`, `text-only-channel`,
  `text-only-peer`. In a sync call between contracts, a 0.2.0 callee may
  reply with a non-text body to anyone; a 0.1.0 caller handed such a reply
  traps, and the callee is unaffected. The guest SDK builds 0.2.0 (the
  `brenn-guest` entry above); a raw 0.2.0 component reads both WIT files.
  0.2.0 is stable from this release, and 0.1.0's removal period begins with
  it (the entry above). **No existing
  component is affected**: 0.1.0 artifacts load and run as before. See
  `docs/component-packages.md`, *Contract evolution*.
- **For operators: `non_text` on channels and binding tails.** A channel
  declared `non_text = true` may carry non-text bodies; every channel is
  text-only by default, so no existing configuration changes meaning. A
  consumer's binding tail may say `non_text = true`, which declares a link or
  free `io` channel `non_text` and is checked against a declared channel. A
  0.2.0 component's non-text publish to a text-only channel is refused to it,
  and the backend writes one `warn!` per activation naming the component, the
  port and the channel, so a missing declaration shows up in the log.
  Refused, at `config-check`, boot or reload as each can be checked: a 0.1.0
  component with any port on a `non_text` channel (at boot and reload, naming
  the component, port and channel); a `non_text` key on a surface binding
  whose channel is a reserved plane or is declared text-only, and a
  `non_text` surface config channel; redeclaring a durable channel text-only
  while it holds a non-text message, and, on reload only, while a `remote`'s
  publish ACL covers it or a running surface may publish to it (restart
  instead). A remote attacher on attach protocol v4 subscribing to a
  `non_text` channel is answered `Unavailable`, which is not a violation and
  feeds nothing to fail2ban; one on v5 is served it. See
  `docs/config-dsl.md` and `docs/message-bus.md` §2.1.
- **For remote daemons: attach protocol v5, served beside v4 on the remote
  route.** Under v5 the body-carrying frames (`Publish`, `PublishBatch`,
  `Deliver`, `DeferredView`) are binary websocket messages — a JSON header,
  then the raw body bytes — so a remote can publish and receive non-text
  bodies, onto and from channels declared `non_text`, and receives every body
  with its content type. A non-text body offered for a text-only channel is
  answered with the new outcome `TextOnlyChannel` and nothing is stored. v5
  carries no blob transfer. The browser page and the surface route speak v5
  only (above). **A deployed v4 daemon keeps everything it has, frame for frame**: it
  negotiates v4 and sees the frames it saw before. It does not get non-text
  bodies, content types, `non_text` channels, or the non-text entries a v5
  session of the same remote parked, which are left out of its deferred
  view. No end date is set for v4's service. Limits on what a remote can
  sustain are in `docs/message-bus.md` §2.1.3.
- **BREAKING for daemon authors, at the next `brenn-attach-client` pin
  bump.** The body-carrying types (`PublishRequest`, `BatchEntry`, deferred
  edits, `DeferredViewEntry`) hold a `MessageBody` instead of a `String`
  (`"text".into()` converts); build requests with `PublishRequest::new` and
  `BatchEntry::new`. `ConnConfig` gains `versions`, the range offered — offer
  `VersionRange::exactly(5)` to require bytes, or `SUPPORTED_VERSIONS` to
  keep working against an older server. `TransportConnection::send_text` is
  `send(WireMessage)` and `TransportEvent` carries one `Message(WireMessage)`.
  The batch law and frame cap moved onto `Codec`
  (`AttachmentFacts::codec()`), and `FlushBatch::frame_bytes` and
  `Outboxes::on_attached` take it. `PublishOutcome` and
  `PublishBatchOutcome` gain `TextOnlyChannel`; `BatchAnswer` gains
  `outcome`. A daemon asks `AttachmentFacts::carries` before publishing a
  body its attachment may not carry. See the `brenn-attach-client` crate
  documentation.
- The attach frame cap is per protocol version: v4's is unchanged (532 480
  bytes at the default body cap); v5's is 204 800. `docs/config-dsl.md`
  documents the `remote` block's `publish_burst`, `publish_per_sec`,
  `max_sessions` and `max_subscriptions`.
- **Slower first start.** The first start of this release rebuilds the
  message search index once and builds one new index on the message store,
  both before serving, so it takes longer on a large database. No stored
  message is rewritten, and the previous release still opens the database.
- **Notice: the string component contract is being removed.** The backend
  names loaded `brenn:processor@0.1.0` components, and instantiated 0.1.0
  surface kinds, in one `warn!` each at boot and after each applied reload;
  the notice states the removal period and asks for a rebuild against
  `brenn:processor@0.2.0` (the entry above).
- In the browser surface, a publish carrying an unrecognized urgency value
  now traps the component instead of being refused as `invalid-payload`.
  Such a value can only come from the page glue and the kernel disagreeing,
  not from a component bug.
- Development: `make check` now runs the real browser page kernel, the page
  glue and real transpiled components together in node (the "page rig"),
  covering publish, retained round trips and sync calls on the page.

## [0.35.1] — 2026-11-13

- **API-key spend is no longer overcounted.** A key profile's month-to-date
  `usd` in the account ledger (`brenn:agent.accounts`) was inflated for
  long-lived sessions: each turn was charged from Claude Code's running
  session total, so resumed or respawned sessions re-booked spend already
  counted. Each turn is now charged only the cost Claude Code states for that
  result alone. Expect the ledger at or below the Platform console's figure;
  one well above it is a bug to report. Already-booked months are not
  corrected.
  - A result carrying a running total but no per-result cost is counted as a
    turn with no cost, logged as an error and alerted once per server run.
  - A negative per-result cost now panics where it is read, for every profile.
- The status-bar cost pill's per-turn and conversation figures use the same
  per-result cost; the conversation figure is the conversation's stored total
  and does not reset at compaction. The chat-protocol `cost_usage` field
  `since_last_compaction_usd` keeps its name (now a misnomer, documented).

## [0.35.0] — 2026-11-12

- **Claude profiles can be API keys.** A `claude_profile` that states
  `api_key_file` is a key profile: Claude Code is spawned with the key in
  `ANTHROPIC_API_KEY` instead of a token in `CLAUDE_CODE_OAUTH_TOKEN`, and an
  agent's `claude_profiles` may list token and key profiles together and
  switch between them like any two profiles. To turn it on, declare a
  `claude_profile` with `api_key_file` (a 0600, one-line file) and list it in
  an agent's `claude_profiles`; `budget_usd`, `reset_tz` and
  `exhausted_markers` are optional and apply to key profiles only.
  - The credential-variable guards apply to key profiles unchanged: none of
    the six credential variables may reach a profiled agent from its `env`,
    an integration, or (for a bare agent) the server's environment.
  - `--bare` with `claude_profiles` stays a config error for either kind; the
    message no longer names only the token.
  - The `expires` warning names the remedy for the profile's kind.
- **Agent sessions and Claude accounts on the bus.** Brenn keeps a ledger
  per configured profile — a key profile's month-to-date spend and turn
  count, and the refusal flags observed on either kind — and publishes it,
  with the running Claude Code sessions, on three channels:
  `ephemeral:agent.sessions` (state: every running session),
  `brenn:agent.ended` (fact: one per run that ended) and
  `brenn:agent.accounts` (state: the ledger, read back at boot). To turn it on,
  declare all three — stamp `AgentStatus()` from the new `@agent-status`
  library module — and grant consumers access with ordinary ACLs. Declaring
  some without the rest is refused at boot. The bodies are
  `brenn_envelope::agent`'s; `docs/message-bus.md` §2.9 and
  `docs/claude-accounts.md` describe them.
  - A refused turn is recognised: a `rejected` `rate_limit_event` flags the
    subscription bucket it names until its reset; under a key profile, an
    error turn matching `exhausted_markers` flags the account until a turn
    succeeds or the month rolls over. Brenn never publishes `cleared_at`.
  - The channels need a send rate of at least 240 in a burst and 30 a
    second sustained. The `AgentStatus` assembly states it, so
    `[messaging] default_send_rate` does not reach them; boot refuses any of
    the three below that. A publish refused at runtime, by the rate gate or
    otherwise, stops the server.
  - A conversation that subscribes to `ephemeral:agent.sessions`, or to
    `brenn:agent.accounts` while it runs under a key, should bind with
    `push_depth = 0`: otherwise it is woken by its own turns, back to back,
    and under a key each one is billed.
  - Boot refuses to start when the retained `brenn:agent.accounts` message
    was written by anyone but `system:agent-status`; remove that principal's
    publish grant and clear the channel.
- **`allowed_warning` no longer reports "Rate limited".** A
  `rate_limit_event` whose status is `allowed_warning` used to broadcast a
  rate-limit error to the conversation although the turn was allowed; it now
  logs the utilisation warning only. Only `rejected` broadcasts the error. An
  unrecognised status is logged and raises one Warning alert per process, and
  is not shown as an error.
- **Components can publish MQTT messages that expire.** The
  `brenn:processor/mqtt` interface gains `mqtt-publish-expiring`:
  `mqtt-publish` plus the MQTT 5 message-expiry interval, in seconds, with the
  same grant, budgets and errors. A retained message published with it stops
  being retained when the interval runs out, so a component can publish a
  liveness record that disappears from the broker when the component stops
  refreshing it. A zero interval is refused as `invalid-payload`. In
  `brenn-guest`, `mqtt::try_publish_expiring` and `mqtt::publish_text_expiring`
  call it.
  - Compatibility: additive, with no package version bump. A component built
    before this keeps linking, because a component imports only the functions
    it calls and `mqtt-publish` is unchanged. A component that calls the new
    function does not link on an older Brenn. No shim in either direction.
  - The LLM `MqttSend` tool is unchanged and sends no expiry.

## [0.34.0] — 2026-11-10

- **`api_key_provider` changes now take effect on reload, without a
  restart.** Adding, changing or removing an `account`, editing a provider's
  `header`, `api_hosts` or `max_response_bytes`, switching a provider between
  a single `key_file` and per-user `account` blocks, adding or removing a
  provider block, and replacing a key file in place are all applied by the
  next reload. Every key file is re-read at each reload and compared by
  content, the same way rotated MQTT and webhook secrets are handled. The
  reload status reports what moved in `api_key_providers_added`,
  `api_key_providers_removed` and `api_key_providers_changed`.
  - The startup checks now also run at reload: a missing, world-readable or
    malformed key file, an account naming an unregistered user, or a
    permission clause that no account matches refuses the reload with the
    same message startup would give.
  - Removing the deployment's last API-key or OAuth provider still needs a
    restart.
- **Adding a tool provider now takes effect on reload.** A reload that adds a
  `wasm_consumer` whose class provides tools, or gives an existing instance
  that provided nothing a class that does, registers its tools: agents
  granted them in the same document and other components can call them
  straight away, and each added tool's `brenn:tools/<name>` channel is listed
  in `channels_added`. A deployment's first `api_key_provider` registers
  `api-request` the same way. Tools already registered keep their
  concurrency bounds, rate limits and in-flight calls across the reload.
  Still needing a restart: changing a providing instance's set of tools (as
  upgrading a component to a version with different tools does), removing a
  providing instance, removing the last provider, and adding the first
  asynchronous tool to a process that started with none.
- `api-request` accepts `PROPFIND` and `REPORT`, so a provider's CalDAV or
  WebDAV endpoint can be read. A grant clause that names no `method`, or
  `method = "*"`, now admits them. A caller with no acting user is still
  admitted `GET` and `HEAD` only. Note that a grant clause's `path` limits
  the request URL, not what a DAV request returns: a `PROPFIND` on a
  collection (a DAV folder, whose path ends in `/`) lists everything beneath
  it, and a `REPORT` can return data from elsewhere on the same account. See
  `docs/security-posture.md`; keep data that must stay apart behind separate
  credentials.
- `api-request` sends a path that ends in `/`, and the root path `/`, as
  written; both used to be refused. A grant clause's `path` template may end
  in `/` and then admits only such paths. No existing clause admits one:
  `path = "*"` and a clause with no `path` admit only paths that do not end in
  `/`.
- **A grant clause's `path` template that already ended in a `**` segment
  now admits every path beneath its prefix**; it used to admit only a path
  whose last segment was literally `**`. A template with a `**` segment
  anywhere else, which used to be a literal, is now refused at startup and at
  reload. `**` as a template's last segment is a new thing a grant can
  express: `path = "/files/**/"` admits every folder beneath `/files/`, at any
  depth, and `path = "/files/**"` every file there. Neither admits `/files/`
  itself, which takes its own clause. See `docs/config-dsl.md`.
- `api-request` accepts `MKCOL`, `COPY` and `MOVE`, so a provider's WebDAV
  endpoint can make folders and copy, move and rename files and folders. A
  grant clause that names no `method`, or `method = "*"`, now admits them; an
  operator who does not want that names the methods. A caller with no acting
  user is still admitted `GET` and `HEAD` only.
- `COPY` and `MOVE` take their destination as a `Destination` header holding
  an absolute path, written like `path`. Both the request path and the
  destination must be admitted by the caller's grant clauses for that method,
  and Brenn sends the destination as the absolute URI on the request's host;
  a destination on another host cannot be named. A `Destination` header on
  any other method, which used to be passed through, is now refused. A `COPY`
  or `MOVE` of a folder carries everything beneath it, and one onto an
  existing file or folder replaces it unless the request sends
  `Overwrite: F`. An `api_key_provider` whose `header` is `Destination` is
  now refused at boot and reload, since Brenn writes that header. See
  `docs/security-posture.md`.
- For WASM component authors: the provider kit needs no change to issue
  `MKCOL`, `COPY` or `MOVE`. On `COPY` and `MOVE` set
  `headers["Destination"]` to the destination's absolute path;
  `api-request` sends the absolute URI. `read_api_payload` reads their
  `201`, `204` and `207` answers as a `Payload`.
- For WASM component authors: the provider kit's new `read_api_payload`
  reads a successful (2xx) answer's body as bytes or text, whatever its content type and whichever
  field carried it, together with the response headers; `read_api_result` is
  unchanged and still requires JSON. The `testutil` fixtures gain `payload`.
- Provider components built with the provider kit can send a binary request
  body (`Body::Bytes`), and a binary (`body_base64`) response is read back
  as `Answer::Bytes` instead of failing.
- The bundled SQLite moves from 3.51.3 to 3.54.1 (`rusqlite` 0.41.1,
  `libsqlite3-sys` 0.39.1), which carries the fix for CVE-2026-119375
  (RUSTSEC-2026-0459): a database opened together with a crafted `-wal` file
  of a different page size could return uninitialized memory.

## [0.33.0] — 2026-11-08

- **An `api_key_provider` can hold per-user accounts.** Instead of one
  `key_file`, a block may hold `account <name> { user = "…"; key_file = "…";
  default = true; }` sub-blocks, each one registered user's key in its own
  `0600` file. `api-request` picks the acting user's key: `account` names one
  of that user's accounts, and an omitted `account` resolves to the user's
  default or only one. `user` and `account` clauses bound such calls as they
  do OAuth calls, and a clause naming such a provider with a `user` or
  `account` none of its accounts matches is refused at boot and on reload.
  Boot refuses an account whose user is not registered;
  `brenn config-check` notes each user it cannot check. The `key_file` form is
  unchanged. Adding or changing an account takes a restart.

## [0.32.0] — 2026-11-08

- **BREAKING: `google-workspace-tools` is split into five components, one
  per Google scope set; no compatibility shim.** The package
  `google-workspace-tools` and its class `GoogleWorkspaceTools` are gone.
  In their place: `google-gmail-read` (`GoogleGmailRead`: `GmailLabels`,
  `GmailSearch`, `GmailRead`, `GmailThread`), `google-gmail-compose`
  (`GoogleGmailCompose`: `GmailDraft`), `google-gmail-organize`
  (`GoogleGmailOrganize`: `GmailModifyLabels`), `google-calendar-read`
  (`GoogleCalendarRead`: `CalendarList`, `CalendarEvents`, `CalendarEvent`,
  `CalendarFreeBusy`) and `google-calendar-write` (`GoogleCalendarWrite`:
  `CalendarCreate`, `CalendarUpdate`, `CalendarDelete`). Each instance holds
  only the `api-request` clauses its own tools call, with its own rate-limit
  bucket and result inbox, so reading mail, drafting, changing labels,
  reading calendars and changing them are separate grants. The tools' names,
  descriptions, arguments and answers are unchanged.
  **Config change required:** replace `use @google-workspace-tools::*;`, the
  `brenn:tool-results/workspace` tuning block and the `workspace` instance
  with the five `use` lines, five tuning blocks and five instances of
  `docs/google-workspace.md` section 5, which is the paste. Each instance's
  `config` names `provider` and its one host key: `gmail_host` for the three
  Gmail components, `calendar_host` for the two Calendar ones. Leave out the
  instances whose tools you do not want, with their grants. The agent's
  `tool` grants are unchanged. The old store file
  (`/var/lib/brenn/google-workspace-tools.db`) is no longer opened and may be
  deleted. A request in flight across the upgrade is answered `internal` by
  the host at its 240-second deadline, as across any restart.

- **Tool providers can now take far more concurrent requests: a provider
  no longer has to make every outbound API call in one go.** Brenn's tool
  providers (WASM components such as `google-workspace-tools` and
  `google-maps-tools`, built on the shared `brenn-provider-kit` library)
  used to make every nested `api-request` call in the same activation that
  received the requests, which capped a providing class at a combined
  `max_concurrency` of 32. A kit-built provider now makes at most 32 calls
  per activation and queues the rest, issuing them as earlier calls answer;
  a request that waits in the queue for 60 seconds is answered `internal`
  rather than left to time out silently at the host's 240-second deadline.
  Callers see the same tools and the same answers.

  For operators:
  - A providing class may now sum `max_concurrency` up to 1000 (was 32).
  - The `tool-results` inbox must keep `retain_depth ≥ 2 × Σ max_concurrency`.
    The 64 the guides set covers Σ up to 32; a larger class needs it raised
    (at most 1000, so Σ up to 500). Sizing a full window against the
    guest's 16 MiB memory is yours to check — see `docs/config-dsl.md`.
  - Requests in flight across the upgrade are lost with the restart. A late
    result for one fails that activation with "in-flight row does not
    parse" until it is cleared, seven minutes after it was issued. A queued
    request whose stored shape changes across a future upgrade fails
    activations with "backlog row does not parse" for up to 60 seconds, then
    is answered `internal` and cleared.

- **BREAKING for component authors: `brenn-provider-kit` and the new
  `brenn-provider-kit-guest` are now build-time contracts for out-of-tree
  components, and this release cuts them.** Like `brenn-guest`, they change
  only at your pin bump; `docs/component-packages.md`, "A tool provider on
  the kit", explains how to depend on them. A provider now implements
  `Logic` and calls `brenn_provider_kit_guest::serve` from `receive`; the
  kit owns the activation loop and the queueing. Other cuts:
  `read_api_result`, `Unreadable::render` and `Unreadable::failure` take the
  API's display name instead of saying "Google"; `google_error_detail` and
  the `GOOGLE_*` bounds moved to `brenn_provider_kit::google`; the in-flight
  row gained `received_at_ms`.

- **`Weather` no longer looks up the user's location itself; the "here"
  default added in 0.31.0 is withdrawn, and so is 0.31.0's grant of
  user-acting tools to WASM components.** `Weather` takes a `place` or
  `lat`/`lng` again. For "what's the weather here?", the assistant first asks
  `DeviceLocation` for the device's position and passes it on. The Maps
  component therefore no longer copies users' device positions into its
  stored tool results.
  **Config change required:** remove `tool device-location {}` from the
  `google-maps-tools` instance if you added it; boot refuses it there (and on
  any other WASM component). Keep the grant on the *assistant* — it is what
  shows "Share location from this device" in the user menu.
  A `google-maps-tools` request in flight across the upgrade is lost with the
  restart; a late result for one fails that activation with "in-flight row
  does not parse" and is cleared seven minutes after it was issued.

- **Device locations are now kept in a per-user bus channel instead of the
  database.** Each report from a sharing device, with the browser's time
  zone, goes to the user's own durable bus channel,
  `brenn:device-location.<username>`, which Brenn creates for every user;
  "Stop sharing location from this device" adds a `stopped` message for the
  device and deletes nothing. The channel keeps the last 2000 reports by
  default (about a week of one device); size it with
  `channel at prefix "brenn:device-location." { … }`, or for one user with an
  exact address, and restart to change it (its send rate is fixed by Brenn
  and a `send_rate` there is refused). A component written for one user can
  bind that user's channel as an `in` port. `DeviceLocation` answers as
  before.
  Deployments that ran 0.31.0 with location sharing drop its
  `device_locations` table and `device_users.browser_tz` column on upgrade;
  each device must turn sharing on again.

- Bumped pulldown-cmark from 0.13.5 to 0.13.6, clearing RUSTSEC-2026-0433.

## [0.31.0] — 2026-11-01

- **"What's the weather?" now means "here".** The `Weather` tool, called
  without a place or coordinates, uses the user's most recent shared device
  location (from `DeviceLocation`) and says which device it used, how old the
  position is, and whether it is stale. If no device is sharing location, the
  call is refused with instructions for turning sharing on. To page through a
  "here" forecast, pass the coordinates the first answer reported.
  **Config change required:** add `tool device-location {}` to the
  `google-maps-tools` instance, next to its `api-request` grant. Without it,
  `Weather` with no location is refused, naming the missing grant; calls that
  give a location work as before. The app still needs its own
  `device-location` grant to show the sharing menu item.
  **Privacy note:** each "here" call stores the user's sharing devices'
  positions in that instance's durable `brenn:tool-results` inbox. They stay
  there until newer results push them out (64 results in the documented
  configuration), even if the user stops sharing.

- **WASM components that provide tools may now be granted tools that act as
  the user**, such as `device-location`. Such a call is allowed only when it
  serves a request made on behalf of a user. Any other call is refused with
  `denied` instead of crashing the server. The OAuth account tools are still
  refused to all components.

- **The assistant can now ask where the user is.** The PWA's user menu gains
  "Share location from this device": with the browser's permission, the
  device reports its position on connect, when the app comes to the
  foreground, and every five minutes while it is visible; "Stop sharing
  location from this device" deletes the stored position. The new
  `DeviceLocation` tool answers, for the acting user only, each sharing
  device's name, coordinates, accuracy, when Brenn received the fix and its
  age, the device's time zone and its local time; a fix older than 15 minutes
  is marked `stale`. Brenn also stores each device's browser time zone per
  user, so the tool knows it off-connection. Coordinates are never logged.
  **Config change required:** the assistant needs `tool device-location {}`;
  without it the menu item does not appear and the tool is not callable. The
  grant is refused on `multiuser` apps and apps with no `container`.

- **The Google Workspace assistant can now create, change and delete
  Calendar events.** The `google-workspace-tools` component gains three
  tools. `CalendarCreate` adds one event (summary, start and end as RFC 3339
  times or all-day dates, plus optional description, location, attendees,
  time zone and recurrence). `CalendarUpdate` changes only the fields it is
  given; `attendees` replaces the whole list. `CalendarDelete` removes one
  event, deletes a recurring series only when explicitly asked to, and
  refuses an event that is already cancelled. None of them emails attendees
  unless the assistant asks it to (`notify`). If Google's answer to a create
  is lost, the assistant is told the event may already exist and to check
  before trying again. **Config change required:** the instance's
  `api-request` grant on `www.googleapis.com` needs `POST
  /calendar/v3/calendars/*/events`, `PATCH /calendar/v3/calendars/*/events/*`
  and `DELETE /calendar/v3/calendars/*/events/*`; the Google provider needs a
  `calendar_events` scope set
  (`https://www.googleapis.com/auth/calendar.events`); the assistant needs
  `tool calendar-create {}`, `tool calendar-update {}` and
  `tool calendar-delete {}`; and each user reconnects their Google account
  with `calendar_events` before the tools can work.

- **The Google Workspace assistant tools can now write a Gmail draft.** The
  `google-workspace-tools` component gains `GmailDraft`: a new draft, with
  `to`, optional `cc`, `subject` and a plain-text `body`, saved in the user's
  Gmail Drafts for the user to review and send from Gmail. No tool sends
  mail: the component only ever creates drafts. Recipients are printable
  ASCII (a display name in another script is left out or written in ASCII);
  the subject and body may be any text, up to 500 characters and about
  20 000 bytes. If Google's answer to the write is lost, the assistant is
  told the draft may already exist and to look in Drafts before asking
  again. **Config change required:** the instance's `api-request` grant
  needs a `POST` clause for `/gmail/v1/users/me/drafts` on the Gmail host;
  the Google provider needs a `gmail_compose` scope set
  (`https://www.googleapis.com/auth/gmail.compose`); the assistant needs
  `tool gmail-draft {}`; and each user reconnects their Google account with
  `gmail_compose` before the tool can write.

- **A Gmail draft can now be a reply.** `GmailDraft` takes an optional
  `reply_to_id`, a message id from `GmailSearch` or `GmailRead`: the draft is
  then filed in that message's conversation with the headers a reply
  carries, and `subject` may be left out, in which case it is the
  original's with `Re: ` in front. If Gmail files the draft as a new
  conversation instead, the assistant is told so. Reading the original needs
  the user's Gmail read access as well as `gmail_compose`. No config changes
  beyond those `GmailDraft` already needs.

- **The Google Workspace assistant tools can now label and archive Gmail
  messages.** The `google-workspace-tools` component gains
  `GmailModifyLabels`: one change that adds and removes labels on up to
  1000 messages at once, all or none, using the label ids `GmailLabels`
  lists — removing `INBOX` archives, removing `UNREAD` marks read. The tool
  never moves mail to Trash or Spam: adding `TRASH` or `SPAM` is refused
  before anything is changed, while removing them is allowed. **Config
  change required:** the instance's `api-request` grant needs a `POST`
  clause for `/gmail/v1/users/me/messages/batchModify` on the Gmail host;
  the Google provider needs a `gmail_organize` scope set
  (`https://www.googleapis.com/auth/gmail.labels` and
  `https://www.googleapis.com/auth/gmail.modify`); the assistant needs
  `tool gmail-modify-labels {}`; and each user reconnects their Google
  account with `gmail_organize` before the tool can change labels.

- **The Google Workspace guide now tells operators to keep the assistant's
  own Gmail access read-only.** `docs/google-workspace.md` documents
  `GmailDraft` and `GmailModifyLabels` and drops its earlier example that
  gave the assistant raw `POST` grants for Gmail drafts and labels through
  `api-request`. A raw write grant on the Gmail host lets the assistant do
  anything the user's Google connection allows, including sending or
  trashing mail, which the tools themselves never do. If you followed that
  example, remove those two clauses from the assistant's `api-request` tool;
  the tools cover both writes. No other config changes.

- **The Google Workspace assistant tools can now tell when the user is
  busy.** The `google-workspace-tools` component gains `CalendarFreeBusy`:
  for a window between two RFC 3339 times and up to 50 calendars (`primary`
  unless named), each calendar's busy intervals as Google gives them, or
  Google's reasons it could not answer for that calendar, which are not
  reported as free time. A window too large to read or answer whole is
  refused, saying what to narrow. **Config change required:** the instance's
  `api-request` grant needs a `POST` clause for `/calendar/v3/freeBusy` on
  the Calendar host (the query changes nothing, and is admitted only for a
  call made for a user), and the assistant needs
  `tool calendar-free-busy {}`; `docs/google-workspace.md` shows both.

- **The Google Workspace assistant tools can now read one calendar event.**
  The `google-workspace-tools` component gains `CalendarEvent`: one event by
  id (on `primary` unless a calendar is named) with its title, description
  (as the organizer wrote it, cut at `max_chars`), start and end as Google
  gives them, location, status, organizer, the response of the calendar
  read (the user's own on `primary`), recurrence rules, the recurring event
  it belongs to and its video link; and its attendees with each one's
  response, counted. An event with more than 50 attendees answers the
  calendar's own entry alone and says the others were left out. An event
  too large to read whole is refused, saying why.
  **Config change required:** the instance's `api-request` grant needs a
  `GET` clause for `/calendar/v3/calendars/*/events/*` on the Calendar host,
  and the assistant needs `tool calendar-event {}`;
  `docs/google-workspace.md` shows both.

- **The Google Workspace assistant tools can now read a calendar's events.**
  The `google-workspace-tools` component gains `CalendarEvents`: the events
  on one calendar (`primary` unless named) between two RFC 3339 times,
  recurring events expanded, in start order, each with its id, title, start
  and end as Google gives them, location, status, organizer and the response
  of the calendar read (the user's own on `primary`); up to 50 to a page,
  with a text search. A
  window too large to read whole is refused, saying what to narrow.
  **Config change required:** the instance's `api-request` grant needs a
  `GET` clause for `/calendar/v3/calendars/*/events` on the Calendar host,
  and the assistant needs `tool calendar-events {}`;
  `docs/google-workspace.md` shows both.

- **The Google Workspace assistant tools can now list the user's
  calendars.** The `google-workspace-tools` component gains `CalendarList`:
  each calendar in the user's Google Calendar list with its id, name, whether
  it is the user's own, the user's access to it and its time zone, 100 to a
  page. **Config change required:** the instance's `config` map needs a new
  key, `calendar_host = "www.googleapis.com"` (without it every call fails,
  naming the key); its `api-request` grant needs a `GET` clause for
  `/calendar/v3/users/me/calendarList` on that host; the Google provider's
  `api_hosts` must include `https://www.googleapis.com`; and the assistant
  needs `tool calendar-list {}`. `docs/google-workspace.md` shows all four.
  Each user's Google account must also be connected with the
  `calendar_readonly` scope set: an account connected for Gmail alone is
  refused by Google on every Calendar tool until it is reconnected. When
  Google refuses a Calendar call for a missing permission, the assistant
  is now told to reconnect with the Calendar read scope rather than the Gmail
  one.

- **Gmail tools answer around a message they cannot read whole instead of
  failing.** A `GmailSearch` page whose listed message cannot be read — cut
  at the provider's read limit, or refused by the bus's `max_body_bytes` —
  now answers the page's other messages and its `next_page`, and names that
  message in `unavailable` with the reason; `unavailable` entries are now
  objects (`id`, `reason`), a message deleted since the listing included. A
  `GmailThread` too large to read whole is read again without `to` and `cc`,
  and `recipients_omitted` says why. A `GmailRead` refused by the bus cap
  now falls back to the message's headers and attachments, as one over the
  read limit already did. The `internal` error for a read that stays too
  large now reads "Google's answer (…) exceeds api-request's read limit for
  this provider (max_response_bytes)", or names the host's refusal. A
  passing fault on one of these reads, such as a dropped connection, is
  answered the same way and its reason names neither limit: asking again
  usually reads the message whole.

- **Google Workspace tool arguments are now limited in characters, not
  bytes.** The length caps on the `google-workspace-tools` arguments (`q`,
  `page`, `account`, `from`/`to`, `calendar` and `calendars`) now count
  characters, as the tools' argument schemas do, so a non-ASCII value of up
  to the stated length is accepted and a refusal reads "… longer than N
  characters" or "a calendar id of 1 to 320 characters". A whole-number
  argument written as a float (`5.0`) is now accepted. A Google error whose
  status is not a short upper-case code is reported as "not in the standard
  error shape" rather than quoted. No config changes.

- `CalendarFreeBusy` now refuses a `calendars` list whose ids together
  exceed 48 000 bytes of UTF-8, before any call to Google, with an error
  naming `calendars` and asking for fewer. Such a list could exceed the
  host's size limit for the call to `api-request` and was answered as an
  internal error. No config changes.

- **Google Maps tool arguments are now limited in characters, and its
  errors read like the Workspace tools'.** The length caps on the
  `google-maps-tools` arguments (`address`, `place`, `page`, `origin`,
  `destination`, `depart_at`, `arrive_by`, `query`, `near` and the place
  `id`) now count characters, so a non-ASCII value of up to the stated
  length is accepted and a refusal reads "… longer than N characters". A
  Google answer too large to read now reads "Google's answer (…) exceeds
  api-request's read limit for this provider (max_response_bytes)" and, where
  an argument would shrink it, says what to ask for instead (fewer places,
  no `reviews`, fewer `days`, a shorter route); one the host did not deliver
  reads "the host did not deliver api-request's answer: …". A refused write
  reads "api-request refused the nested POST …" without the old closing
  sentence, and a provider whose credential is missing or expired is
  answered with `api-request`'s own message. Review text and weather alert
  descriptions are now cut at 600 and 1000 bytes rather than characters, so
  non-Latin text is cut sooner. As for Workspace, a whole-number argument
  written as a float is accepted and a Google error with an unusual status
  code is reported as "not in the standard error shape". No config changes.

- **The Google tools' argument schemas now state every limit their checks
  enforce.** The schemas the assistant reads for the `google-maps-tools`
  arguments now give each text argument's length (1024 characters, 4096 for
  `page`) and the place `id`'s allowed characters; the free-text arguments of
  both Google components now say a value must not be blank; and the
  Workspace `calendar` and `calendars` arguments state the calendar-id rule
  as a pattern instead of a bare length. A test now fails the build when a
  tool's schema and its argument check disagree. No config changes.

- The Google Workspace operator guide (`docs/google-workspace.md`) now
  covers the Calendar tools: connecting users with `calendar_readonly`, what
  each answer holds, the assistant's instructions and Calendar setup and
  troubleshooting steps. The raw `api-request` route is now documented for
  writes, Gmail endpoints the tools do not cover, Calendar's colors and
  settings, and automations that act without a user;
  `docs/providers/google.md` drops the Calendar examples the tools replace.

- Internal: a new in-tree crate, `brenn-provider-kit`, holds the core the
  two Google tool components share (the record a request keeps while it
  waits on `api-request`, argument and setting readers, and the reading of
  Google's answers), with its own tests. Both the Google Workspace and the
  Google Maps tool components now build on it.

- Internal: `brenn-provider-kit` now accepts a successful Google answer with
  an empty body (as some write endpoints give) instead of failing it as "not
  JSON". The Google tools have no such call yet and still fail one, now
  reading "Google answered HTTP … with no body". No config changes.

## [0.30.1] — 2026-10-31

- **Fix: an MQTT client with more than 64 subscriptions no longer hangs
  when it connects or reconnects.** After connecting to the broker, Brenn
  subscribes again to every topic filter the client needs. With more
  filters than the client's outgoing request queue holds (64), that step
  waited for queue space that could only free up once it had finished, so
  the connection stalled silently: nothing more was sent or received, the
  broker dropped the session for missing keepalives, and Brenn never
  reconnected. The log stopped after "connected to broker". Subscriptions
  added while the client is running could hang behind it too. Now a
  subscribe that finds the queue full is handed to the connection task to
  send when the queue has room, and re-subscribing on connect never blocks.
  If a topic filter cannot be subscribed during that step, the rest still
  go through, and the summary log line becomes a warning giving the number
  that failed.

- Known issue, older than this release: if an MQTT client is stopped or
  reloaded just after the broker has sent it data, the broker may treat the
  disconnect as abnormal and publish the client's configured Last Will.
  Tracked as `mqtt-disconnect-lost-to-reset` in `TODO.md`.

## [0.30.0] — 2026-10-31

- **New `google-maps-tools` component: Google Maps Platform as assistant
  tools.** Instead of composing raw `api-request` calls, the assistant gets
  native tools that call Google through an `api_key_provider` (the key stays
  with Brenn, never with the component) and return compact answers. This
  first release has `Geocode`: an address to coordinates, or coordinates to
  addresses. The component's class spec, `config/specs/google-maps-tools.brenn`,
  documents its configuration keys and the result-inbox sizing it needs. For
  WASM component authors, the guest SDK (`brenn-guest`) now implements
  `Display` for its `Error` and adds `brenn_guest::provides::drop_not_pending`,
  which lets a provider drop a completion for a request that has expired
  instead of failing; both are additive.

- **`google-maps-tools` adds a `Weather` tool**: current conditions, an
  hourly forecast, up to ten days of day and night forecasts, and public
  weather alerts, for a named place or for coordinates. An alert's source
  is shown with it when Google gives one. An empty alerts list is not proof
  that no alert is in force: Google gives the same answer when a provider's
  alert lacked a required field.

- **`google-maps-tools` adds a `Directions` tool**: a route between two
  places by car (with traffic), on foot, by bicycle or by public transit,
  optionally for a given departure time or, for transit, arrival time. It
  gives the distance, the travel time and the turn-by-turn steps; a transit
  step names its line and stops and gives each departure and arrival time
  with its own time zone. When Google could not take traffic into account,
  the answer says so. Directions must be asked for on a user's behalf (from
  a conversation, for instance); a component calling on its own account is
  refused.

- **`google-maps-tools` adds a `PlacesSearch` tool**: find businesses,
  landmarks or addresses by a text search such as "coffee", optionally
  near a place or coordinates and optionally only those open now. Each
  result gives the place's name, address, location, rating, price level
  and whether it is open, plus an id that Directions accepts as a
  destination. Like Directions, it must be asked for on a user's behalf.

- **`google-maps-tools` adds `PlaceDetails` and `SearchAlongRoute`.**
  PlaceDetails gives one place's details from its id: address, rating,
  opening hours, phone, website and a Google Maps link, plus Google's
  summary and a few reviews when asked for (they are billed at a higher
  rate). SearchAlongRoute finds places along a route, such as "gas station"
  between two cities, and gives the route's distance and travel time with
  the results. A route too long to search along is refused. Like Directions,
  SearchAlongRoute must be asked for on a user's behalf.

- **The Google Maps guide, `docs/google-maps-platform.md`, now sets up the
  `google-maps-tools` component**: the key, the configuration, the
  component's instance and agent grants, what to tell the assistant, and how
  to check it works. Its configuration needs a
  `messaging { max_body_bytes = 393216; }` block (restart required), because
  Google's answers reach the component over the message bus and the 64 KiB
  default is too small for a long transit route or a day's forecast. The
  guide also explains the alternative, which is a lower
  `max_response_bytes` on the key provider. Corrected: a component serving a
  user's tool call may use any method its grant allows, not just
  `GET`/`HEAD`, and the `api_hosts` examples no longer end with a trailing
  comma. For Google endpoints the component does not cover,
  `docs/providers/google-maps.md` gives ready-made `api-request` examples.

- **New `google-workspace-tools` component: a user's Gmail as assistant
  tools.** Instead of composing raw `api-request` calls, the assistant gets
  native tools that call Gmail with the calling user's connected Google
  account (Brenn attaches the token; the component never sees it) and
  return compact answers. This first release has `GmailLabels`: the user's
  labels, or one label with its message and thread counts. A user who has
  not connected an account gets the same `credential_invalid` answer
  `api-request` gives, so the assistant offers to connect; a user with
  several accounts names one as `account`. The component's class spec,
  `config/specs/google-workspace-tools.brenn`, documents its configuration
  keys and the result-inbox sizing it needs; `docs/google-workspace.md`
  gives its instance.

- **`google-workspace-tools` adds a `GmailSearch` tool**: a Gmail search
  (`q`, in Gmail's search-box syntax) answered with one compact line per
  message — sender, subject, date, Gmail's preview and labels by name — up
  to 25 per page, with a token for the next page. Messages deleted between
  the listing and their reading are named as unavailable rather than
  failing the search. A sender, subject or date over 1,000 bytes (UTF-8)
  is cut, and the line says which were cut; control characters in them and
  in the preview are shown as spaces. Bodies are not included. The
  guide's instance block in
  `docs/google-workspace.md` gains the two `messages` clauses the tool needs
  and the assistant's grant lines.

- **`google-workspace-tools` adds a `GmailRead` tool**: one message by id,
  answered with its headers, labels by name, its body as plain text and a
  list of its attachments (name, type, size; their contents are not
  fetched). Of a plain and an HTML version the plain one is read; UTF-8,
  ISO-8859-1 and Windows-1252 text is decoded, and anything that could not
  be decoded exactly is noted in the answer rather than shown garbled. The
  text is cut at `max_chars` (12,000 characters by default), or shorter if
  the whole answer would pass the component's result size limit, and the
  answer says so. A message larger than the provider's read limit is
  answered with its attachments and Gmail's preview instead of its body.
  Recipient headers are cut over 4,000 bytes and the others over 1,000,
  and the answer names those cut. Generated component scaffolding no longer
  fails clippy when several of a spec's tools share a name prefix.

- **`GmailRead` reads HTML-only mail as text.** Tags, styles and scripts
  are dropped. Links keep their text but not their address. Paragraphs,
  list items and table cells stay apart. Text that the HTML hides is still
  included. When a message has both versions the plain one is still read,
  unless it is blank.

- **`google-workspace-tools` adds a `GmailThread` tool**: one email
  conversation's messages in order, oldest first, without their bodies —
  each with its sender, recipients, date, Gmail's preview and labels by
  name, and its subject only where it changes. The newest 30 are given by
  default (up to 100), and the answer says how many were left out,
  including any dropped to keep it within the component's result size
  limit. A thread whose headers together are too large to read is refused,
  whatever the number asked for. The guide's instance block in
  `docs/google-workspace.md` gains the `threads` clause the tool needs and
  the assistant's grant line. All of the component's Gmail reads now ask
  Google for compact answers, which leaves more room under the size limits.

- **The Google accounts guide, `docs/google-workspace.md`, now sets up the
  `google-workspace-tools` component**: its configuration, instance and
  agent grants, what to tell the assistant, and how to check it works. The
  component needs a `messaging { max_body_bytes = 393216; }` block (restart
  required; the same value as the Maps guide's), or a lower
  `max_response_bytes` on the OAuth provider at the cost of large messages'
  text. A message whose sender filled its headers with quotes or
  backslashes can still be refused by these tools whatever the setting. For
  Gmail and Calendar endpoints the component does not cover,
  `docs/providers/google.md` gives ready-made `api-request` examples.

- **`api-request` can call services that use one operator-held API key**,
  such as Google Maps Platform. Declare the service in a new
  `api_key_provider` config section and name it as the `provider`. The key
  is held in a host file (mode `0600`) that Brenn reads at boot; Brenn
  attaches it as the request header the block names, sends it only to the
  block's configured hosts, follows no redirects, never passes it into the
  container and never writes it to a diagnostic or security log line. An
  answer that echoes it is recorded in the conversation's transcript like any
  other tool result. The provider's answer comes back unchanged, whatever its
  status. A caller cannot set the key's header
  or pass `account`, a WASM component may only read (`GET`/`HEAD`), and a
  grant clause that names a key provider together with `user` or `account`
  is refused at boot. A deployment may declare key providers alone, with no
  `oauth_provider` or `vault_dir`. Changing a block or its key file needs a
  restart. See `docs/google-maps-platform.md` for a Google setup and
  `docs/config-dsl.md`, "API-key providers", for the reference.

- **A tool result too large for the message bus no longer crashes Brenn.**
  Answers to tool calls made by WASM components travel over the message bus.
  Every message there is limited by `[messaging] max_body_bytes`, 64 KiB by
  default. A bigger answer, such as a large `api-request` response from an
  API-key provider, used to crash Brenn's tool executor, and the caller waited
  minutes for a timeout. The caller now gets an `internal` error at once
  that names the setting, and operators get a Warning alert once per process.
  To carry larger answers, raise `max_body_bytes` in a `messaging` block
  (restart required) or lower the provider's `max_response_bytes`. The
  `messaging` section is now documented in `docs/config-dsl.md`, "Messaging
  defaults".

- **The MQTT integration tests no longer need mosquitto installed.** The
  test broker (mosquitto 2.0.22) is now built from source by Bazel and
  handed to the tests, so they run the same on every machine and in CI. This
  fixes CI failures where Ubuntu's packaged mosquitto, confined by AppArmor,
  could not read the tests' config files. Developers can uninstall a system
  mosquitto kept only for the tests; the `BRENN_MOSQUITTO_BIN` override is no
  longer forwarded from your shell into Bazel tests.

- **WASM components can now serve tools to the assistant and to other
  components.** A component instance granted `provides` that declares a tool
  with a `provide` statement is now connected to Brenn when it loads, at
  boot and on reload: Brenn hands it each call to that tool and returns its
  answer to the caller. Tool calls the component makes while serving a call
  act for the user who made that call and are allowed only by the
  component's own tool grants. A configuration that declares a providing
  instance now boots.

- Component-provided tools are now documented for operators. The security
  posture (`docs/security-posture.md`) has a new section on the `provides`
  grant, covering who admits a call, which user a component acts for, the
  four-minute limit and the known limits. `docs/config-dsl.md` spells out
  what this means for configuration. A grant of a provided tool is refused
  on `multiuser` and container-less agents. When a component serving a
  user's call calls `api-request`, it acts as that user and may write
  within its grant. A component calling on its own account still only
  reads.

- **WASM components can import a new `provides` interface** (granted by
  the new `provides` grant word), the first step toward components that
  serve tools to the assistant and to other components. It lets a
  component answer a pending tool request and make tool calls on behalf of
  the user it is serving; the guest SDK gains a matching `provides` module.
  Nothing changes for existing deployments. Also fixed: a top-level WASM
  consumer granted `calls` no longer crashes Brenn at boot.

- **Component classes can declare the tools they provide.** A new `provide`
  statement in a class spec names a tool, the name the assistant sees it by
  (`mcp_name`), a description, its input schema (JSON text) and how many
  requests it may serve at once (`max_concurrency`, at most 32 summed over
  the class). It goes with the `provides` grant word and an
  `in tool-requests` port. `config-check` refuses a malformed declaration,
  two instances providing the same tool, and an instance granted a tool it
  provides itself; generated component scaffolding gains a `Provided` enum.
  Adding, removing or changing a declaration needs a restart. Channel names
  under `tool-requests` are now reserved, like `tools` and `tool-results`.

- A provided tool's input schema is now checked against the shape Brenn
  validates tool arguments with: a JSON object schema whose `properties` are
  objects and whose `required` names only listed arguments. Its `mcp_name`
  may not take the name of one of Brenn's built-in agent tools.
  `config-check` reports both. The `provide` statement is documented in
  `docs/config-dsl.md`.

- **A component's tool-result inbox no longer crashes Brenn under a burst
  of answers.** Each WASM component gets the answers to its async tool calls
  on its own `brenn:tool-results/<slug>` channel. That channel used to take
  the server-wide `[messaging].default_send_rate`. A component with several
  tools could use up that rate, and Brenn then stopped. Brenn now sets this
  channel's send rate itself, high enough that no real load reaches it. A
  `channel` tuning block on `brenn:tool-results/…` that sets `send_rate` is
  now refused at boot and by `config-check`: remove that line. Depths and
  `noise` can still be tuned. Groundwork for component-provided tools: Brenn
  now registers each declared provided tool and creates the providing
  instance's request inbox, `brenn:tool-requests/<slug>`, which accepts no
  tuning blocks.

- Groundwork for component-provided tools: Brenn can now hand a call to a
  provided tool to the component that serves it and wait up to four minutes
  for its answer, alerting once if a provider runs out the clock. Tool calls
  a component makes while serving such a call act for that call's user and
  end no later than it does.

- `google-maps-tools` is now tested end to end: the compiled component
  answers Geocode and Weather calls through a key provider against a
  stand-in for Google, including Google's errors, oversized answers and
  requests its grant does not cover, and the shipped class spec with an
  operator's key provider and instance block is booted into an agent's
  tool list.

- `google-workspace-tools` is now tested end to end: the compiled component
  answers `GmailLabels` and `GmailSearch` calls through an OAuth provider
  against a stand-in for Gmail, sending the right user's token, including a
  missing or ambiguous connected account, a caller with no user, Google's
  refusals and requests its grant does not cover; and the shipped class spec
  with an operator's `oauth_provider` block and instance block is booted into
  an agent's tool list.

- Bumped wasmtime from 48.0.7 to 48.0.8, clearing RUSTSEC-2026-0422,
  RUSTSEC-2026-0423 and RUSTSEC-2026-0424.

- **Building Brenn no longer loads the host's `libxml2`.** The hermetic C
  toolchain is now LLVM 23.1.4 (toolchains_llvm 1.12.0); LLVM 22.1.8's linker
  needed `libxml2.so.2`, which hosts with libxml2 2.14 or newer (Ubuntu 26.04,
  and the `ubuntu-latest` CI image with it) no longer ship, so every link
  failed there. `make check` now works on those hosts, and CI drops its
  "Install lld" step, which never chose the linker. The first build after
  updating rebuilds the host lane from scratch.

## [0.29.0] — 2026-10-29

- **Users can connect their own Google accounts, and the assistant can act
  through them.** Configure it with a new `oauth_provider` section and a
  `server.vault_dir` key; `docs/google-workspace.md` walks through the Google
  console setup, the configuration, the per-app grants and a first-connect
  check. Four new tools: `oauth-connect` (the assistant gives the user a link
  to sign in with Google), `oauth-accounts`, `oauth-disconnect`, and
  `api-request`, which calls the provider's API with the conversation
  owner's credential attached by Brenn. Tokens are kept in a host-side vault
  directory that no agent can reach and never enter a container. Grants are
  refused on `multiuser` apps and on apps without a `container`; a WASM
  component may hold only read-only `api-request`. The `path` access rule of
  `api-request` is a segment template (see `docs/config-dsl.md`). Nothing
  changes for a deployment that declares no provider.

- **Four RustSec advisories cleared; push encryption moved in tree.**
  tokio, ctutils and pulldown-cmark are bumped (RUSTSEC-2026-0385, -0386,
  -0414). RUSTSEC-2026-0387 (`crypto-bigint` 0.5) is cleared by moving to
  `p256` 0.14: the `web-push-native` crate, which pinned the old curve line,
  is removed, and PWA push payloads are now encrypted by Brenn itself (RFC
  8291 over `ece-native`), checked against the RFC's own test vector. The
  wire format is unchanged and no configuration or VAPID key changes; watch
  the first push after deploy as usual.

- **Brenn's own checks now use the published `brenn-scrub` from crates.io,
  not the copy in this repo.** `brenn-scrub` is the secret-and-hygiene
  scanner behind the git hooks and `make check`. `make check` and
  `make scrub-tree` run it built by Bazel at the release
  `tools/scrub/Cargo.toml` pins (currently 0.2.0), the same bytes on every
  machine.
  The git and Claude Code hooks run the `brenn-scrub` on your `PATH`, which
  `make setup-hooks` installs from crates.io and `make check` requires to be
  at or above that release. Action needed on each developer machine: run
  `make setup-hooks` once; it also replaces an older install of the in-tree
  crate. Until you do, `make check` (and therefore every commit) fails and
  tells you why. The in-tree `scrub/` crate stays for other repositories that
  have not switched yet; nothing in brenn's own checks runs it. Known gap:
  release 0.2.0 skips the design-reference rule on whole-tree scans, so
  `make scrub-tree` and the pre-push scan no longer report it locally; CI
  still enforces it (tracked as `scrub-diff-only-tag-upstream` in `TODO.md`).

- Logging in from a link to `/app/…`, `/r/…` or `/oauth/…` now returns you
  to that link, even after a mistyped password. Login and registration forms
  posted from another site are refused (403).

## [0.28.0] — 2026-10-22

- **Chat can now hand the agent a secret, and paste text as a file.** The
  attachment menu gains **Snippet**: an inline composer where you type or
  paste text, name it (default `snippet.txt`) and attach it as a file instead
  of putting it in the message. Pasting a large block (2000+ characters or
  20+ lines) into the message box offers the same composer, with a
  **Paste inline** button to keep the old behaviour. Both the composer and
  file attachments have a 🔒 **Secret** checkbox. A secret attachment is
  written only to a private per-app inbox, `<state_dir>/secrets/` (mode
  0700, each file 0600, outside the working directory and every repo
  mount); the database, logs, message bus and the chat history keep only its
  name and size, it can never be downloaded back, and the agent is told its
  path and asked to move it where needed without reading it. Unclaimed
  files are deleted after 24 hours. This protects the transport, not the
  secret from the model: the agent can still read the file. A known gap,
  tracked as `security-host-paths-follow-agent-symlinks` in `TODO.md`: an
  agent that replaces part of its container home with a symlink can
  redirect where Brenn writes the file. `docs/security-posture.md`
  describes the full policy.
- **Brenn's secret-and-hygiene scanner (`scrub`) now enforces the
  design-reference rule on whole-tree scans, not just on diffs.** The rule
  that rejects comments citing private design documents (`design §2.1`,
  `docs/designs/...`) used to skip full-tree scans because the tree carried
  a backlog of them; that backlog is now cleared, so CI's tree scan and
  `scrub` tree mode report it like any other rule. Other repositories using
  `scrub` that still carry such a backlog can opt out per rule by adding
  `tags = ["diff-only"]` to it in their `.gitleaks.toml` (the repo template
  explains how); diffs are still checked either way.
- **Configs that give a surface-placed instance more authority than its
  surface now fail to load.** In a `.brenn` file, an instance placed on a
  surface (`new w: Widget { ... }`) may state its own `acl`. That statement
  must now lie within the surface's own `acl` on the `brenn:` and
  `ephemeral:` channels; a wider one is refused at load (and by config
  checking) with an error naming the extra entry and the instance, e.g.
  `panel.w`. Nothing was exploitable before — the surface's own lists always
  governed at runtime — but the config could claim authority it did not
  have. Fix any refused config by narrowing the instance's `acl` or widening
  the surface's.
- **`brennfmt` output changed: reformat your configs before deploying.**
  Brenn's config formatter, `brennfmt`, now uses a newer fltk (the parser and
  formatter library it is built on). The formatter no longer invents blank lines it
  previously added inside sections, keeps comments in the right place, and
  never rewrites a raw-string value. Because `brennfmt --check` is
  byte-exact, configs formatted by the old version fail the check until
  re-run through `brennfmt`; the in-tree configs are already updated.
  One known defect stays open (`dsl-fmt-orphan-terminator` in `TODO.md`):
  a stray space can still precede a section keyword that starts a document.
- A surface page that receives a component-bindings document it cannot use
  (the list of components the page hosts and what each may do) now reports
  the error and reloads, up to the usual reload cap, instead of failing.
  The usual cause is a browser holding assets older than the server. Past
  the cap the page stays connected, refuses the document again on each
  reconnect, and configures itself once a readable document arrives.
- The page-component test harness (`surface/page-harness`, which component
  authors run before deploying) now judges port directions by the same rule
  as the production hosts, so it refuses exactly what a deploy would. It
  now also refuses cancelling or editing a deferred message on a port the
  component never declared for output. Its documentation separates checks
  that mirror the host from the harness's own script checks, and notes that
  `calls` ports are not hosted.
- Config errors about a wrongly-typed list element (for example a quoted
  `"publish"` where a bare word belongs in `grants`) now point at the
  offending element instead of the list's opening `[`, and no longer carry an
  `element N:` prefix. This matters most for lists that span several lines.
- Internal cleanup of how publish refusals are classified for components
  (the shared publish-budget code both the server and the browser page use).
  Six "can't happen" panic paths are gone because the types no longer allow
  those cases. Components see the same refusals and messages as before.
- Bumped wasmtime from 48.0.6 to 48.0.7, clearing RUSTSEC-2026-0378 and
  RUSTSEC-2026-0379; rustls-webpki from 0.103.15 to 0.103.16, clearing
  RUSTSEC-2026-0382; and webpki-root-certs from 1.0.7 to 1.0.10, clearing
  RUSTSEC-2026-0383.

## [0.27.1] — 2026-10-20

- **Containerized agents on SELinux hosts can reach `brenn-tool`'s socket.**
  On a host that confines containers (Fedora/RHEL with the container policy),
  every `brenn-tool` call failed with `Permission denied`: the kernel refused
  the container's connect to a socket labelled with the server's own domain.
  A `container` definition's new `tool_socket_selinux_context` key labels the
  conversation's tool socket for the container instead; on Fedora/RHEL set it
  to `system_u:system_r:container_t:s0`. Unset (the default), nothing changes.
  Boot stops naming the key if the host refuses the context. Brenn now logs
  `tool socket bound` with the socket path and context for each conversation.
  `docs/agent-tool-cli.md` "Verifying a deployment" shows how to recognise the
  denial and choose the value.

## [0.27.0] — 2026-10-19

- **Agents can run their Brenn tools as shell commands.** A new command,
  `brenn-tool`, lets an agent call the Brenn tools it already has from Bash,
  a script, a loop or a pipeline: `brenn-tool list`, `describe <Tool>`, and
  `call <Tool>` with the tool's input as JSON on the command line, on stdin,
  or assembled from files (`--str-file` puts a large body in without
  escaping it). The tool's result is printed as JSON; failures exit
  non-zero with a JSON error. The command is only a proxy: it talks to a
  per-conversation socket, and Brenn runs the call with exactly the checks a
  tool call gets (channel permissions, send rate and budget, schema
  validation). No credential is involved. Tools whose effect is on the chat
  itself (showing a file, compaction, choice cards) and tools that need
  approval are refused as commands. See `docs/agent-tool-cli.md`.
- **Operators must update deployment for `brenn-tool`.**
  - The launcher must now set `XDG_RUNTIME_DIR` to a directory owned by the
    server's user with mode 0700, for every configuration. Boot stops if it
    is missing or wrong. A systemd user service already has it.
  - The binary is mounted into containerized agents from the new
    `claude_defaults.tool_cli_path` (default `/opt/brenn/brenn-tool`). Boot
    checks it when any agent is containerized. Bare agents must find
    `brenn-tool` on Brenn's own `PATH`.
  - An agent `env` may not set `BRENN_TOOL_SOCKET`, and an agent slug long
    enough to overflow the socket path limit is refused at load.
  See "Spawn artifacts and the runtime directory" in `docs/config-dsl.md`.
- Tool-argument validation moved from the noop MCP stub into the server, so
  tool calls and commands are checked by the same code.
- **New repositories are now cloned on the host, not inside the agent's
  container.** When a configured git repo has not been cloned yet, Brenn
  clones it at startup using the host's git and SSH setup, the same way
  pulls and pushes already work. Previously a repo mounted read-write was
  cloned inside the agent's container, which no longer holds SSH keys, so
  adding any new writable repo failed. Containers never need git
  credentials.
- **A failed clone at startup is reported clearly.** Brenn now waits for
  every clone to finish, logs each failure with the repo name and
  (sanitized) git error, empties the failed target directory so the next
  start retries cleanly, then refuses to boot with one message naming every
  failed repo. Startup also refuses, before cloning, if a target directory
  is non-empty but is not a git checkout.

## [0.26.1] — 2026-10-19

- **Components can read their own instance name** through the new WASM
  import `dom.instance()` (`brenn_guest::dom::instance()` in the guest SDK).
  An instance's name is the one the operator gave it in the layout. Chrome
  reports the current overlay holder by that name, so a component that asks
  for the full-screen overlay can now tell whether it was granted the
  overlay, denied, or preempted by a takeover such as an alarm. Existing
  components are unchanged.
- **A gesture on a container names the child it came through.** When one
  `dom.listen` listener sits on a container, the gesture now also carries
  `child`: the container's own child that the hit landed inside
  (`Gesture::child` in the guest SDK). A component that builds subtrees
  under one listener can tell which subtree was hit without keeping a handle
  for every node it created. The field is optional, so existing components
  run unchanged; Rust code that builds a `Gesture` (or the contract crate's
  `GestureBody`) as a struct literal, typically a test, adds `child: None`.
  The echo-stub test component's scrollback reports which entry was
  clicked.

## [0.26.0] — 2026-10-18

- **Components can animate.** Two new imports on the WASM `dom` interface:
  `animate(node, keyframes, options)` runs a browser Web Animations
  animation described entirely as JSON data (no stylesheet needed), and
  `prefers-reduced-motion()` reports the user's reduced-motion setting.
  Loops work: `"iterations": "infinite"` is read as endless. A value the
  browser rejects comes back as an error rather than a crash, so a component
  animating from bus messages survives bad input. Exposed in the guest SDK as
  `brenn_guest::dom`; existing components keep working unchanged.
- **Surface takeovers gain an overlay mode.** A request on
  `local:brenn/takeover` (the channel chrome uses to grant the single
  full-screen slot) may now carry `mode: "overlay"`, which draws full-screen
  *over* the layout instead of replacing it, letting taps through except
  where its content opts in. `mode` defaults to `"takeover"`, so existing
  requests are unchanged, and a takeover always preempts an overlay — a
  meeting alarm can never be blocked. See chrome's help page.
- **Gestures carry where and what was pressed.** A `dom.listen` gesture now
  includes, when the event has them, pointer position and size relative to
  the element, the key pressed, and modifier keys. The fields are optional,
  so components built against an older guest SDK keep working. Each event is
  still one activation, so listening on `pointermove` costs one per move.
- **Out-of-tree component kinds can ship their own help and schema** pages
  in their bundle, instead of getting only a generated stub.
- **Motion colour tokens in the skins:** `--motion-1` … `--motion-5` and
  `--motion-blend`, defined for bench dark/light and foundry.
- `tabindex` (`0` or `-1` only) is now allowed on `dom`-created elements, so
  a component can take keyboard focus and receive key events.
- Fixed: the meeting takeover card kept a panel shadow the skins meant to
  drop.
- The test page harness can simulate animation refusal, reduced motion and
  detailed gestures; the echo-stub test component gains an `animate` button.

## [0.25.0] — 2026-10-14

- **Per-agent environment variables.** A new `env` attribute on agents sets
  environment variables on the Claude Code subprocess, enabling local LLM
  backends (e.g. llama-server with an Anthropic-compatible API) on a per-agent
  basis without PATH wrappers or global environment changes. Combined with
  existing `cc_extra_args` flags (`--restricted`, `--tools`, `--system-prompt`),
  this is enough to lock an agent down to read-only file access within its
  working directory and a controlled tool set — suitable for untrusted or
  demo scenarios. The attribute refuses keys that would conflict with
  integration-emitted variables, outrank a configured Claude profile's
  credentials, or place secrets in plaintext config.
- **Start an agent on a fresh conversation from the message bus.** A
  singleton agent (one that has a single, always-on conversation) owned by
  one user can name a `conversation_epoch` channel. Whenever a new value is
  published there, the agent moves to a brand-new conversation instead of
  compacting the old one. The old conversation stays in the database and in
  the conversation list for reference; its Claude Code session shuts down at
  its next idle moment, never mid-turn. The new conversation picks up where
  the old one left off on the bus, so no message is replayed or lost, and
  open browser tabs switch over on their own. Adding or changing the setting
  needs a restart. See "Conversation epoch" in `docs/config-dsl.md`.
- **New `conversation-recycler` component** decides when to trigger that
  reset. It counts interactions on a channel you point it at and publishes a
  new epoch after a quiet spell (`idle_secs`, default 180), or once
  `max_interactions` (default 50) have piled up and a shorter `settle_secs`
  gap (default 10) passes. Useful for kiosk and demo agents that no one is
  watching.
- **BREAKING (config-carrying mounts): channels declared by a mounted
  fragment can no longer set the host's resource knobs.** A document mounted
  with `mount X under <principal>` (a "ceiling" that limits which addresses
  it may use) may still declare channels inside that reach, with any finite
  depth, but `config-check` now refuses three settings on them: an
  `unbounded` depth (which keeps the channel's history from ever being
  cleaned up), `send_rate` (which could raise the publish rate above the
  host default), and `sink` (which writes evicted messages into the
  operator's archive file). Those stay at the host's defaults; only the
  operator's own documents may set them. No checked-in deployment uses
  `mount … under` today. The security-posture document's "Exhausting the
  host's storage" entry (B10) is rewritten to say what this does and does
  not bound — total storage across a fragment's channels is still not
  accounted — and drops its claims of a filesystem quota and alert channel
  that never existed.
- Fixed: when two config files each define an `mcp_server` with the same
  name, an agent now gets the one its own file refers to. Previously it could
  silently get the other file's definition — and so launch the wrong MCP
  server command with the wrong environment.
- **Frontmatter is parsed with a new YAML library.** The YAML header at the
  top of a markdown file ("frontmatter") is now read by `saphyr` instead of
  the deprecated `serde_yaml`, whose underlying parser (`unsafe-libyaml`) is
  unmaintained (RUSTSEC-2024-0449); graf is pinned to a revision that made
  the same move. Visible differences: numbers display exactly as written
  (`012`, `0x1F`, `+5`); frontmatter that uses YAML anchors or aliases, nests
  deeper than 128 levels, holds more than 100 000 nodes, or carries more
  than 4 MiB of text now renders as an error block instead of the parsed
  header; and parse-error messages are worded differently.
- Bumped rustls to 0.23.46, clearing RUSTSEC-2026-0285 and
  RUSTSEC-2026-0346 through RUSTSEC-2026-0349.
- Bumped wasmtime from 47.0.4 to 48.0.6, clearing RUSTSEC-2026-0315,
  RUSTSEC-2026-0316, RUSTSEC-2026-0325 through RUSTSEC-2026-0327,
  RUSTSEC-2026-0353 through RUSTSEC-2026-0359, and RUSTSEC-2026-0361 through
  RUSTSEC-2026-0364.
- Fixed: after an agent's owner changes on reload, the former owner's
  conversation no longer keeps its positions on non-durable (`ephemeral:`)
  channels, so the wake no longer respawns that conversation on every publish
  there.
- Hardened: the surface kernel now rejects a malformed full-screen takeover
  request from a component instead of passing it on unchanged. Previously the
  kernel stamped the sender's identity only on requests it could parse, and
  the browser chrome was the only check on the rest. A component granted
  `takeover` that sends a malformed request now gets an error report
  attributed to it.
- Internal: the config loader now takes the `send_rate` table keys, the
  attachment handler `type` words and the webhook signature `scheme` words
  from the code that defines them, so a new option added there can no longer
  go without a config spelling unnoticed. What you write in config and the
  error messages for bad config are unchanged.
- Internal (tests only): a panic in a background task started by the
  in-process test server, such as a WebSocket session tearing down, now fails
  the test that started the server. Before, tokio swallowed these panics and
  the test passed. Some late-teardown panics on the surface route can still
  slip past and are tracked in `TODO.md` (`test-server-teardown-wait`).
  Production panic handling is unchanged.

## [0.24.0] — 2026-09-13

- **Mounts can now curate surfaces.** A new `extend surface "<slug>" { ... }`
  statement lets a config-carrying mount place components on a surface the
  deployment declares. A `surfaces` list on the mount's principal caps which
  displays it may touch, and the surface's own ACL caps what it may bind
  there. The chrome stays the deployment's; a mount contributes panels and
  their wiring.
- **`StandardChrome` assembly** in the packaged `chrome` module. Every in-tree
  page now stamps one line instead of hand-writing eight lines of reserved-plane
  wiring. The four chromes that previously omitted `takeover` bindings now carry
  them; three pages that had no layout channel gain an ephemeral placeholder.
- A principal's `surfaces` axis is attenuated, dead-checked and fit-checked
  like `grants` and reach. A stamp body may write it too.
- The merged-surface chrome rule is now enforced at compile time (exactly one
  `chrome = true` per surface after all contributions land), backing up the
  existing plan-stage assert.

## [0.23.1] — 2026-09-13

- **BREAKING: `sync` and `call` ports are typed.** The specification is now the
  only place either is spelled. `brenn-guest` drops `dom::SyncPort` and
  `dom::MOUNT`; `Activation::sync_is` takes a `brenn_guest::SyncPortName`, whose
  only implementors are `brenn_guest::MOUNT` and the scaffold's generated
  `spec::SyncPort`, and `dom::listen` takes a `brenn_guest::ListenPort`, which
  only the generated `spec::SyncPort` implements, so the mount cause cannot be
  wired to a gesture. `calls::{call, try_call, call_json}` are replaced by
  methods on `calls::CallPort`, one handle per declared `call` port.
  `surface/page-harness` takes the class's vocabulary from the specification
  instead of the caller: `Harness::new(artifact, spec, page)` and
  `Kind::compile(artifact, Declared)`, and the harness refuses an undeclared
  listen, publish, sync cause or inbound window the way the hosts do.
- Out-of-tree migration at the next pin bump, four lines: add `sync <name>;` (and
  `call <name>;`) to the specification, replace hand-spelled port constants with
  `spec::SyncPort::<Variant>` / the generated call handle, replace `dom::MOUNT`
  with `brenn_guest::MOUNT`, and call `Harness::new(artifact, spec, page)` with
  the specification carried as `data`/`env` in the test target. Nothing about an
  already-built artifact's behaviour changes: no runtime contract, package record
  version or bindings-document version moves.

## [0.23.0] — 2026-09-13

- **Component-to-component synchronous calls.** A component can now call a
  peer and receive its reply inline, on either host, through a new `calls` WIT
  import (`brenn:processor/calls`). The caller declares a `call` port; the
  callee declares a `sync` port; the operator wires them in the DSL with
  `call <port> -> <instance>.<port>`. The call graph must be acyclic (enforced
  at compile time). A call runs the peer's activation to completion, flushes
  its buffer, and hands the reply back before the caller resumes. Both hosts
  share one contract (`brenn-activation`), one conformance suite, and identical
  guest SDK verbs. See `docs/config-dsl.md` for the grammar and
  `docs/component-packages.md` for the contract-evolution entry.
- **Sync-call activations on the backend.** The facility that the page kernel
  used for DOM gestures is now a two-host primitive. Backend consumers can be
  called synchronously by native code through `MessagingRouter::sync_call`,
  which is also how the `calls` import reaches a peer. The "headless component
  has no sync cause" restriction is removed.
- **BREAKING: WASM binding depths above 1000 are refused at boot.** Both hosts
  now enforce a shared `WINDOW_DEPTH_CEILING` (1000) as a hard refusal rather
  than silently capping. A deployment whose config carries a depth above 1000
  on any WASM or surface binding will fail to boot until the value is lowered.
  `Depth::Unbounded` on a backend binding resolves to the ceiling once and is
  never seen downstream.
- **BREAKING: `dom.listen` requires a declared `sync` port.** A surface
  component that calls `dom.listen(node, event, port)` must declare `sync
  <port>;` in its specification. Existing in-tree kinds (chrome, echo-stub,
  meeting) have been updated; out-of-tree kinds pay this cut at their next
  rebuild. The runtime `PortCollision` refusal is replaced by a compile-time
  duplicate-declaration check.
- Unified the `NoiseLevel` enum: one shared type in `brenn-envelope`, replacing
  two hand-copied definitions in `brenn-lib` and `surface/schema`. Wire
  spelling, ordering, and `is_backend_enactable()` are defined once.
- The host-specific behaviours record in `brenn-activation` is rewritten as two
  sections (observable differences vs. substrate differences), with sync-call
  causes, the activation time bound, pacing shape, failure record, and the
  `Unbounded` vocabulary each stated with its substrate reason.
- Documented the sync-call facility in `docs/message-bus.md` (a sync call is
  not a message on the bus) and recorded the `calls` grant in
  `docs/security-posture.md` (new section 8.3).
- **Documented which channel schemes each component placement may bind**
  (`docs/message-bus.md` §2.1.1): a parity matrix covering backend consumers
  and surface instances for every scheme, sourced to `bindable_schemes`.
- Host-conformance suite now exercises `ephemeral:` and `local:` on the
  backend, not just `brenn:`. The backend adapter publishes and reads through
  the production `Messenger` path instead of raw SQL, and the three
  previously-ignored scheme-varied scenarios run on all three schemes with
  identical assertions. A `publish_policy_for_addresses` test fixture in
  `brenn-lib` supports the narrowed publish authority.

## [0.22.0]

### Added

- **Config-carrying mounts** close the authorship gap (security-posture B9):
  a mount declared `under` a principal and offering a `config/` tree has that
  tree compiled as part of the deployment, bounded by that principal's words
  and reach. An assistant or any non-operator author can now declare channels,
  place components and wire them without being able to reach past the ceiling
  the operator wrote. A fragment that exceeds its ceiling, or fails to compile,
  refuses the whole reload with the old document still running. See
  `docs/config-dsl.md` *Mounted documents* and boundary **B10** in
  `docs/security-posture.md`.

- **`--mounted NAME=PRINCIPAL=DIR` flag** on `config-check` and `config-diff`,
  naming a config-carrying mount the deployment's mounts document declares.
  Needed because a ceiling principal is dead config without its mount, so a
  `--modules` check of a root that declares one refuses until the flag says
  which mount claims it.

### Changed

- **BREAKING (mounts document): `brenn mounts` prints four tab-separated
  fields, not three.** The fourth is the ceiling (`under:<principal>` or
  empty). Readers splitting on three fixed fields will fold the ceiling into
  the status; `cut -f3` readers are unaffected.
- A mount with `config/` but no `under` is refused, and so is `under` with no
  `config/`. A ceiling principal with nothing beneath it is dead config, so the
  `principal` line and its `mount … under` line must arrive and leave in one
  reload.

## [0.21.0]

### Added

- **Webhook endpoints converge at reload.** Adding, removing, or editing a
  `[[webhook_endpoint]]` block -- or moving an agent's `subscribe "webhook:…"`
  line between agents -- is applied by a reload instead of requiring a restart.
  A rotated signing secret or bearer token is picked up automatically: every
  declared endpoint's `secret_file` is re-read on every reload, so rotating the
  file and reloading is enough. A missing or unreadable secret file refuses the
  whole reload, even when the triggering edit was elsewhere.
- **MQTT client blocks converge at reload.** Declaring a new broker registers
  its session and dials it; removing one stops its supervisor with an orderly
  DISCONNECT; editing one restarts the supervisor, resuming the same persistent
  session at the broker. A rotated broker password or CA bundle needs no
  document edit -- `password_file` and `ca_file` are re-read on every reload on
  the same terms as webhook secrets. A credential the broker rejects is an
  `applied` reload whose client then reaches `Failed` on connect, not a refusal
  (prepare never dials).
- **Offline config-check covers webhook resolution.** `brenn config-check` now
  runs the document half of webhook endpoint resolution: orphan endpoints,
  duplicate slugs and mounts, ownership violations, and replay-store aliasing
  are caught without the deployment host's secrets.

### Changed

- **BREAKING (surface components): a component's linear memory lives for one
  activation.** The browser page now instantiates a kind's compiled module
  inside its activation entry and drops the instance when the entry returns, as
  the backend has always done. A surface component that kept state in a
  `thread_local!`, a static or the heap across activations stops working. There
  is no shim. State is externalized instead: declare an `io state` port, bind it
  `push_depth = 0; retain_depth = 1`, and use the guest SDK's `RetainedState`,
  which reads the newest body out of that port's context window at the top of an
  activation and publishes the new one at the bottom only when it changed. Host
  resources reached by handle are unaffected -- a DOM node handle is owned by the
  mount, not by the memory, and travels in the published state like any other
  field. Two signals for a stale kind: the surface asset record version goes
  from 2 to 3, so a kind built before this change is withheld (see below) with a
  message naming the lifetime; and `brenn-page-harness` instantiates per
  activation, so an unmigrated kind fails its own suite before it is packaged.
  Every in-tree kind is migrated; out-of-tree kinds migrate before they are
  rebuilt against a release carrying this.
- **A surface kind whose record this host cannot read is withheld, not fatal.**
  A record-version mismatch under a **bundle** mount no longer refuses the boot.
  The kind is left out of the served set, one `Warning` alert per withheld kind
  goes out at boot (and at the reload that first withholds it), the page
  manifest of every surface configuring it names the instance under `withheld`
  with the reason, the kernel marks that instance `failed` and reports it, its
  help and schema documents state the withholding, and `/surface-static` answers
  for it with a plain 404 and no fail2ban signal. The reload after the bundle's
  re-release serves it, with no restart. A withheld *chrome* instance renders
  the reason as the page's terminal state rather than spending the reload cap --
  no reload can clear it. A record mismatch under **brenn's own mount** is still
  a boot refusal: the surface tree, the kernel and the binary travel in one
  tarball, so a mismatch there is a broken install. The backend package record
  keeps its boot panic under every mount for now. Record reading is two-phase,
  so a record whose *shape* is foreign is reported as the version skew it is
  rather than as an unknown field.
- **Every backend WASM consumer receives one activation when its task starts.**
  At boot and at every reload that starts or replaces a consumer, the component
  is activated once with whatever its windows hold -- possibly nothing at all --
  plus its deferred windows, exactly as a surface instance is at mount. A
  component whose only input is its own deferred tick can therefore run on the
  backend at all; before this it never activated. A component that reads its
  first message without checking the window now fails loudly at every start
  (alert and log naming wake kind `mount`), and that failure writes no
  quarantine rows, because a mount that consumed nothing has nothing to
  quarantine. The backend package record stays at version 2: the entry ABI did
  not change.
- **BREAKING (components and attachers): a deferred window holds every
  unreleased message, due or not.** The window a component is shown on an
  output port -- and the attach protocol's `DeferredView` frame -- used to hold
  only the entries whose release time was still in the future, so a message
  parked before an outage whose instant passed during it was invisible until
  the host's release pass caught up. It is now every entry no release pass has
  taken, carried with its own `deliver_after`, which may be in the past. An
  empty window therefore means "nothing standing" at every instant, which is
  what the re-arm rule (park a tick at mount iff the deferred window holds
  none) needs to be exact. Cancel and edit keep their cutoff: an entry already
  due is shown and is not cancellable -- the op is the existing no-op outcome,
  now logged with the target's instant and the flush's. No compatibility shim
  for either the WIT or the attach frame: a component or attacher that treated
  every entry in the window as cancellable must read `deliver_after`.
- **A component whose every input binding is sampled is live config, not a boot
  refusal.** Boot used to panic on a surface instance or a WASM consumer whose
  bindings were all `push_depth = 0`, on the ground that it could never
  activate. It can: every mount is owed one activation, so such a component runs
  once at mount and thereafter on its own deferred ticks or on gestures -- which
  is exactly the shape of an instance whose only binding is its retained `io
  state` port. The refusal is now a `tracing::warn!` naming the ports, so a
  config that could not start before starts now.
- **The page manifest names each kind's core modules.** Transpilation moved to
  jco's `--instantiation sync`, whose core-module lookup is synchronous and so
  cannot fetch -- which is what lets an instance be minted inside a sync-call
  activation. `ManifestComponent` gains `cores`, the `.wasm` entries of the
  kind's boot-validated record as build-stamped URLs, and the page compiles them
  all at bring-up.
- **One wildcard webhook route.** The per-endpoint route fold is replaced by a
  single `/webhooks/{*tail}` route matched for every HTTP method, with the
  endpoint ceiling enforced in-handler before the body is buffered past it. An
  unknown mount is a 404 with an unrecognized-URL security event for any method;
  a non-POST to a known mount is a 405 with no security event.
- **MQTT service is always present.** The MQTT service and event router are
  built unconditionally, even when no client is declared. The "MQTT is not
  configured on this server" error paths are removed; per-client checks handle
  undeclared clients instead.
- **Replay components defer their store open.** `ReplayComponent::load` now
  verifies and compiles the WASM module without opening the KV store; a separate
  `open_store` call claims the file. This lets a reload compile a replacement
  while the predecessor still holds the store.

### Fixed

- **A surface page is told when its parked set changes.** Every
  `DeferredView` restatement -- the one a flush that parked, edited or cancelled
  owes -- was addressed to the page's participant id rather than to the key its
  session registered under, so it reached nobody. A page saw its parked set only
  in the snapshot seeded behind `Welcome`, and mirrored a stale one until it
  reattached. Daemons on the remote route were unaffected: their two spellings
  coincide.
- **Webhook event delivery no longer panics on a reload race.** A request
  holding an endpoint entry whose mount was removed mid-flight gets a channel-
  miss warning instead of a false host-bug panic and a Critical alert.
- **MQTT session miss after a reload is a tool error, not a panic.** An agent
  tool call whose ACL snapshot predates the reload that removed its client now
  returns a tool error naming the client, instead of panicking on the registry
  miss.
- **Dynamic MQTT filter edits survive a client restart.** A `subscribe_filter`
  that lands on a successor handle during the predecessor's drain is no longer
  discarded by the inherit step.

## [0.20.3]

### Added

- **Agent reload.** An agent's authority -- grants, ACLs, tool grants,
  subscriptions, and send budget -- converges on reload without a restart.
  Display and behavioural fields (name, icon, allowed users, model allowlist,
  replay limit, and others) also converge. Spawn-shaped fields (model, MCP
  servers, working directory, approval rules, compaction settings, idle
  timeouts) take effect at each live session's next idle moment: the reload
  retires the session and the wake path respawns it, resuming the conversation.
  The status body names the sessions it retired and the ones it left finishing a
  turn. Adding, removing, or renaming an agent, and a handful of boot-shaped
  fields (`mounts`, `container`, `integrations`, Claude profiles, startup hooks,
  webhook subscriptions), are still refused with a diagnostic naming the field.
- **Dynamic subscription re-merge.** Dynamic subscriptions held by a changed
  agent are re-authorized against the candidate's policy on every reload:
  revoked to dormant with the broker filter dropped, revived at the granted
  depths with filter and route restored, or pruned when a static declaration
  supersedes them.

### Changed

- **MQTT ingress diff covers dynamic subscriptions.** The broker
  SUBSCRIBE/UNSUBSCRIBE set is now computed over static channels plus
  kept-dynamic rows, matching what a fresh boot derives, rather than the
  plan's static list alone.
- **Removed-user connections are closed on reload.** When `allowed_users` drops
  a user, the reload pulses every open WebSocket; each re-checks and closes
  itself when denied. An owner change reaps the old owner's delivery positions
  and seats the new owner's conversation.

## [0.20.2]

### Changed

- **Declared MQTT clients hold sessions unconditionally.** Previously, a broker
  session was spawned only for clients referenced by an ingress channel or ACL
  matcher at boot. Now every `mqtt_client` block gets a supervisor at startup.
  An idle connection costs one keepalive; the operator wrote the declaration, so
  the connection is what they asked for.
- Fully hermetic toolchain via bazel

### Fixed

- **Failed MQTT clients distinguished from deferred ones.** A broker supervisor
  that gave up after an authoritative failure (bad credentials, rejected TLS
  chain) was reported as "deferred, will converge on reconnect" in reload status
  and in `MessageSubscribe` results. It is now reported separately:
  `mqtt_failed` in the reload status body, `subscribed_client_failed` on the
  subscribe response. The filter is still installed for reconnect survival, but
  the operator sees that waiting is unlikely to help.

## [0.20.1] — 2026-09-07

Reload covers the full deployment surface. A running server can now pick up new
WASM component packages, new surface kinds, new surface instances, and new MQTT
ingress subscriptions without restarting -- the four things the v0.20.0 reload
left non-convergible that operators actually hit on every deploy. The unit of
deployment is a "mount": one directory declared in a new mounts document that
replaces the three `--modules`/`--components`/`--surface` flags and is re-read
on every reload, so a bundle installed into a declared mount is live after a
`systemctl --user reload` with no unit edit and no bounce.

### Added

- **Mounts.** A small `.brenn` document (`--mounts FILE`) declares the
  directories the host reads components, surface assets, and packaged modules
  from. Brenn's own release is a mount; each out-of-tree bundle is another.
  Roots are derived from the mounts on every reload rather than fixed at boot,
  so a bundle installed after the process started is visible to the next reload.
  Two new subcommands: `brenn mounts` lists declared mounts and their on-disk
  status (the bundle installer calls it), `brenn config-status` reads the
  retained reload outcome off the database (the installer polls it to learn
  whether a reload landed). The retired `--components` and `--surface` flags are
  parse errors; `--modules` is accepted only on `config-check` and `config-diff`
  for workstation use.
- **Surface convergence.** Surfaces join channels, links, and WASM consumers as
  convergible blocks. Adding a surface, changing one (including upgrading the
  kind's assets under a mount), or removing one is applied in place. Open pages
  of a changed surface receive close code 3002 and reload themselves; pages of a
  removed surface receive 3003 and show "this surface has been retired". A page
  that lands during the brief swap window gets a 503 with a meta-refresh rather
  than an error. The retained `brenn:config.status` body reports
  `surfaces_added`, `surfaces_removed`, `surfaces_changed`, and
  `kinds_changed`.
- **MQTT ingress convergence.** Adding or removing an `mqtt:` channel on reload
  issues the broker SUBSCRIBE/UNSUBSCRIBE and wires or unwires the ingress
  route. A binding on a client the process has no broker session for (because
  the client was not referenced when it booted) is a stated refusal asking for
  a restart, not a silent failure.
- **Dev mounts generator** (`bazel/wasm/dev_mounts.sh`): synthesises a mounts
  document and symlinked mount directories from build outputs, so `make
  launchdev` and out-of-tree bundle repositories can run under the same
  `--mounts` path the production host uses.

### Changed

- **Reload rule 1 generalized.** A subscriber on a moving channel entry is now
  accepted when it is a surface the surface delta is also moving, not only a
  WASM consumer.
- **Kind fingerprints recorded unconditionally.** Every surface kind offered by
  any mount is fingerprinted at boot and at every reload, whether or not a
  surface currently instantiates it. A kind upgrade under a mount promotes
  every surface mounting it to "changed" through the delta's kind closure.
- **Consumer change identity is release content, not path.** A WASM package
  whose world and digests are identical across a mount's versioned-tree swap is
  left running; only a real artifact or specification change restarts it.
- **Attach send budgets are swappable.** The per-principal rate-limit buckets
  for surface attach sessions can be replaced or removed at reload without a
  restart.

### Fixed

- **Reload rule 2 covers dynamically minted channels.** A channel address that
  a dynamic MQTT subscription or an attach session created at runtime is now
  refused if the reloaded document tries to declare the same address, rather
  than hitting an assert in the commit path.
- **Surface reconfigure swap-window page load.** A page that reloads itself on
  close code 3002 and lands before the new runtime is installed gets a 503 with
  a `<meta http-equiv="refresh">` document instead of an empty error page, so
  the browser retries automatically.

## [0.20.0] — 2026-09-06

A running server can now be told to re-read its config document and converge to
it. The reload is deliberately narrow: channels, links, and WASM consumers move
in place, and anything else that differs is refused outright rather than
half-applied. Both doors — `SIGUSR1` and a published message — are off unless
the deployment declares the reload channel pair.

The rest is DSL and formatter correctness: the substrate-wired `tool-results`
port is now something a `.brenn` document can express, and `brennfmt` stops
orphaning statement terminators.

### Added

- **Live config reload.** A running server can re-read its config document and
  converge to it without restarting. Two doors, both off unless the deployment
  declares the reload channel pair (`ConfigReload` from the new
  `@config-reload` library module): `SIGUSR1` and a message published to
  `brenn:config.reload`. Channels, links, and WASM consumers — including
  consumers whose package artifact changed on disk — are added, removed, or
  retuned in place. Everything else (agents, surfaces, remotes, MQTT clients,
  server settings) is compared whole: any difference is refused and the running
  system is left untouched. Outcomes are published to `brenn:config.status` as
  a retained JSON body and fired as alerts on refusal.
- **Document identity hash.** `brenn config-check` and the boot log now report a
  SHA-256 over every file the compile read, keyed by document-relative path, so
  what an operator certified offline can be held against what a process is
  running.
- **Channel tombstones.** Removing a channel on reload drops its subscriptions
  and publishes a tombstone so late readers learn the address is gone rather than
  silently missing messages.
- **Messaging planner extracted.** The channel and consumer resolution that boot
  performs is now a pure `plan_messaging` function in `brenn-messaging-boot`,
  callable from both boot and reload without duplicating the wiring logic.
- **WASM consumer lifecycle extracted.** Consumer load, start, and stop are
  factored out of `brenn-bootstrap` into a `ConsumerRegistry` and per-consumer
  `LoadedConsumer`, so reload can retire and replace individual consumers
  without touching the rest of the dispatch tree.
- **Security posture: boundary B9** documents the reload requester's trust
  model — the request is a trigger, never an input, and the content gate is the
  document on disk under the same compiler the boot path uses.

### Fixed

- **Statement formatting no longer orphans terminators.** A `;` that ends a
  statement used to drift below any trailing comment or blank line the author
  wrote after it — `const p = ["a"];` followed by a blank line became two lines
  with the blank above a bare `;`. The formatter now keeps the semicolon with
  its statement and renders trailing trivia after it. Every corpus golden is
  regenerated to match. The fix is in fltk (the grammar toolkit that generates
  the formatter); brenn re-pins fltk and removes the grammar-level workarounds
  that `link` and `use` statements carried for the bug, restoring an
  optional-whitespace position before their terminators.
- **`git-sync-consumer` config keys are writable in `.brenn`.** The per-repo
  remote keys used a colon (`remote:<slug>`) that the DSL grammar cannot spell.
  They are now `remote_<slug>`, with underscores. No backward compatibility —
  the consumer has never been configured from a `.brenn` file in production.
- **The `tool-results` inbox port is substrate-wired.** A component class
  requiring `tools` declares `in tool-results;`, but an instance never binds
  it — the runtime folds the inbox in from the instance's `tool` grants. The
  DSL now knows this: it skips the port in its mandatory-binding check, refuses
  any explicit binding of it, and enforces the coupling between `tools` in
  `requires` and the port declaration in both directions. Previously no
  `.brenn` document could instantiate a component holding an async tool grant —
  leaving the port unbound was a compile error, binding it was a runtime
  refusal. The shared constant `TOOL_RESULT_INPUT_PORT` moves to
  `brenn-envelope` so both layers reference one name.
- **Component bindings cannot reach tool-namespace channels.** A component
  port bound to a `brenn:tool-results/` or `brenn:tools/` address is now
  refused at compile time and at boot. Agents' `subscribe` statements are
  unaffected.
- **`fatal` noise on a tool-result inbox is refused.** A tuning block or
  inherited global default that would set `fatal` on a `brenn:tool-results/`
  channel is caught at compile time — the backend has no kill wire to enact it.


## [0.19.0] — 2026-09-04

Out-of-tree components are real — a separate repository can build and deploy
them. The install flags changed to support that, and `surface_dist_dir` moved
out of config and onto the command line. Separately, agents can switch between
Claude accounts at run time without losing their conversation.

Authority is the other half of the out-of-tree components story. A packaged
assembly is config text an out-of-tree author wrote, and stamping one used to
accept everything it declared. Now the deployment states what the arrangement may hold — a
*principal*, a *ceiling*, or both — and the compiler proves the arrangement fits
under it. Brenn's own surface self-description vocabulary stops being a file
every deployment copies and ships as a library module in the release instead.

### Added

- **Principals and stamp ceilings.** Deployer text can declare a `principal` — a
  named bundle of authority, grant words plus reach, optionally `under` another
  principal — and a `new` of an assembly can be stamped `under` one and carry a
  ceiling body (`grants = [...];` and `acl` lines). The compiler derives what the
  arrangement actually confers — capability words, surface attach words, every
  `acl` entry, binding-derived reach, and every `grant` it emits — and refuses
  anything the ceiling does not cover, in both directions: consent that covers
  nothing is refused as dead config too. An assembly reaches a principal through
  a new `Principal` parameter type. Nothing is lowered; this is entirely a
  compile-time check. `docs/config-dsl.md` (*Principals*, *Assemblies*) is the
  reference.
- **Library modules, and brenn's own vocabulary as the first one.** A `.brenn`
  file that no component package or surface kind owns can now ship in a release
  or bundle's `modules/` tree: `library_modules` on `release_package` and
  `component_bundle` stages it and lists it in `modules/library-modules.txt`,
  each entry gated by a generated `library_module_test`, and the package and
  bundle checkers hold the listed half against the harvested one. The surface
  self-description assemblies — `SurfaceCommons`, `SurfaceDescription(slug)`,
  `KindDescription(kind)` — are the first: they ship as
  `@surface-description` and are imported, not copied.
- **The host says what it resolved.** The existing "WASM processor component
  loaded" and "replay protection loaded" lines now carry the resolving root, the
  world, and the artifact and spec hashes, and the surface asset walk logs one
  line per kind (kind, root, spec and source hashes) plus one for the kernel
  root. No new log lines per instance — the journal now answers "which release
  is this host running" without one.
- **`config_fit_refusal_test`.** The fit gate's other direction as a rule an
  out-of-tree author can call: a fixture root that must *not* compile, with a
  mandatory `expect` string.
- **Security posture: the packaged-module boundary has a name.** A new actor row
  for the component author's `.brenn` text (out-of-tree: untrusted, adversarial
  — it is not operator config even though the operator imports it) and boundary
  B8, packaged module text → compiler, with its threats and reviewer rubric.
- **Out-of-tree components are real.** A separate repository can depend on
  brenn as a Bazel module and build backend and page-hosted components using
  brenn's own rules and gates. The release artifact is a *bundle* — a directory
  tree the host loads alongside brenn's own. Copy `examples/component/` to
  start; `docs/component-packages.md` is the full contract.
  **Operator action:** give each bundle its own install directories, separate
  from brenn's. A bundle directory nested inside brenn's sync tree will be
  deleted on brenn's next deploy.
- **Repeatable install roots.** `--modules`, `serve --components`, and the new
  `serve --surface` all accept multiple directories — one per installed release.
  A duplicate module or package name across roots is refused at boot.
- **Claude account profiles.** `claude_profile` declares a named account backed
  by a `claude setup-token` token file. An agent lists the accounts it may use
  (`claude_profiles`) and can optionally be steered between them at run time via
  a pub/sub channel (`claude_profile_goal`). Switching is seamless — the
  conversation respawns transparently, no history lost. Agents without profiles
  are unaffected. See `docs/claude-accounts.md`.
  **Operator action:** the server's environment must not carry credentials that
  outrank the token (`ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`, etc.) or
  the profile is silently ignored. Boot refuses the cases it can detect.
  **Accepted risk (§7.1a):** the token is readable by the conversation's own
  tools and is minted for a year; see `docs/security-posture.md`.

### Changed

- **BREAKING: stamping a packaged assembly now states what it may hold.** A
  `new` of an assembly declared in a packaged module, written in deployer text,
  is checked against its ceiling. An arrangement that confers a capability word,
  a surface attach word, an `acl` entry or a `grant` the ceiling does not cover
  is refused — and the refusal writes the line you need. An arrangement that
  confers nothing and reaches only its own and its handed channels still needs
  no ceiling text, which is the common case.
  **Operator action:** expect a refusal on the first check after a bundle pin
  bump that grows an assembly's authority. That is the intended failure:
  authority a bundle grows is authority you re-consent to, in your file.
- **BREAKING: `config/surfaces.brenn` is gone.** The self-description
  vocabulary ships in the release's `modules/` tree.
  **Operator action:** replace `use config::surfaces::*;` with
  `use @surface-description::*;` and add `new surface_commons: SurfaceCommons;`
  beside the per-slug and per-kind description stamps. Retention that was tuned
  by forking the file is now arguments at the stamp (`errors_retain`,
  `errors_standing`, `geometry_retain`, `status_retain`). Release first: a
  config importing this vocabulary does not compile against an older tarball.
- **BREAKING: `config-check` refuses a missing self-description stamp.** The
  half of the boot-time description validation that is a pure function of the
  document now runs offline, so a forgotten stamp is refused *before* the
  installer stops the service instead of panicking after it. The widening this
  implies: every document with messaging active must declare
  `brenn:surface.index`, surface or no surface — hand-written or by stamping
  `SurfaceCommons`. Documents that used to pass the gate and die at boot are now
  refused by the gate. These refusals and the error-lane ones are spelled
  `config: [<lane>] …` rather than `boot: …`, and the eviction-frontier warning
  is reported by `config-check` as advice beside its verdict.
- **BREAKING: `unbounded` cannot name a constant or a parameter.** It is the
  word a depth spells an unbounded window with, and is refused at the
  declaration so a depth written `unbounded` always means the word.
- **`SurfaceCommons`'s `errors_standing` default is `1024`.** The old default
  tripped the boot check's eviction-frontier warning in every deployment that
  took it; 1024 is four admitted send bursts. Raise it at the stamp if more
  surfaces than that report.
- **BREAKING: `server.surface_dist_dir` is now `serve --surface <DIR>`.**
  The config key is gone; a document that still has it is refused.
  **Operator action:** remove the key from your `.brenn` file and add
  `--surface <DIR>` to the unit's `ExecStart` in the same deploy.
- **BREAKING for component authors: out-port enforcement.** Publishing to a
  port name the component's spec does not declare now traps the activation.
  Publishing to a declared but unwired optional port now silently succeeds
  (the message is dropped) instead of returning an error. Bindings document
  is v2; no shim.
- `npm-audit` is temporarily out of `make check`; run `make npm-audit` by hand.

### Fixed

- **A name could not stand in for a number in a depth.** `push_depth`,
  `retain_depth`, `standing_retain_depth` and a surface instance's
  `parked_batch_depth` read a bare token, so a constant or an `Int` parameter
  was refused there. They now take an integer, the word `unbounded`, or a name
  that resolves to an integer — which is what lets a packaged assembly
  parameterise its own retention.
- **A multi-line `///` comment on an indented declaration was a syntax error.**
  Anything declared inside an assembly or surface body could carry only a
  single-line doc comment. Whitespace and blank lines between `///` lines are
  now part of one comment (a `//` line between them is still refused), and
  `brennfmt` renders every line contiguously at the declaration's indent.

## [0.18.2] — 2026-08-31

### Added

- **Per-app model allow-list.** `models = [...]` on an app restricts the model
  picker and the server enforces it. A single-element list locks the picker.
  Omit the key to keep the current unrestricted behavior.

### Changed

- **Model re-assertion on every send.** The server now resets a conversation to
  the app's configured model on every message, not just when a client sends an
  explicit override. This is what unsticks a session that drifted to the wrong
  model. It also means the model follows whichever client sent last.

### Fixed

- **Stale model preference in the browser.** A model the server no longer
  offers was being resurrected from `localStorage` on every page load. Now
  cleared the first time the server's model list excludes it.

## [0.18.1] — 2026-08-31

### Fixed

- **Surface components from bundles would crash immediately.** The kernel was
  handing message envelopes to guests as JSON objects instead of the JSON
  strings the WIT contract specifies. Every out-of-tree surface component
  trapped on its first non-empty port window, and nothing in the logs or the
  status document said why. Fixed the serialization, and also fixed the
  observability: trap reasons now reach the console and `brenn:surface-errors`,
  the status document reports `degraded` immediately when an instance dies
  (instead of `health: ok` until the next heartbeat), and repeated identical
  failures are counted rather than flooding the error channel.

## [0.18.0] — 2026-08-31

The story here is that a component now ships as a **package** — the artifact,
the specification its author wrote, and a record binding the two — and the host
re-computes that binding at boot and refuses to start when anything disagrees.
Around that, the processor/DOM split is gone: one component vocabulary, one
artifact shape, and page access is a capability rather than an ABI. Two new
command-line flags tell the server where the installed modules and packages
live, so the same document checks on a workstation and boots on a host. And the
TOML config front end is gone, which makes the config DSL the only notation.

Nearly everything below is breaking. Budget a config edit and a unit-file edit
for this upgrade, not a binary swap. `docs/config-dsl.md` is the new prose
reference for the configuration language, and `docs/component-packages.md` the
normative contract for out-of-tree component authors.

### Added

- **Component packages.** A backend WASM component is no longer a bare `.wasm`.
  It is a directory named for the package, holding `package.json` (record v2),
  the artifact, and — for a processor-world component — a verbatim copy of the
  author's `<name>.brenn` specification. Before loading an instance the host
  resolves `<components root>/<package name>/`, re-hashes the artifact and the
  packaged specification against the record, and compares the packaged
  specification's hash to the hash of the specification the *configuration*
  compiled against. Byte-identical or refuse. Every failure is a panic naming
  the path and the remedy.
  **Operator action:** install one package directory per component and point
  the server at their parent with the new `serve --components <DIR>`. A host
  started without it panics naming the flag as soon as a configuration loads a
  component.
- **`brenn --modules <DIR>` and packaged-module imports.** A deployment no
  longer restates a component's specification; it imports the author's file:
  `use @processor-demo::*;`, resolved as `<module root>/processor-demo.brenn`.
  The root is an environment fact, so it is named on the command line and never
  in the document — the same document checks against a source checkout on a
  workstation and boots against the installed tree on a host. `--modules` is a
  global flag and must precede the subcommand; a document with an `@` import and
  no `--modules` is refused naming the flag.
  **Operator action:** the release now stages a flat `modules/` tree beside the
  packages; pass it as `--modules`. Surface kinds are imported this way too, so
  a deployment needs the module root whether or not it runs backend components.
- **Page capabilities and the sync channel.** `dom` gives a component a handle
  table over its own subtree and nothing else; `page-dom` — held by exactly one
  instance per surface, its chrome — is the separate authority to reach outside
  it. The element and attribute vocabularies are fixed allow-lists, admitting
  nothing that can navigate, fetch, or execute; `docs/security-posture.md`
  carries the admission rule. Alongside them, an activation may now carry a
  **sync** port: a live request the component answers in the same turn, which is
  how a DOM event listener gets to cancel or proceed in band.

### Changed

- **BREAKING: one component vocabulary at both placements.** The
  processor/DOM component split is gone. `abi = dom;` no longer exists —
  `abi = processor;` is the one artifact shape both hosts load, and where an
  instance runs is decided by where it is placed and what it is granted. What
  this costs a configuration, concretely:
  - Every component instance now carries a required `grants` list, at both
    placements, in one vocabulary. A surface-placed component that renders needs
    `dom`; a chrome needs `page-dom` as well. `takeover` moved off the `surface`
    block and onto the instance that requests the overlay.
  - Every class writes `requires` (a component that needs nothing writes
    `requires = [];`). Spec fit is checked in both directions: a required word
    the instance was not granted is refused, and so is a granted word the class
    never asked for.
  - Ports are required unless the class marks them `optional`. An unbound port
    is no longer legal by default.
  - `component_path` is gone from the vocabulary entirely; a document that still
    states it is refused as an unknown key. The package name is the whole
    reference.
  - A webhook's `replay_protection` names its guard by installed package:
    `component = "replay-generic";` in place of the old artifact path.
- **BREAKING: the release tree changed shape.** Components moved out of `lib/`:
  the deploy manifest is now `components/deployed-components.txt` and each entry
  is a `components/<name>/` package directory. New alongside it are `modules/`
  (the module root) and `scripts/manifest_names.sh` (the manifest grammar, which
  a deploying repo's preflight execs rather than transcribing). Deploy tooling
  that copied `lib/*.wasm` will not find them.
- **BREAKING: surface components ship as jco-transpiled directories.** The
  wasm-bindgen `brenn_<kind>.js` / `_bg.wasm` quadruple is replaced by
  `surface/processor/<kind>/`, carrying a v2 `manifest.json` with the kind's
  packaged specification and its hash. Boot re-derives every stated filename,
  re-hashes each, and binds per instance against the specification the
  configuration compiled against. **Operator action:** install the surface asset
  tree wholesale rather than overlaying it — a file from a prior release
  surviving beside a fresh record is a boot refusal. The same holds for the
  module root and the components root: sync them, never overlay.
- **BREAKING for component authors: `receive` changed shape.** It now returns
  `result<option<string>, receive-error>`; `activation` gains a `sync` field
  naming the live port when the activation is a sync call. `ok(none)` on every
  ordinary activation, `ok(some(reply))` only on a sync call, and a reply to a
  cause that asked nothing is a trap. The `processor` world also imports the new
  `dom` and `page-dom` interfaces. There is no compatibility shim: a component
  built against the 0.17 world does not load.

### Removed

- **BREAKING:** TOML config is no longer loadable. `--config` accepts only a
  `.brenn` document; any other extension is a startup panic, and the no-`--config`
  fallback probes `brenn.brenn` only. A `brenn.toml` beside it is ignored.
  Convert the file before upgrading — `brenn config-check <file>.brenn` validates
  the result, and `brenn config-diff` compares two documents as configurations.

## [0.17.0] — 2026-08-22

### Changed

- A new config DSL replaces TOML config

## [0.16.4] — 2026-08-14

### Fixed

- Containers were not properly shut down and could sometimes be orphaned.

### Changed

- More Bazel/CI speed optimizations

## [0.16.3] — 2026-08-11

### Changed

- Bazel/CI speed optimizations

## [0.16.2] — 2026-08-10

### Changed

- Bazel is now the build system.

## [0.16.1] — 2026-08-07

### Fixed

- Help channel text (channels published by components to teach LLMs how to use
  them) had documentation rot. Now help text is generated within and from the
  code itself to help prevent this.

## [0.16.0] — 2026-08-02

The main stories here are: We are preparing to bring voice assistants in (by
extending the message bus to remotes over a generic websocket protocol, and
putting LLM conversations on the bus), and we refactored a lot of code in the
pixel-surface and the websocket, which fixed various bugs that had never been
logged as bugs and also just made the code much better and easier to maintain.

### Added

- **Remote attachers.** A non-browser process can attach to the message bus over
  the same websocket the browser uses: new `[[remote]]` config block and
  `/remote/<slug>/ws` route, file-backed bearer token (mode-checked `0600`;
  unreadable or empty is a boot refusal), authority re-derived per frame from the
  config's channel matchers. No mTLS.
- **Per-app conversation roster** — `brenn:<prefix>.app.<app-slug>.roster`, a
  full snapshot of an app's conversation ids with a single reserved writer
  (`docs/chat-protocol.md` §9). Completes the chat protocol that shipped
  undocumented in 0.15.0, below.
- **`make wasm-toolchain-install`.** Guest toolchain pins now live only in the
  `Makefile`; preflights, CI, and the check-wit gate read them from there.

### Changed

- **The browser websocket protocol is now a pure message-bus extension** — it
  knows nothing about DOM, pixels, or components, and everything
  surface-specific rides as ordinary messages on ordinary channels.
  **Operator action:** every surface needs a `[[channel]]` block for its bindings
  document (`ephemeral:surface.surface.<slug>.bindings`, both depths `1`) or the
  surface will not come up. Delivery is now single-target per (attachment,
  channel); fan-out to instances and ports happens in the attacher.
- **Guest WebAssembly proposals are a closed allow-list** rather than whatever
  the wasmtime release defaults to. Notably relaxed-simd is no longer accepted —
  its results are implementation-dependent, which a byte-identical transcript
  cannot survive. Out-of-envelope guests are refused at compile time with the
  proposal named. Ships with wasmtime 47 (from 45), clearing RUSTSEC-2026-0222.
- **Gesture handlers run inside an activation,** so their publishes get the same
  flush boundary, cancelability, and offline parking as everything else, and
  every component gets a guaranteed activation at mount. Breaking for component
  authors: the free publish functions are gone in favor of `wire_gesture`,
  `PersistentTimer` is deleted (use a deferred self-publish on an in/out port),
  and `Activation` gains a `sync` field.
- **Latest-wins ports no longer fold.** More than one new message is an operator
  misconfiguration, so the port takes the latest and publishes an error rather
  than papering over it. **Operator action:** set `push_depth = 1` on every
  latest-wins binding. Event streams and accumulating ports are unaffected.
- `surface/proto` is now `surface/schema`, and the wire contract and its client
  moved to new `attach/proto` and `attach/client` crates. The attach protocol is
  at v3; both ends land together and there are no compatibility shims.

### Fixed

- **surface:** no more burst of `rejected layout doc` warnings on page load. A
  subscriber's catch-up replay was one frame per retained message, so each
  arrived in its own activation looking like the only new message; a catch-up run
  is now a single frame.
- **chat:** an app's lazily minted singleton conversation could exist without its
  chat channels and go unnamed on the app's roster.
- **wasm:** the guest yield fix for a component livelock had shipped on a line
  that never executed.
- `make e2e` runs again, and no longer carries a hardcoded date that turned the
  tree red once it passed.

## [0.15.0] — 2026-07-30

> The chat-over-pub/sub entries below were added retroactively; the work shipped
> in 0.15.0 but went undocumented at the time.

### Added

- **Chat with a Brenn-hosted LLM over the message bus.** Any authorized peer can
  drive a conversation and read its record without a browser, keeping Brenn's
  conversation context and tool calling. Each app+conversation gets `in`
  (commands), `out` (the durable record), `stream` (tokens), and `wake`
  (pre-warm) under a configurable prefix; the conversation id sits last in the
  address so grants work at one-conversation, all-conversations, or whole-app
  grain without wildcards. Commands are `send`, `stop`, `set_model`, `compact` —
  no busy-gate, and text sent mid-turn is injected at the end of the current
  tool-use round. There is no history API: the `out` retained window is the
  history. The stream is decoration, the durable record is truth. Raw text only,
  no HTML. Protocol `v1`, additive-only; `docs/chat-protocol.md` is the normative
  spec, written for a peer author who reads no Rust.
- **`[llm_chat]` config section** — `prefix`, `retained_window`, `wake_min`,
  `idle_timeout_secs`. A command published below `wake_min` parks instead of
  buying a subprocess. Malformed values are a boot refusal.
- **Impetus.** A bounded per-conversation pool that unattended turn-provoking
  injections draw from and human attention refills. Minting it requires a
  capability no configuration can grant, so an automation loop is bounded by a
  stock only a person can restore. A command arriving at an exhausted pool is
  refused whole.
- Auto and anonymous channels which come into being not with a separate spec and
  ACLs but because two component ports are connected to one another. ACLs are
  automatically granted and depth is automatically computed. If a name is given,
  it's a normal named channel; otherwise it's a private anonymous channel given
  only a UUID which is not discoverable by any means except to the ports
  connected to it.
- In/out ports, for a component that relies on listening to its own outputs
  (e.g. for timers/scheduling). This is one port that's both an input and an
  output and must be connected to the same channel. It automatically gets an
  anonymous local channel there if the operator doesn't connect something
  different.

### Changed

- **Behavior change for every deployed LLM app.** Authority over a conversation's
  chat channels moved from the app's own LLM to the server-side harness that
  wraps it. Previously each app was granted its whole chat tree, which put bus
  tools in every LLM prompt and let a conversation prompt itself. Bus identity is
  unchanged and there is no compatibility shim. A conversation is also no longer
  ambience-injected with its own messages.
- Unification of ephemeral/durable channels on the messaging substrate resulted
  in many configuration and default-behavior changes, and many bugs fixed (not
  individually enumerated here). The tldr is that every channel (other that auto
  channels) need explicit depth specifications in the config file. Nothing
  defaults to `unbounded` anymore, but the operator can still use that in a
  config file explicitly.

### Fixed

- **scrub:** `brenn-scrub tree` handles submodules. A tracked gitlink is now
  recognized from the index and scanned as the pointer text git records for it
  — the same text the staged and push scans see in a diff — with one stderr
  line naming the path and saying the submodule's own contents belong to
  another repository. Previously a repository carrying a submodule could not be
  swept at all: a checked-out submodule directory aborted the scan, and an
  uninitialized one was mislabeled a staged deletion and skipped.
- **scrub:** `brenn-scrub tree` refuses a repository with an unresolved merge
  instead of destroying the conflicted file. A path in conflict is three index
  entries, and the tree sweep processed all three: the second one copied the
  mirror's hardlink back onto the worktree file, truncating it to zero bytes,
  and the run then reported the empty result as a clean tree. Unmerged entries
  are now a hard refusal naming the path, and mirroring one path twice is
  refused outright.

## [0.14.3] — 2026-07-27

### Added

- Extensive refactoring of message bus substrate on the backend brings parity to
  all transports: both LLMs and WASM apps can now publish and subscribe with all
  features in parity across `brenn:`, `ephemeral:`, and `local:` channels. Many
  redundant internal code paths unified onto common traits.
- WASM apps gain the ability to query retained messages on inputs, set
  `deliver_after` on output messages, and view/edit/cancel deferred/pending
  output messages, giving them the ability to self-schedule timed events
  (replacing the need for timers, cron, and scheduling).

## [0.14.2] — 2026-07-22

### Added

- **surface:** the surface status document now reports overlay state — whether a
  fullscreen takeover is showing, which component holds it, and since when. A
  bar stuck fullscreen is now visible to health tooling instead of reporting
  `health: ok`.

### Fixed

- **surface:** the deskbar no longer wedges fullscreen when a meeting is
  rescheduled or replaced while inside its takeover window. Takeover requests and
  releases published from within a component activation are now stamped with the
  publishing identity, so a release is always attributable and the fullscreen
  overlay clears cleanly no matter when the replacement arrives.
- **surface:** the deskbar no longer logs spurious "dropped takeover release …
  does not hold the overlay" warnings at theme boundaries, on reconnects, and at
  other odd times. The chrome layer now processes only newly delivered
  control-plane messages instead of re-folding the retained last value on every
  screen update.
- **surface:** dismissing or snoozing a meeting now applies only to that specific
  occurrence. Previously a dismissal was keyed by meeting id alone and never
  aged out, so it silently suppressed every future meeting that reused the same
  id.
- **surface:** messages published to a `local:` channel prior to the consumer
  mounting the channel are now delivered as new instead of only existing as
  retained context.

## [0.14.1] — 2026-07-22

### Fixed

- **scrub:** hook mode resolves the repo — and its `.gitleaks.toml` — from the
  write destination rather than the session's working directory. A write into a
  different repo is now scanned against that repo's config, and a write to an
  ungated destination passes instead of being refused.
- **xtask:** `xtask check` no longer fails intermittently. Lanes that overlap
  the tree walk are now read-only, eliminating a readdir/stat race in which
  transient files written by one lane vanished while a sibling stat'd them.
- **xtask:** a failing `xtask check` lane now reports its own name and panic
  message instead of a generic "a scoped thread panicked".
- **xtask:** `xtask check` builds the WASM components it reads, so check-wit no
  longer aborts with "artifact not found" on a fresh tree.

### Internal

- brenn-cli's binary-spawning tests moved to integration tests, locating the
  binary through `CARGO_BIN_EXE_brenn-cli` instead of guessing a `target/debug`
  path that only existed after a prior build. Suite grew from 17 to 24 tests.
- New `git-fixture` dev crate runs git-touching tests in a scrubbed, hermetic
  environment, with a canary that detects fixture escape into the real repo and
  an xtask gate against unallowlisted raw git spawns.

## [0.14.0] — 2026-07-21

First public release.
