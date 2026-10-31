# Connecting Google accounts

This guide sets Brenn up so the people who use it can connect their own Google
accounts (Gmail and Calendar), and their assistant can read and act through
them. Each user's tokens live in a vault on the host. They never enter an
agent's container: Brenn attaches the credential to each request itself.

The configuration keys are documented in `docs/config-dsl.md`, under *OAuth
providers and the credential vault*. This guide covers the Google side, a
working block, the grants, and the first connect.

Google Maps Platform APIs (Weather, Places, Routes) take a deployment-wide API
key rather than a user's account; they are set up in
`docs/google-maps-platform.md`.

## 1. The Google Cloud project

One project serves every user of this Brenn deployment.

1. In the Google Cloud console, create a project (for example `brenn-alice`).
2. **APIs & Services → Library**: enable the **Gmail API** and the **Google
   Calendar API**. Add others (People, Drive) only when a grant needs them.
3. **Google Auth Platform → Branding / Audience**: user type **External**. Set
   the app name. Brenn shows the same name as `app_name`, so the user
   recognises it on the consent screen. Then **publish** the app. Leave it
   unverified: an app in *Testing* status gets refresh tokens that expire after
   seven days. A published app's tokens last until the user revokes them,
   changes their password, or leaves them unused for six months.
4. **Data access**: add every scope that any `scope_sets` entry below names.
   The consent screen offers the union.
5. **Clients → Create client → Web application**. Under *Authorized redirect
   URIs*, add exactly
   `<server.public_url>/oauth/google/callback`
   (for example `https://brenn.example.com/oauth/google/callback`). Download
   the client's JSON.
6. Put the download on the host where Brenn can read it, mode `0600`, owned by
   the server's user (for example `/etc/brenn/google/client_secret.json`).
   Brenn refuses to start if the mode is wider, or if the file's
   `redirect_uris` does not contain the callback above.

Gmail's read and modify scopes are *restricted*. An unverified app shows users
an "unverified app" warning before consent. Google may also refuse restricted
scopes to an unverified app entirely. If so, the consent attempt ends with
"nothing was connected", and calendar scopes, which are only *sensitive*,
still connect on their own. The first acceptance step (section 6) finds out
which applies to your project.

## 2. The vault

Create the vault directory once, as the server's user:

```
install -d -m 0700 /var/lib/brenn/vault
```

It must not lie inside, or contain, any path the configuration gives an
agent: working directories, repo mounts, state directories, container homes,
`extra_mounts` host paths, or the repo-sync directory. Brenn checks this at
boot and on every reload. A container's `extra_args` reach podman unread, so
a mount written there is not checked and could expose the vault: mount host
paths through `extra_mounts`. Brenn creates `<provider>/<username>/` beneath it. Each credential is
the file `<account>.json`, mode `0600`.

## 3. Configuration

```
server {
  public_url = "https://brenn.example.com";
  vault_dir = "/var/lib/brenn/vault";
}

oauth_provider google {
  app_name           = "Brenn";
  client_secret_file = "/etc/brenn/google/client_secret.json";
  authorize_url      = "https://accounts.google.com/o/oauth2/v2/auth";
  token_url          = "https://oauth2.googleapis.com/token";
  revoke_url         = "https://oauth2.googleapis.com/revoke";
  userinfo_url       = "https://openidconnect.googleapis.com/v1/userinfo";
  account_field      = "email";
  base_scopes        = ["openid", "https://www.googleapis.com/auth/userinfo.email"];
  scope_sets = {
    gmail_readonly    = ["https://www.googleapis.com/auth/gmail.readonly"],
    gmail_organize    = ["https://www.googleapis.com/auth/gmail.labels", "https://www.googleapis.com/auth/gmail.modify"],
    gmail_compose     = ["https://www.googleapis.com/auth/gmail.compose"],
    calendar_readonly = ["https://www.googleapis.com/auth/calendar.readonly"],
    calendar_events   = ["https://www.googleapis.com/auth/calendar.events"],
  };
  extra_authorize_params = { access_type = "offline", prompt = "consent", include_granted_scopes = "true" };
  api_hosts          = ["https://gmail.googleapis.com", "https://www.googleapis.com", "https://people.googleapis.com"];
}
```

`access_type = "offline"` and `prompt = "consent"` make Google issue a refresh
token on every connect, including a reconnect. Without them a reconnect can
come back with no refresh token, and Brenn stores nothing.

Changing this block, or replacing the client file, takes a restart. A reload
that changes it is refused.

## 4. Grants

Only apps with a `container`, and not `multiuser`, may hold these tools. Each
conversation acts with its owner's credentials, never anyone else's.

The assistant, reading only, able to connect and manage its owner's accounts:

```
agent PersonalAssistant {
  container = "sandbox";
  allowed_users = ["alice"];
  tool api-request      { allow { provider = "google"; method = "GET"; } }
  tool oauth-connect    { allow { provider = "google"; } }
  tool oauth-accounts   { allow { provider = "google"; } }
  tool oauth-disconnect { allow { provider = "google"; } }
}
```

The same assistant, also allowed to create drafts and to label mail. Each
write clause names the exact path. `drafts` does not admit `drafts/send`:

```
tool api-request {
  allow { provider = "google"; method = "GET"; }
  allow { provider = "google"; method = "POST"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/drafts"; }
  allow { provider = "google"; method = "POST"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/messages/batchModify"; }
}
```

A WASM automation with no conversation behind it, reading one user's calendar.
Its clause lends it that user's credential. A lent credential is read-only:

```
new agenda_sync: AgendaSync {
  grants = [ports, tools];
  tool api-request {
    allow { provider = "google"; user = "alice"; account = "alice@example.com"; method = "GET"; host = "www.googleapis.com"; path = "/calendar/v3/calendars/*/events"; }
    rate_limit { burst = 5; sustained_per_minute = 30; }
  }
}
```

The rate limit applies per app, or per component, across all its users.

## 5. The component

The `google-workspace-tools` component answers the assistant's Gmail tools
with stamped `api-request` calls for whichever user is calling. Its class,
`GoogleWorkspaceTools`, is imported from the release's module root, at the top
of the document with any other `use` lines:

```
use @google-workspace-tools::*;
```

Then the inbox tuning and the instance:

```
// Each live request has one nested call outstanding: Σ max_concurrency
// results can arrive between two activations, twice over if one is
// re-delivered. 64 holds the 32 ceiling on Σ twice over, so tools joining the
// class change nothing here.
channel at "brenn:tool-results/workspace" {
  push_depth = 1;
  retain_depth = 64;
  standing_retain_depth = 64;
  noise = alarm;
}

new workspace: GoogleWorkspaceTools {
  grants = [log, store, config, tools, provides];
  store_path = "/var/lib/brenn/google-workspace-tools.db";
  tool api-request {
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/labels"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/labels/*"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/messages"; }
    allow { provider = "google"; user = "*"; method = "GET"; host = "gmail.googleapis.com"; path = "/gmail/v1/users/me/messages/*"; }
    rate_limit { burst = 60; sustained_per_minute = 120; }
  }
  config = {
    provider = "google",
    gmail_host = "gmail.googleapis.com"
  };
}
```

The assistant is granted the tools beside its `oauth-connect`,
`oauth-accounts` and `oauth-disconnect` grants (section 4):

```
tool gmail-labels {}
tool gmail-search {}
```

## 6. First connect: acceptance

Do this once per deployment, in order.

1. **Connect your own account first.** In a conversation with the assistant,
   ask it to connect Google with every scope set. Open the link in the browser
   you use for Brenn. Choose the account, go through the "unverified app"
   warning (*Advanced* → *Go to Brenn*), and tick every box. The page should say
   **Connected `<account>` to Brenn for Brenn user `<you>`**. Ask the assistant to list connected
   accounts. Confirm that the Gmail scopes appear among the granted scopes. If
   they are missing, Google refused restricted scopes to the unverified app;
   only calendar access will work.
2. **A family member connects from a phone.** Same steps, on their phone. If
   the link opens in a browser where they are not signed in to Brenn, they see
   the Brenn login page. After logging in they land back on the callback and
   the connect completes, provided they finish within ten minutes of the link
   being made.
3. **The assistant reads the inbox.** With only the `method = "GET"` clause,
   ask the assistant to list the five newest inbox messages. It calls
   `api-request` against `gmail.googleapis.com`.
4. **No token in the container.** In the agent's container home, `grep -r` for
   the start of the access token (`ya29.`) and for the refresh token (`1//`).
   Nothing should match.
5. **Vault modes.** `ls -la` of the vault shows `0700` directories and `0600`
   files, owned by the server's user.
6. **Disconnect.** Ask the assistant to disconnect one account. Then confirm
   that Brenn no longer appears at `myaccount.google.com/permissions` for that
   account, and that its file is gone from the vault.

## 7. Rotating and removing by hand

- **A user's credential.** Delete
  `<vault_dir>/google/<username>/<account>.json`. The next request for it
  reports `credential_invalid`, and the assistant offers to reconnect. Delete
  the file *and* remove Brenn at `myaccount.google.com/permissions` to cut
  access at Google too. Deleting the file alone leaves the token valid at
  Google until it is revoked.
- **The client secret.** Create a new secret on the client in the console,
  download the JSON over the old file (keeping mode `0600`), and restart Brenn.
  Existing refresh tokens keep working. Delete the old secret in the console
  once Brenn is running on the new one. If Google refuses the client's
  credentials (a deleted secret, a deleted client), Brenn alerts once per
  provider until restart, and users' requests fail without asking them to
  reconnect: their credentials are fine, and reconnecting cannot work until
  the client file is fixed.
- **Everything.** Remove the `oauth_provider` block, the `vault_dir` key and
  every grant of the four tools, restart, then delete the vault directory.
- **A stale consent.** If a connect reports that Google did not issue a
  long-lived credential, the user removes Brenn at
  `myaccount.google.com/permissions` and connects again.
