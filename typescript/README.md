# @grow-codes/**KEY**

TypeScript SDK for [**DISPLAY**](https://grow-codes-packages.github.io/openobserve/) by Grow Codes.
Node.js 22.12 or newer, ESM and CommonJS, full type definitions.

```sh
npm install @grow-codes/openobserve
```

```ts
import { OpenObserveClient } from "@grow-codes/openobserve";

// Reads GROW_OPENOBSERVE_URL, GROW_OPENOBSERVE_TOKEN, timeouts, TLS and proxy settings.
const client = OpenObserveClient.fromEnv();
try {
  console.log(await client.health()); // { healthy: true, status: "ok", details: {} }
  console.log(`ping: ${(await client.ping()).toFixed(1)} ms`);
} finally {
  await client.close();
}
```

Every error is a `OpenObserveError` subclass with a canonical `code` (`NOT_FOUND`,
`AUTHENTICATION`, `RATE_LIMITED`, …), `httpStatus`, `requestId` and `retryable`. Credentials
never appear in messages, logs, hook data or `toString()` output.

Test helpers (in-memory fakes and the conformance runner) live in
`@grow-codes/openobserve/testing`.

Documentation: https://grow-codes-packages.github.io/openobserve/ · Licence: Apache-2.0.
**DISPLAY** is a trademark of **VENDOR**; this package is not affiliated with or endorsed by it.
