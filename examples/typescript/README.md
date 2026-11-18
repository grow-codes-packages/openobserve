# TypeScript example

```sh
(cd ../../typescript && npm install && npm run build)
npm install
OPENOBSERVE_URL=http://127.0.0.1:5080 OPENOBSERVE_USER=root@example.com OPENOBSERVE_PASSWORD=... npm start
```

It ingests two records into a new stream, searches them with SQL, sends records through the
batching `LogClient`, reads one back with the `LogQueryClient`, changes stream settings, exports a
span with `initTracing` and reads the trace back, then deletes the stream.

Replace the `file:` dependency in `package.json` with a version (`npm install @grow-codes/openobserve`)
to run it against the published package.
