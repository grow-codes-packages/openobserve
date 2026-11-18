#!/bin/sh
# One-shot bootstrap for the OpenObserve SDK fixture (curlimages/curl, POSIX sh, no jq).
# Idempotent: re-running it rotates the service-account token and rewrites /bootstrap/env.
# Never prints credentials.
set -eu

URL="${OPENOBSERVE_INTERNAL_URL:-http://openobserve:5080}"
ORG="${OPENOBSERVE_ORG:-default}"
STREAM="${OPENOBSERVE_STREAM:-sdk_integration}"
TSTREAM="${OPENOBSERVE_TRACES_STREAM:-sdk_traces}"
SA="${OPENOBSERVE_SA_EMAIL:-sdk-tests@stack.local}"
ROOT_USER="${ZO_ROOT_USER_EMAIL:?ZO_ROOT_USER_EMAIL missing}"
PASS="${ZO_ROOT_USER_PASSWORD:?ZO_ROOT_USER_PASSWORD missing}"
OUT=/bootstrap/env
TMP=/bootstrap/.env.tmp
CURLRC=/tmp/oo-curlrc

log() { echo "bootstrap: $*" >&2; }

# Keep the root credentials out of argv (visible in `ps`): pass them via a curl config file.
umask 077
printf 'user = "%s:%s"\n' "$ROOT_USER" "$PASS" > "$CURLRC"

api() { # api METHOD PATH [BODY] -> prints body, then "HTTP:<status>" on the last line
  if [ $# -ge 3 ]; then
    curl -sS -K "$CURLRC" -X "$1" -H 'Content-Type: application/json' --data-binary "$3" -w '\nHTTP:%{http_code}' "$URL$2"
  else
    curl -sS -K "$CURLRC" -X "$1" -w '\nHTTP:%{http_code}' "$URL$2"
  fi
}
status_of() { printf '%s\n' "$1" | sed -n 's/^HTTP:\([0-9][0-9][0-9]\)$/\1/p' | tail -n 1; }
json_str() { # json_str KEY TEXT -> value of the first "KEY":"value" pair (values here never contain quotes)
  printf '%s' "$2" | tr -d '\n' | grep -o "\"$1\":\"[^\"]*\"" | head -n 1 | sed "s/^\"$1\":\"//; s/\"\$//"
}

# 1. Liveness (unauthenticated) - bounded wait, the healthcheck already gated us.
i=0
until [ "$(curl -s -o /dev/null -w '%{http_code}' "$URL/healthz")" = "200" ]; do
  i=$((i + 1)); [ "$i" -ge 60 ] && { log "healthz never returned 200"; exit 1; }
  sleep 1
done

# 2. Authenticated readiness + version.
r=$(api GET "/api/$ORG/config")
[ "$(status_of "$r")" = "200" ] || { log "authenticated /api/$ORG/config failed: $(status_of "$r")"; exit 1; }
VERSION=$(json_str version "$r")
[ -n "$VERSION" ] || { log "no version in /api/$ORG/config"; exit 1; }
log "server version $VERSION"

# 3. Service account (create, or rotate the token when it already exists).
#    Only the create/rotate responses carry the real token; GET /service_accounts returns a masked one.
r=$(api POST "/api/$ORG/service_accounts" "{\"email\":\"$SA\",\"first_name\":\"SDK\",\"last_name\":\"Tests\"}")
st=$(status_of "$r")
if [ "$st" != "200" ]; then
  enc=$(printf '%s' "$SA" | sed 's/@/%40/g')
  r=$(api PUT "/api/$ORG/service_accounts/$enc?rotateToken=true" '{}')
  st=$(status_of "$r")
  [ "$st" = "200" ] || { log "service account create/rotate failed: $st"; exit 1; }
  log "service account existed, token rotated"
else
  log "service account created"
fi
SA_TOKEN=$(json_str token "$r")
[ -n "$SA_TOKEN" ] || { log "no token in service account response"; exit 1; }

# 4. Seed the default log stream so listStreams/getStreamSchema have something on a fresh volume.
r=$(api POST "/api/$ORG/$STREAM/_json" '[{"message":"bootstrap seed","level":"info","source":"bootstrap"}]')
[ "$(status_of "$r")" = "200" ] || { log "seed ingest failed: $(status_of "$r")"; exit 1; }
case "$r" in *'"failed":0'*) ;; *) log "seed ingest reported failures"; exit 1 ;; esac

# 5. Write /bootstrap/env (KEY=value, no quotes; values contain no spaces or newlines).
{
  echo "GROW_OPENOBSERVE_URL=$URL"
  echo "OPENOBSERVE_URL=$URL"
  echo "OPENOBSERVE_ORG=$ORG"
  echo "OPENOBSERVE_USER=$ROOT_USER"
  echo "OPENOBSERVE_PASSWORD=$PASS"
  echo "OPENOBSERVE_STREAM=$STREAM"
  echo "OPENOBSERVE_TRACES_STREAM=$TSTREAM"
  echo "OPENOBSERVE_SA_EMAIL=$SA"
  echo "OPENOBSERVE_SA_TOKEN=$SA_TOKEN"
  echo "OPENOBSERVE_SERVER_VERSION=$VERSION"
} > "$TMP"
chmod 644 "$TMP"
mv "$TMP" "$OUT"
rm -f "$CURLRC"
log "wrote $OUT ($(wc -l < "$OUT") keys)"
