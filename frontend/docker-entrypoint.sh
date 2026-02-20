#!/bin/sh
set -eu

if [ -z "${BASIC_AUTH_USER:-}" ] || [ -z "${BASIC_AUTH_PASS:-}" ]; then
  echo "ERROR: Defina BASIC_AUTH_USER e BASIC_AUTH_PASS para habilitar Basic Auth."
  exit 1
fi

htpasswd -bc /etc/nginx/.htpasswd "$BASIC_AUTH_USER" "$BASIC_AUTH_PASS"
exec nginx -g "daemon off;"
