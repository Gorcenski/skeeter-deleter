#!/usr/bin/env bash
set -euo pipefail

: "${RUN_INTERVAL_MINUTES:?RUN_INTERVAL_MINUTES env var (minutes between runs) must be set}"

STALE_LIMIT_DAYS="${STALE_LIMIT_DAYS:-}"
MAX_REPOSTS="${MAX_REPOSTS:-}"
DOMAINS_TO_PROTECT="${DOMAINS_TO_PROTECT:-}"
FIXED_LIKES_CURSOR="${FIXED_LIKES_CURSOR:-}"
FIRST_RUN_DELAY_SECONDS="${FIRST_RUN_DELAY_SECONDS:-30}"
VERY_VERBOSE_LOGGING="${VERY_VERBOSE_LOGGING:-true}"

if ! [[ "$RUN_INTERVAL_MINUTES" =~ ^[0-9]+$ ]] || [ "$RUN_INTERVAL_MINUTES" -le 0 ]; then
  echo "RUN_INTERVAL_MINUTES must be a positive integer" >&2
  exit 1
fi

if [ -n "$STALE_LIMIT_DAYS" ]; then
  if ! [[ "$STALE_LIMIT_DAYS" =~ ^[0-9]+$ ]]; then
    echo "STALE_LIMIT_DAYS must be a non-negative integer when set" >&2
    exit 1
  fi
fi

if ! [[ "$FIRST_RUN_DELAY_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "FIRST_RUN_DELAY_SECONDS must be a non-negative integer" >&2
  exit 1
fi

case "${VERY_VERBOSE_LOGGING,,}" in
  true|false)
    ;;
  *)
    echo "VERY_VERBOSE_LOGGING must be 'true' or 'false'" >&2
    exit 1
    ;;
esac

if [ "${VERY_VERBOSE_LOGGING,,}" = "true" ]; then
  LOG_LEVEL_FLAG="-vv"
else
  LOG_LEVEL_FLAG="-v"
fi

stop_requested=false
current_child_pid=""

RUN_INTERVAL_SECONDS=$((RUN_INTERVAL_MINUTES * 60))

CMD=(python skeeter_deleter.py "$LOG_LEVEL_FLAG" -y)

if [ -n "$STALE_LIMIT_DAYS" ]; then
  CMD+=(-s "$STALE_LIMIT_DAYS")
fi

if [ -n "$MAX_REPOSTS" ]; then
  if ! [[ "$MAX_REPOSTS" =~ ^[0-9]+$ ]]; then
    echo "MAX_REPOSTS must be a non-negative integer when set" >&2
    exit 1
  fi
  CMD+=(-l "$MAX_REPOSTS")
fi

if [ -n "$DOMAINS_TO_PROTECT" ]; then
  CMD+=(-d "$DOMAINS_TO_PROTECT")
fi

if [ -n "$FIXED_LIKES_CURSOR" ]; then
  CMD+=(-c "$FIXED_LIKES_CURSOR")
fi

print_header() {
  local border="============================================================"
  local username_display="${BLUESKY_USERNAME:-<not set>}"
  local password_display="[not set]"
  local interval_display="${RUN_INTERVAL_MINUTES:-<not set>}"
  local stale_display="${STALE_LIMIT_DAYS:-<not set>}"
  local repost_display="${MAX_REPOSTS:-<not set>}"
  local domains_display="${DOMAINS_TO_PROTECT:-<not set>}"
  local cursor_display="${FIXED_LIKES_CURSOR:-<not set>}"
  local delay_display="${FIRST_RUN_DELAY_SECONDS:-<not set>}"
  local verbose_display="${VERY_VERBOSE_LOGGING:-<not set>}"

  if [ -n "${BLUESKY_PASSWORD:-}" ]; then
    password_display="[set]"
  fi

  echo "$border"
  echo " Skeeter Deleter Scheduler"
  echo " License: MIT (c) Skeeter Deleter contributors"
  echo " Credit: https://github.com/Gorcenski (origin) | Form: https://github.com/nomad-geek"
  echo "$border"
  echo " Summary: Downloads Bluesky archives, prunes stale or viral posts, and repeats on a schedule."
  echo
  printf "  %-25s | %s\n" "Environment Variable" "Value"
  printf "  %-25s | %s\n" "--------------------" "----------------------"
  printf "  %-25s | %s\n" "BLUESKY_USERNAME" "$username_display"
  printf "  %-25s | %s\n" "BLUESKY_PASSWORD" "$password_display"
  printf "  %-25s | %s\n" "RUN_INTERVAL_MINUTES" "$interval_display"
  printf "  %-25s | %s\n" "FIRST_RUN_DELAY_SECONDS" "$delay_display"
  printf "  %-25s | %s\n" "VERY_VERBOSE_LOGGING" "$verbose_display"
  printf "  %-25s | %s\n" "STALE_LIMIT_DAYS" "$stale_display"
  printf "  %-25s | %s\n" "MAX_REPOSTS" "$repost_display"
  printf "  %-25s | %s\n" "DOMAINS_TO_PROTECT" "$domains_display"
  printf "  %-25s | %s\n" "FIXED_LIKES_CURSOR" "$cursor_display"
  echo "$border"
}

timestamp() {
  date -u +"%Y-%m-%d %H:%M:%S UTC"
}

handle_auth_factor_error() {
  echo "[$(timestamp)] Bluesky rejected the login with AuthFactorTokenRequired."
  cat <<'EOF'
Action required: Bluesky requires 2-factor Authentication for this account.
We must use an App Password instead of your user password.

How to fix:
  1. Sign in to https://bsky.app/settings/app-passwords
  2. Create a new App Password.
  3. Update BLUESKY_PASSWORD in your .env file with the new App Password
  4. Rebuild/restart the container

Do not use this App Password for anything else.
Revoke it in the future if you no longer use this container.
The scheduler will now exit so you can update credentials.
EOF
  exit 1
}

announce_next_run() {
  local next_run
  next_run=$(date -u -d "+${RUN_INTERVAL_SECONDS} seconds" +"%Y-%m-%d %H:%M:%S UTC")
  echo "[$(timestamp)] Next run scheduled for ${next_run}"
}

handle_signal() {
  local signal="$1"
  if [ "$stop_requested" = false ]; then
    stop_requested=true
    echo "[$(timestamp)] Received ${signal}. Shutting down scheduler..."
  fi
  if [ -n "$current_child_pid" ]; then
    kill -s "$signal" "$current_child_pid" 2>/dev/null || true
  fi
}

interruptible_sleep() {
  local duration="$1"
  if [ "$duration" -le 0 ]; then
    return 0
  fi

  local remaining="$duration"
  while [ "$remaining" -gt 0 ] && [ "$stop_requested" = false ]; do
    local chunk=$((remaining > 5 ? 5 : remaining))
    if ! sleep "$chunk"; then
      break
    fi
    remaining=$((remaining - chunk))
  done
}

trap 'handle_signal TERM' TERM
trap 'handle_signal INT' INT
trap 'handle_signal QUIT' QUIT

print_header

echo "[$(timestamp)] Skeeter Deleter scheduler starting. Interval: ${RUN_INTERVAL_MINUTES} minute(s)."
echo "[$(timestamp)] Waiting ${FIRST_RUN_DELAY_SECONDS} seconds before first run..."
interruptible_sleep "$FIRST_RUN_DELAY_SECONDS"

if [ "$stop_requested" = true ]; then
  echo "[$(timestamp)] Stop requested before first run; exiting."
  exit 0
fi

while true; do
  if [ "$stop_requested" = true ]; then
    break
  fi

  echo "[$(timestamp)] Starting skeeter-deleter.py run..."
  log_capture=$(mktemp)
  "${CMD[@]}" > >(tee "$log_capture") 2>&1 &
  current_child_pid=$!

  if wait "$current_child_pid"; then
    echo "[$(timestamp)] skeeter-deleter.py run completed."
  else
    if [ "$stop_requested" = true ]; then
      echo "[$(timestamp)] skeeter-deleter.py run interrupted; exiting."
    else
      if grep -q "AuthFactorTokenRequired" "$log_capture"; then
        rm -f "$log_capture"
        handle_auth_factor_error
      fi
      echo "[$(timestamp)] skeeter-deleter.py run failed; scheduler will retry after the next interval." >&2
    fi
  fi

  rm -f "$log_capture"
  current_child_pid=""

  if [ "$stop_requested" = true ]; then
    break
  fi

  announce_next_run
  interruptible_sleep "$RUN_INTERVAL_SECONDS"
done

echo "[$(timestamp)] Scheduler shutdown complete."
