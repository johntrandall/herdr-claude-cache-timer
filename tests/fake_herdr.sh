#!/bin/sh
# Stands in for the herdr CLI: appends each call to $FAKE_HERDR_LOG, prints nothing.
printf '%s %s\n' "$(date +%s)" "$*" >> "$FAKE_HERDR_LOG"
