#!/bin/sh
set -e

/app/docker/prepare-bind-mounts.sh
exec cron -f
