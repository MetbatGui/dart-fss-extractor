#!/bin/sh
set -e

/app/docker/prepare-bind-mounts.sh

# 권한 준비만 root로 수행하고, 실제 명령은 항상 최소 권한 계정으로 실행한다.
exec runuser -u nonroot -- "$@"
