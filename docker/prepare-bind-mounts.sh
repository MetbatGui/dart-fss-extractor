#!/bin/sh
set -e

# Linux bind mount는 호스트의 UID/GID를 그대로 사용한다. cron/entrypoint는 root로
# 기동하므로 실제 payload(nonroot)가 DB 작업 사본 캐시·산출물·OAuth 토큰을 쓸 수 있게
# 마운트 디렉터리를 준비한다. DB 자체는 Drive SSOT라 여기 없다(db_ssot_guide.md).
for mount_path in /app/data /app/output /app/secrets; do
    mkdir -p "$mount_path"
    chown -R nonroot:nonroot "$mount_path"
done
