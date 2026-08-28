#!/bin/sh
set -e
cd /app

# cron은 자체 PATH(/usr/bin:/bin)로만 잡을 실행해 이미지 ENV PATH를 물려받지
# 않으므로 venv python을 절대경로로 호출한다(docker_guide.md §4). .env는
# load_dotenv()가 코드에서 직접 읽으므로 별도 env 상속 트릭은 불필요.
exec su -s /bin/sh -c '/app/.venv/bin/python src/daily_scheduler.py' nonroot
