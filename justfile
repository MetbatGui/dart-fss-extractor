# 윈도우 파워쉘 셸 명시적 지정 (sh/bash 미인식 오류 해결)
set shell := ["powershell", "-Command"]

setup-release:
    git checkout master
    git remote add dart-fss-extractor https://github.com/guruta71/dart-fss-extractor.git

# Release to dart-fss-extractor
# Usage: just release
release:
    git checkout -B release master
    git push -u dart-fss-extractor release:master
    git checkout master