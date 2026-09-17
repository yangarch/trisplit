# Training Companion Lakehouse 환경 변수
#   사용법:  source lakehouse/env.sh
#
# Java 21 은 brew 로 설치돼 있으나 PATH 에 링크되지 않아 JAVA_HOME 을 직접 지정한다.
# (`brew install openjdk@21` 은 시스템 JVM 디렉토리에 심볼릭 링크를 걸지 않음)

export JAVA_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

LAKEHOUSE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export LAKEHOUSE_DIR
export TC_ROOT="$(dirname "$LAKEHOUSE_DIR")"
export TC_WAREHOUSE="$LAKEHOUSE_DIR/warehouse"

# Spark 가 드라이버/워커 양쪽에서 venv 의 python 을 쓰도록 고정
export PYSPARK_PYTHON="$LAKEHOUSE_DIR/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

# dbt-spark(session) 는 SparkSession.builder.getOrCreate() 로 JVM 을 띄운다.
# 드라이버 힙은 JVM 기동 전에 정해져야 하므로 여기서 함께 내보낸다 (scripts/spark_session.py 와 동일 이유).
export TC_DRIVER_MEMORY="${TC_DRIVER_MEMORY:-8g}"
export PYSPARK_SUBMIT_ARGS="--driver-memory $TC_DRIVER_MEMORY pyspark-shell"

# dbt 가 프로젝트 안의 profiles.yml 을 쓰게 한다 (~/.dbt 에 두지 않음)
export DBT_PROFILES_DIR="$LAKEHOUSE_DIR/dbt"

echo "[env] JAVA_HOME     = $JAVA_HOME"
echo "[env] TC_ROOT       = $TC_ROOT"
echo "[env] TC_WAREHOUSE  = $TC_WAREHOUSE"
