# Airflow + Spark 실행 환경.
#
# 태스크가 LocalExecutor 로 스케줄러 컨테이너 안에서 돌기 때문에, 그 안에 JRE 와 PySpark 가
# 있어야 한다. 호스트의 .venv 는 macOS/arm64 바이너리라 마운트해도 쓸 수 없다.
FROM apache/airflow:3.3.1

USER root
# Spark 4.x 는 Java 17+ 가 필요하다. default-jre 는 배포판에 따라 17 또는 21 인데 둘 다 된다.
# /usr/lib/jvm/default-java 심볼릭 링크가 걸리므로 JAVA_HOME 을 버전에 묶지 않는다.
# procps 는 Spark 가 메모리 조회에 쓴다 (없으면 경고가 계속 뜬다).
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
# Ivy 캐시 디렉토리를 이미지 안에 미리 만든다.
# 이게 없으면 named volume 이 root 소유 빈 디렉토리로 생성되고,
# uid 50000 인 컨테이너가 하위 폴더를 못 만들어 spark-submit 이 죽는다
# (FileNotFoundException: .../cache/resolved-...xml → JAVA_GATEWAY_EXITED).
# 도커는 첫 마운트 때 이미지 쪽 내용과 소유권을 볼륨에 복사하므로, 여기서 만들어두면 해결된다.
RUN mkdir -p /home/airflow/.ivy2.5.2/cache /home/airflow/.ivy2.5.2/jars \
 && mkdir -p /opt/airflow/auth

# 호스트 requirements.txt 와 같은 버전으로 고정한다 —
# 컨테이너와 호스트가 다른 Spark 를 쓰면 같은 웨어하우스를 다르게 읽을 수 있다.
RUN pip install --no-cache-dir \
      pyspark==4.1.3 \
      dbt-core==1.12.5 \
      "dbt-spark[session]" \
      pandas pyarrow
