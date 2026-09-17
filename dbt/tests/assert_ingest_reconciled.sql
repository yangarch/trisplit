{#
  수집량 대비 적재량 — 실무에서 하던 검증의 dbt 이식.

  소스에서 읽은 건수와 테이블에 들어간 행 수가 어긋나면 실패한다.
  파싱 실패로 파일 하나가 통째로 날아가거나, MERGE 가 중복 삽입하거나,
  필터가 의도보다 많이 걸러내면 여기서 잡힌다.

  실무(Airflow + HDFS) 와의 차이:
    · 예전에는 이 비교가 적재 태스크 **안에** 있었다. 적재 코드와 검증 코드가 섞여 있어서
      검증만 따로 돌릴 수 없었고, 실패해도 "그 태스크가 죽었다" 로만 남았다.
    · dbt test 는 검증이 적재와 분리된 선언이라 `dbt test` 만 따로 돌릴 수 있고,
      무엇이 왜 실패했는지가 테스트 이름으로 남는다.
    · 대신 **수집량을 기록하는 일은 여전히 적재 쪽 책임**이다 (scripts/ingest_audit.py).
      dbt 는 테이블 밖을 못 보기 때문에, 감사 테이블이 없으면 이 검증 자체가 불가능하다.
      "dbt 로 옮기면 공짜로 된다" 가 아니라, 경계가 바뀌는 것이다.
#}

-- ⚠️ 전역 max(run_ts) 로 잡으면 안 된다.
--    적재 스크립트가 셋으로 나뉘어 있어 run_ts 가 스크립트마다 다르다 —
--    전역 최댓값을 쓰면 **마지막에 돈 스크립트의 테이블 하나만** 검사하고
--    나머지 넷은 통과한 척한다. (실제로 그렇게 짰다가 Airflow 실행 후에 발견했다)
--    테이블별 최신 적재를 각각 봐야 한다.
with latest_per_table as (
    select
        table_name,
        source_kind,
        source_count,
        loaded_count,
        row_number() over (partition by table_name order by run_ts desc) as rn
    from {{ source('audit', 'ingest_runs') }}
)

select
    table_name,
    source_kind,
    source_count,
    loaded_count,
    loaded_count - source_count as delta
from latest_per_table
where rn = 1
  and loaded_count != source_count
