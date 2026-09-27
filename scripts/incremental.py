"""
증분 적재 — 소스 파일 단위로 신규/변경만 다시 읽는다.

왜 필요한가:
  처음 구현은 매 실행마다 `createOrReplace()` 로 전체를 다시 적재했다.
  맥북(M4 10코어)에서는 파이프라인 전체가 106초라 티가 안 났지만,
  **GPX 622개를 매일 밤 다시 XML 파싱해서 184만 포인트를 재적재**하고 있었다.
  파일이 늘수록 무한정 길어지고, 서버(2014 i5 4스레드)로 옮기면 런타임을 지배한다.
  신규는 하루 1~2개인데 622개를 다시 파싱할 이유가 없다.

판정 기준 — (경로, mtime) 쌍:
  신규      테이블에 없는 파일            → 파싱
  변경      mtime 이 다른 파일            → 기존 행 삭제 후 재파싱
  변경없음  경로·mtime 동일               → 건너뜀
  삭제      테이블에만 있고 디스크에 없음 → 기존 행 삭제

  mtime 을 쓰는 이유: 파일 내용 해시는 전체를 읽어야 하고, 크기만 보면
  같은 길이로 바뀐 경우를 놓친다. Strava 재수집은 파일을 새로 쓰므로 mtime 이 움직인다.

수집량 감사와의 관계:
  증분으로 바꿔도 감사 테이블의 `source_count` 는 **전체 기대값**이어야 한다
  (dbt test 가 테이블 총 행수와 비교하므로). 그래서 적재는 증분으로 하되,
  기대값은 매 실행 소스 전체를 독립적으로 세어 구한다 — XML 파싱이 아니라
  바이트 스캔이라 622파일 423MB 에 맥북 0.5초 / 서버 추정 2~4초로 싸다.
  "적재를 건너뛴 파일의 수까지 포함해 맞는지" 를 검증하는 의미가 유지된다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession


def to_uri(p: Path) -> str:
    """Spark 의 input_file_name() 이 돌려주는 형식에 맞춘다."""
    return f"file://{p.resolve()}"


@dataclass
class Plan:
    to_parse: list[Path] = field(default_factory=list)   # 신규 + 변경
    to_delete: list[str] = field(default_factory=list)   # 변경 + 삭제 (URI)
    n_new: int = 0
    n_changed: int = 0
    n_unchanged: int = 0
    n_deleted: int = 0
    table_exists: bool = False

    @property
    def nothing_to_do(self) -> bool:
        return not self.to_parse and not self.to_delete

    def describe(self) -> str:
        return (
            f"신규 {self.n_new} / 변경 {self.n_changed} / "
            f"변경없음 {self.n_unchanged} / 삭제 {self.n_deleted}"
        )


def plan_incremental(
    spark: SparkSession,
    table: str,
    source_files: list[Path],
    *,
    full_refresh: bool = False,
) -> Plan:
    """디스크 상태와 테이블에 기록된 (경로, mtime) 을 비교해 할 일을 정한다."""
    disk: dict[str, tuple[Path, float]] = {
        to_uri(p): (p, p.stat().st_mtime) for p in source_files
    }

    exists = spark.catalog.tableExists(table)
    if full_refresh or not exists:
        return Plan(
            to_parse=[p for p, _ in disk.values()],
            n_new=len(disk),
            table_exists=exists,
        )

    loaded: dict[str, float] = {
        r["_source_file"]: r["m"]
        for r in spark.sql(
            f"SELECT _source_file, max(_source_mtime) AS m FROM {table} GROUP BY _source_file"
        ).collect()
    }

    plan = Plan(table_exists=True)
    for uri, (path, mtime) in disk.items():
        if uri not in loaded:
            plan.to_parse.append(path)
            plan.n_new += 1
        elif loaded[uri] != mtime:
            plan.to_parse.append(path)
            plan.to_delete.append(uri)
            plan.n_changed += 1
        else:
            plan.n_unchanged += 1

    for uri in loaded:
        if uri not in disk:
            plan.to_delete.append(uri)
            plan.n_deleted += 1

    return plan


def delete_rows(spark: SparkSession, table: str, uris: list[str], *, chunk: int = 200) -> int:
    """변경·삭제된 소스의 기존 행을 지운다. IN 절이 너무 길어지지 않게 나눠 실행."""
    if not uris:
        return 0
    for i in range(0, len(uris), chunk):
        batch = uris[i : i + chunk]
        lit = ", ".join("'" + u.replace("'", "''") + "'" for u in batch)
        spark.sql(f"DELETE FROM {table} WHERE _source_file IN ({lit})")
    return len(uris)


def count_bytes_occurrences(files: list[Path], needle: bytes) -> int:
    """소스에서 기대 행수를 독립적으로 센다 (XML 파싱 없이 바이트 스캔)."""
    total = 0
    for f in files:
        with open(f, "rb") as fh:
            total += fh.read().count(needle)
    return total
