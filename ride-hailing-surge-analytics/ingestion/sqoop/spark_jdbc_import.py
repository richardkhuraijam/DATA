"""PySpark JDBC import script (Backup for Sqoop).

Reads MySQL tables and writes Parquet files to the raw landing zone.
Supports Spark JDBC with auto-downloaded MySQL driver package or fallback via pandas.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("spark_jdbc_import")


def get_jdbc_url() -> str:
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = os.getenv("MYSQL_PORT", "3307")
    database = os.getenv("MYSQL_DATABASE", "ridehail")
    return f"jdbc:mysql://{host}:{port}/{database}"


def get_sqlalchemy_url() -> str:
    user = os.getenv("MYSQL_USER", "etl")
    password = os.getenv("MYSQL_PASSWORD", "change_me_etl")
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = os.getenv("MYSQL_PORT", "3307")
    database = os.getenv("MYSQL_DATABASE", "ridehail")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


def get_spark_session(app_name: str = "SparkJDBCImport") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0")
        .getOrCreate()
    )


def import_table_via_jdbc(
    spark: SparkSession, table_name: str, output_path: str, num_partitions: int = 4
) -> bool:
    jdbc_url = get_jdbc_url()
    user = os.getenv("MYSQL_USER", "etl")
    password = os.getenv("MYSQL_PASSWORD", "change_me_etl")

    try:
        logger.info("Reading table '%s' from MySQL via Spark JDBC...", table_name)
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        count = df.count()
        logger.info("Loaded %d rows for table '%s'. Writing Parquet to %s...", count, table_name, output_path)

        df.repartition(num_partitions).write.mode("overwrite").parquet(output_path)
        logger.info("Successfully exported '%s' to %s via Spark JDBC", table_name, output_path)
        return True
    except Exception as exc:
        logger.warning("Spark JDBC import for '%s' failed: %s. Trying pandas fallback...", table_name, exc)
        return False


def import_table_via_pandas(table_name: str, output_path: str) -> None:
    db_url = get_sqlalchemy_url()
    logger.info("Reading table '%s' via SQLAlchemy/Pandas...", table_name)
    engine = create_engine(db_url)
    df = pd.read_sql_table(table_name, con=engine)
    logger.info("Loaded %d rows for table '%s'. Writing Parquet to %s...", len(df), table_name, output_path)
    os.makedirs(output_path, exist_ok=True)
    df.to_parquet(os.path.join(output_path, f"{table_name}.parquet"), index=False)
    logger.info("Successfully exported '%s' to %s via Pandas fallback", table_name, output_path)


def main() -> None:
    raw_landing_base = os.getenv("RAW_LANDING_BASE", str(ROOT / "data" / "raw" / "mysql"))
    tables = ["trips", "drivers", "riders", "city_zones"]

    spark: Optional[SparkSession] = None
    try:
        spark = get_spark_session()
    except Exception as err:
        logger.warning("Could not initialize PySpark session with JDBC packages: %s", err)

    for table in tables:
        output_dir = os.path.join(raw_landing_base, table)
        success = False
        if spark is not None:
            success = import_table_via_jdbc(spark, table, output_dir)
        if not success:
            import_table_via_pandas(table, output_dir)

    if spark is not None:
        spark.stop()


if __name__ == "__main__":
    main()
