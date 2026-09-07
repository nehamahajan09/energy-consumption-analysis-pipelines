import pytest
from pyspark.sql import SparkSession


# ============================================================
# Spark Session
# ============================================================

spark = SparkSession.builder.getOrCreate()


# ============================================================
# Database and Tables
# ============================================================

DATABASE = "azuredb_cen.silver"

SILVER_TABLES = [
    "silver_device",
    "silver_energy_metrics",
    "silver_grid",
    "silver_tariff",
    "silver_weather"
]


# ============================================================
# 1-5. SILVER TABLE EXISTENCE TESTS
# ============================================================

def test_silver_device_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.silver_device"
    )


def test_silver_energy_metrics_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.silver_energy_metrics"
    )


def test_silver_grid_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.silver_grid"
    )


def test_silver_tariff_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.silver_tariff"
    )


def test_silver_weather_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.silver_weather"
    )


# ============================================================
# 6-10. SILVER TABLE NOT EMPTY TESTS
# ============================================================

def test_silver_device_not_empty():
    df = spark.table(f"{DATABASE}.silver_device")

    assert df.count() > 0


def test_silver_energy_metrics_not_empty():
    df = spark.table(f"{DATABASE}.silver_energy_metrics")

    assert df.count() > 0


def test_silver_grid_not_empty():
    df = spark.table(f"{DATABASE}.silver_grid")

    assert df.count() > 0


def test_silver_tariff_not_empty():
    df = spark.table(f"{DATABASE}.silver_tariff")

    assert df.count() > 0


def test_silver_weather_not_empty():
    df = spark.table(f"{DATABASE}.silver_weather")

    assert df.count() > 0


# ============================================================
# 11. SILVER DEVICE COLUMN VALIDATION
# ============================================================

def test_silver_device_columns():

    df = spark.table(f"{DATABASE}.silver_device")

    expected_columns = {
        "device_category",
        "device_brand",
        "device_model",
        "maintenance_status",
        "installation_region",
        "runtime_hours",
        "device_power_kw",
        "motor_speed_rpm",
        "efficiency_ratio",
        "energy_draw_kwh",
        "heat_output",
        "cooling_load",
        "device_voltage",
        "device_current",
        "device_temperature"
    }

    actual_columns = set(df.columns)

    assert expected_columns.issubset(actual_columns)


# ============================================================
# 12. ENERGY DRAW NULL VALIDATION
# ============================================================

def test_device_energy_not_null():

    df = spark.table(f"{DATABASE}.silver_device")

    invalid = df.filter(
        df.energy_draw_kwh.isNull()
    ).count()

    assert invalid == 0


# ============================================================
# 13. DEVICE POWER NULL VALIDATION
# ============================================================

def test_device_power_not_null():

    df = spark.table(f"{DATABASE}.silver_device")

    invalid = df.filter(
        df.device_power_kw.isNull()
    ).count()

    assert invalid == 0


# ============================================================
# 14. ENERGY DRAW NEGATIVE VALIDATION
# ============================================================

def test_device_energy_not_negative():

    df = spark.table(f"{DATABASE}.silver_device")

    invalid = df.filter(
        df.energy_draw_kwh < 0
    ).count()

    assert invalid == 0


# ============================================================
# 15. DEVICE POWER NEGATIVE VALIDATION
# ============================================================

def test_device_power_not_negative():

    df = spark.table(f"{DATABASE}.silver_device")

    invalid = df.filter(
        df.device_power_kw < 0
    ).count()

    assert invalid == 0


# ============================================================
# 16. RUNTIME VALIDATION
# ============================================================

def test_runtime_not_negative():

    df = spark.table(f"{DATABASE}.silver_device")

    invalid = df.filter(
        df.runtime_hours < 0
    ).count()

    assert invalid == 0


# ============================================================
# 17. EFFICIENCY VALIDATION
# ============================================================

def test_efficiency_range():

    df = spark.table(f"{DATABASE}.silver_device")

    invalid = df.filter(
        (df.efficiency_ratio < 0) |
        (df.efficiency_ratio > 100)
    ).count()

    assert invalid == 0


# ============================================================
# 18. DUPLICATE RECORD VALIDATION
# ============================================================

def test_device_duplicates():

    df = spark.table(f"{DATABASE}.silver_device")

    total_count = df.count()

    distinct_count = df.dropDuplicates().count()

    assert total_count == distinct_count