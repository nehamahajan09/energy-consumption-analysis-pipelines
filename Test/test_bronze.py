from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

DATABASE = "azuredb_cen.bronze"


# ============================================================
# 1. DEVICE TABLE EXISTENCE
# ============================================================

def test_device_table_exists():

    assert spark.catalog.tableExists(
        f"{DATABASE}.device_metrics_stream"
    )


# ============================================================
# 2. ENERGY TABLE EXISTENCE
# ============================================================

def test_energy_table_exists():

    assert spark.catalog.tableExists(
        f"{DATABASE}.energy_usage_stream"
    )


# ============================================================
# 3. GRID TABLE EXISTENCE
# ============================================================

def test_grid_table_exists():

    assert spark.catalog.tableExists(
        f"{DATABASE}.grid_load_stream"
    )


# ============================================================
# 4. TARIFF TABLE EXISTENCE
# ============================================================

def test_tariff_table_exists():

    assert spark.catalog.tableExists(
        f"{DATABASE}.tariff_metrics_stream_v2"
    )


# ============================================================
# 5. WEATHER TABLE EXISTENCE
# ============================================================

def test_weather_table_exists():

    assert spark.catalog.tableExists(
        f"{DATABASE}.weather_source_v2"
    )


# ============================================================
# 6. BRONZE TABLES SHOULD NOT BE EMPTY
# ============================================================

def test_bronze_tables_not_empty():

    tables = [
        "device_metrics_stream",
        "energy_usage_stream",
        "grid_load_stream",
        "tariff_metrics_stream_v2",
        "weather_source_v2"
    ]

    for table in tables:

        df = spark.table(
            f"{DATABASE}.{table}"
        )

        assert df.count() > 0, \
            f"{table} is empty"


# ============================================================
# 7. DEVICE HOUSEHOLD_ID SHOULD NOT BE NULL
# ============================================================

def test_device_household_id_not_null():

    df = spark.table(
        f"{DATABASE}.device_metrics_stream"
    )

    null_count = df.filter(
        df["household_id"].isNull()
    ).count()

    assert null_count == 0, \
        f"Device household_id contains {null_count} NULL values"



# ============================================================
# 8. GRID HOUSEHOLD_ID SHOULD NOT BE NULL
# ============================================================

def test_grid_household_id_not_null():

    df = spark.table(
        f"{DATABASE}.grid_load_stream"
    )

    null_count = df.filter(
        df["household_id"].isNull()
    ).count()

    assert null_count == 0, \
        f"Grid household_id contains {null_count} NULL values"


# ============================================================
# 9. TARIFF AND WEATHER HOUSEHOLD_ID SHOULD NOT BE NULL
# ============================================================

def test_tariff_weather_household_id_not_null():

    tables = [
        "tariff_metrics_stream_v2",
        "weather_source_v2"
    ]

    for table in tables:

        df = spark.table(
            f"{DATABASE}.{table}"
        )

        null_count = df.filter(
            df["household_id"].isNull()
        ).count()

        assert null_count == 0, \
            f"{table}.household_id contains {null_count} NULL values"


# ============================================================
# LIST OF ALL 10 TEST CASES
# ============================================================

test_functions = [

    test_device_table_exists,
    test_energy_table_exists,
    test_grid_table_exists,
    test_tariff_table_exists,
    test_weather_table_exists,

    test_bronze_tables_not_empty,

    test_device_household_id_not_null,
    test_grid_household_id_not_null,
    test_tariff_weather_household_id_not_null
]


# ============================================================
# RUN ALL TESTS
# ============================================================

passed = 0
failed = 0

print("\n")
print("=" * 70)
print("ENERGY FORECAST PROJECT")
print("BRONZE LAYER - PYTEST DATA QUALITY TESTING")
print("=" * 70)


for test in test_functions:

    try:

        test()

        print(f"PASSED : {test.__name__}")

        passed += 1

    except AssertionError as e:

        print(f"FAILED : {test.__name__}")
        print(f"         {e}")

        failed += 1

    except Exception as e:

        print(f"ERROR  : {test.__name__}")
        print(f"         {e}")

        failed += 1


# ============================================================
# ROW COUNT REPORT
# ============================================================

print("\n")
print("-" * 70)
print("BRONZE TABLE ROW COUNTS")
print("-" * 70)


tables = {
    "Device": "device_metrics_stream",
    "Energy": "energy_usage_stream",
    "Grid": "grid_load_stream",
    "Tariff": "tariff_metrics_stream_v2",
    "Weather": "weather_source_v2"
}


for name, table in tables.items():

    try:

        count = spark.table(
            f"{DATABASE}.{table}"
        ).count()

        print(f"{name:10s} : {count:,} rows")

    except Exception:

        print(f"{name:10s} : ERROR")


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n")
print("=" * 70)
print("BRONZE LAYER DATA QUALITY TEST SUMMARY")
print("=" * 70)

print(f"Total Test Cases : {len(test_functions)}")
print(f"Passed            : {passed}")
print(f"Failed            : {failed}")

print("=" * 70)


if failed == 0:

    print("ALL 10 BRONZE TEST CASES PASSED ✅")

else:

    print("BRONZE DATA QUALITY TESTS FAILED ❌")

print("=" * 70)
