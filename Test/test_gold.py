from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

DATABASE = "azuredb_cen.gold"


# ============================================================
# 1. GOLD TABLE EXISTENCE
# ============================================================

def test_dim_feeder_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.dim_feeder"
    )


# ============================================================
# 2. GOLD TABLE EXISTENCE
# ============================================================

def test_dim_household_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.dim_household"
    )


# ============================================================
# 3. GOLD TABLE EXISTENCE
# ============================================================

def test_dim_operator_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.dim_operator"
    )


# ============================================================
# 4. GOLD TABLE EXISTENCE
# ============================================================

def test_dim_substation_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.dim_substation"
    )


# ============================================================
# 5. GOLD TABLE EXISTENCE
# ============================================================

def test_dim_zone_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.dim_zone"
    )


# ============================================================
# 6. FACT TABLE EXISTENCE
# ============================================================

def test_fact_grid_load_exists():
    assert spark.catalog.tableExists(
        f"{DATABASE}.fact_grid_load"
    )


# ============================================================
# 7. GOLD TABLES SHOULD NOT BE EMPTY
# ============================================================

def test_gold_tables_not_empty():

    tables = [
        "dim_feeder",
        "dim_household",
        "dim_operator",
        "dim_substation",
        "dim_zone",
        "fact_grid_load"
    ]

    for table in tables:

        df = spark.table(
            f"{DATABASE}.{table}"
        )

        assert df.count() > 0, \
            f"{table} is empty"


# ============================================================
# 8. FACT TABLE REQUIRED COLUMNS
# ============================================================

def test_fact_grid_load_columns():

    df = spark.table(
        f"{DATABASE}.fact_grid_load"
    )

    expected_columns = {
        "household",
        "feeder",
        "substation",
        "zone"
    }

    actual_columns = set(df.columns)

    assert expected_columns.issubset(actual_columns)


# ============================================================
# 9. FACT TABLE SHOULD NOT HAVE DUPLICATES
# ============================================================

def test_fact_grid_load_duplicates():

    df = spark.table(
        f"{DATABASE}.fact_grid_load"
    )

    total_count = df.count()

    distinct_count = df.dropDuplicates().count()

    assert total_count == distinct_count


# ============================================================
# 10. FACT TABLE SHOULD NOT HAVE NULL VALUES
# ============================================================

def test_fact_grid_load_no_nulls():

    df = spark.table(
        f"{DATABASE}.fact_grid_load"
    )

    required_columns = [
        "household",
        "feeder",
        "substation",
        "zone"
    ]

    for column in required_columns:

        null_count = df.filter(
            df[column].isNull()
        ).count()

        assert null_count == 0, \
            f"{column} contains {null_count} NULL values"