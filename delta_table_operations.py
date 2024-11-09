# Create sample test data

def create_test_data():
    # Create a simple dataset
    data = [(1, "John", 25, "2024-01-01"),
            (2, "Jane", 30, "2024-01-02"),
            (3, "Bob", 35, "2024-01-03")]
    
    df = spark.createDataFrame(data, ["id", "name", "age", "date"])
    
    # Write as delta table
    delta_path = "Tables/test_delta_operations"
    df.write.format("delta").mode("overwrite").save(delta_path)
    print("Initial data written")
    
    return delta_path

def test_operations(delta_path):
    """Test different Delta operations"""
    
    # 1. WRITE (Append) Operation
    new_data = [(4, "Alice", 28, "2024-01-04"),
                (5, "Charlie", 32, "2024-01-05")]
    append_df = spark.createDataFrame(new_data, ["id", "name", "age", "date"])
    append_df.write.format("delta").mode("append").save(delta_path)
    print("WRITE (Append) completed")

    # 2. UPDATE Operation
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, delta_path)
    
    # Update age for id = 1
    delta_table.update(
        condition = "id = 1",
        set = { "age": "26" }
    )
    print("UPDATE completed")

    # 3. DELETE Operation
    delta_table.delete("id = 5")
    print("DELETE completed")

    # 4. MERGE Operation
    merge_data = [(1, "John", 27, "2024-01-01"),  # Update
                  (6, "David", 40, "2024-01-06")]  # Insert
    merge_df = spark.createDataFrame(merge_data, ["id", "name", "age", "date"])
    
    delta_table.alias("target").merge(
        merge_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(
        set = {
            "age": "source.age"
        }
    ).whenNotMatchedInsert(
        values = {
            "id": "source.id",
            "name": "source.name",
            "age": "source.age",
            "date": "source.date"
        }
    ).execute()
    print("MERGE completed")

    # 5. OPTIMIZE Operation
    delta_table.optimize().executeCompaction()
    print("OPTIMIZE completed")

    # 6. VACUUM Operation (setting retention to 0 hours for testing)
    delta_table.vacuum(168)
    print("VACUUM completed")

    # 7. Schema Evolution (ALTER TABLE)
    new_col_data = [(1, "John", 27, "2024-01-01", "USA"),
                    (2, "Jane", 30, "2024-01-02", "UK")]
    schema_df = spark.createDataFrame(new_col_data, 
                                    ["id", "name", "age", "date", "country"])
    
    schema_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_path)
    print("Schema Evolution completed")

    # Display history
    print("\nOperation History:")
    delta_table = DeltaTable.forPath(spark, delta_path)
    display(spark.sql(f"DESCRIBE HISTORY '{delta_path}'"))

# Execute the tests
delta_path = create_test_data()
test_operations(delta_path)

# Query current state
print("\nCurrent table state:")
display(spark.read.format("delta").load(delta_path))

# Check operation history
history_df = spark.sql(f"DESCRIBE HISTORY '{delta_path}'")

# Get counts per operation type
operation_counts = history_df.groupBy("operation").count().show()

# Check timestamps of operations
operation_times = (history_df
                  .select("timestamp", "operation")
                  .orderBy("timestamp", ascending=False)
                  .show())

# Check specific operation details
write_ops = (history_df
             .filter("operation = 'WRITE'")
             .select("timestamp", "operationMetrics")
             .show(truncate=False))
