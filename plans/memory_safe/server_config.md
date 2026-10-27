# Spark Server Configuration for Large Table Exports

> Historical note (2025-09): this plan is for the original Python sparkopy. The Go rewrite streams Arrow batches straight to Parquet and does not need a large driver memory.

## Problem
The `TaskResultLost` error occurs because the Spark driver has insufficient memory to collect large tables via `toPandas()`.

## Solution: Increase Driver Memory on Server

If the driver node has a large amount of RAM, you can configure Spark Connect server with much higher memory limits.

### Option 1: Update spark-defaults.conf
Edit `$SPARK_HOME/conf/spark-defaults.conf` on spark.example.com:

```properties
# Increase driver memory (adjust based on your needs)
spark.driver.memory=100g

# Increase max result size (should be ~50% of driver memory)
spark.driver.maxResultSize=50g

# Enable Arrow optimization for efficient data transfer
spark.sql.execution.arrow.pyspark.enabled=true
spark.sql.execution.arrow.maxRecordsPerBatch=10000

# Optional: Tune garbage collection for large heap
spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+UseCompressedOops
```

### Option 2: Start Spark Connect with Configuration
When starting the Spark Connect server:

```bash
$SPARK_HOME/sbin/start-connect-server.sh \
  --packages org.apache.spark:spark-connect_2.12:3.5.0 \
  --conf spark.driver.memory=100g \
  --conf spark.driver.maxResultSize=50g \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.sql.execution.arrow.maxRecordsPerBatch=10000 \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:+UseCompressedOops"
```

### Option 3: Environment Variables
Set before starting Spark Connect:

```bash
export SPARK_DRIVER_MEMORY=100g
export SPARK_DAEMON_MEMORY=100g
```

## Memory Sizing Guidelines

| Table Size | Recommended driver.memory | Recommended maxResultSize |
|------------|---------------------------|---------------------------|
| < 10 GB    | 20g                       | 10g                       |
| 10-50 GB   | 100g                      | 50g                       |
| 50-200 GB  | 400g                      | 200g                      |
| 200-500 GB | 800g                      | 400g                      |
| > 500 GB   | 1200g                     | 600g                      |

## Verification
After restarting Spark Connect server, verify settings:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://spark.example.com:15002") \
    .getOrCreate()

# Check configuration
print(spark.conf.get("spark.driver.memory"))
print(spark.conf.get("spark.driver.maxResultSize"))
```

## Benefits
- No script changes needed
- Original sparkopy will work for large tables
- Single configuration point for all users
- Utilizes available server resources

## Note
The configurations in the sparkopy script (`spark.conf.set()`) don't actually increase driver memory when called after session creation. They need to be set server-side or during session initialization.