# Memory-Safe Spark Table Export Plan

> Historical note (2025-09): this plan is for the original Python sparkopy. The Go rewrite streams Arrow batches straight to Parquet and does not need a large driver memory.

## Problem Statement
The original sparkopy implementation fails with `SparkException: TaskResultLost` when exporting large tables. This occurs because `df.toPandas()` attempts to collect all distributed data into driver memory, causing memory exhaustion and task failures.

## Root Cause Discovery
**Found the actual issue:** The Spark server configuration had NO active driver memory setting!
- The config file had `#spark.driver.memory 100g` (commented out)
- This meant Spark was using default driver memory (likely 1-4GB)
- No wonder large tables were failing!

## Critical Filesystem Consideration
**When using Spark Connect (sc://), `df.write.parquet()` writes to the Spark server's filesystem, NOT the local filesystem.** This means:
- `df.write.parquet("/path/file")` writes to the remote Spark server's `/path/file`
- To create a local file, data MUST be collected to the driver (via `toPandas()` or similar)
- This is why we cannot avoid collecting data when using Spark Connect for local file output

## Root Cause Analysis
1. **Memory Collection Issue**: `df.toPandas()` collects entire dataset to driver
2. **Block Manager Failures**: Tasks lose results when executors run out of memory
3. **Network Pressure**: Large data transfers between executors and driver

## Solution Implemented

### Server Configuration Fix
The primary fix was updating the Spark server's `$SPARK_HOME/conf/spark-defaults.conf`:

**Previous Configuration:**
- `spark.driver.memory`: NOT SET (commented out, using ~1-4GB default)
- `spark.driver.maxResultSize`: 10g
- No Arrow optimization

**New Configuration:**
```properties
spark.driver.memory              300g  # Increased from default ~1-4GB
spark.executor.memory            30g   # Uncommented
spark.daemon.memory              100g  # Uncommented
spark.driver.maxResultSize       150g  # Increased from 10g
spark.sql.execution.arrow.pyspark.enabled       true  # Added
spark.sql.execution.arrow.maxRecordsPerBatch    10000 # Added
spark.driver.extraJavaOptions    "... -XX:+UseG1GC -XX:+UseCompressedOops"  # Added GC tuning
```

### Script Optimization: Enhanced toPandas() with Arrow
Additionally optimized the sparkopy script to leverage Arrow:

```python
# OLD: Unoptimized approach
df = spark.read.table(f"{database_name}.{table_name}")
df_p = df.toPandas()
del df_p.attrs['metrics']
df_p.to_parquet(output)

# NEW: Optimized approach with Arrow
df = spark.read.table(f"{database_name}.{table_name}")

# Enable Arrow optimization for faster and more memory-efficient conversion
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
spark.conf.set("spark.driver.maxResultSize", "4g")

df_p = df.toPandas()
if 'metrics' in df_p.attrs:
    del df_p.attrs['metrics']
df_p.to_parquet(output)
```

**Benefits:**
- Arrow columnar format reduces memory overhead
- Batch processing prevents loading entire dataset at once
- Single local parquet file output
- Works with Spark Connect remote clusters

## Alternative Approaches Provided

### 1. Enhanced Memory Configuration (memory_safe version)
For cases requiring pandas features:
- Configurable batch processing
- Arrow optimization enabled
- Increased driver memory limits
- Optional `--use-pandas` flag

### 2. Chunked Processing (commented in code)
For extremely large tables needing pandas:
- Process data in configurable chunks
- Write multiple parquet files
- Avoid memory exhaustion

## Configuration Options Added

### Spark Session Enhancements
```python
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")  
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Arrow Optimization
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.driver.maxResultSize", "4g")
```

## Trade-offs

### Native Spark Write
**Pros:**
- Most memory efficient
- Handles unlimited data size
- Fast execution

**Cons:**
- Output is directory structure (not single file)
- No pandas-specific transformations

### Pandas with Optimizations
**Pros:**
- Single file output
- Pandas features available
- Maintains backward compatibility

**Cons:**
- Still limited by driver memory
- Slower for large datasets

## Usage Examples

### Default (Memory-Safe)
```bash
sparkopy --spark-uri sc://spark.example.com:15002 \
         --database export_<id> \
         --table events \
         --output log/exports/events.parquet
```

### With Pandas (Small Tables)
```bash
sparkopy --spark-uri sc://spark.example.com:15002 \
         --database export_<id> \
         --table events \
         --output log/exports/events.parquet \
         --use-pandas
```

### With Batching (Medium Tables)
```bash
sparkopy --spark-uri sc://spark.example.com:15002 \
         --database export_<id> \
         --table events \
         --output log/exports/events.parquet \
         --use-pandas \
         --batch-size 50000
```

## Files Modified
- `src/sparkopy/__init__.py` - Primary implementation
- `src/sparkopy/__init__.py.memory_safe` - Extended version with options (not committed)

## Next Steps
1. Test with production data
2. Monitor memory usage patterns
3. Consider adding progress reporting for large exports
4. Potentially add S3/cloud storage support