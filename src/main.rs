use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let session = SessionContext::new();

    let pattern = "nyc_tlc_yellow/yellow_tripdata_2025-*.parquet";
    session.register_parquet("trips", pattern, ParquetReadOptions::default())
        .await?;


    //aggregation 1 SQL: total trips and revenue by month
    let agg1_sql = session.sql(
    "SELECT
        EXTRACT(MONTH FROM tpep_pickup_datetime) AS pickup_month,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_revenue,
        AVG(fare_amount) AS avg_fare
    FROM trips
    WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2025
    GROUP BY pickup_month
    ORDER BY pickup_month ASC;"
    ).await?;
    println!("\n=== Aggregation 1 (SQL): Trips and revenue by month ===");
    agg1_sql.show().await?;


    //aggregation 1 API: total trips and revenue by month
    use datafusion::functions_aggregate::expr_fn::{avg, count, sum};

    println!("\n=== Aggregation 1 (DataFrame API): Trips and revenue by month ===");

    let trips = session.table("trips").await?;

    // Expressions we will reuse
    let pickup_year = date_part(lit("year"), col("tpep_pickup_datetime"));
    let pickup_month = date_part(lit("month"), col("tpep_pickup_datetime"))
    .alias("pickup_month");

    let agg1_df = trips
    // Keep only 2025 data
    .filter(pickup_year.eq(lit(2025)))?
    // Select the derived month + needed columns
    .select(vec![
        pickup_month,
        col("total_amount"),
        col("fare_amount"),
    ])?
    // Group + aggregate
    .aggregate(
        vec![col("pickup_month")],
        vec![
            count(lit(1)).alias("trip_count"),
            sum(col("total_amount")).alias("total_revenue"),
            avg(col("fare_amount")).alias("avg_fare"),
        ],
    )?
    // Sort by month ascending
    .sort(vec![col("pickup_month").sort(true, true)])?;

    agg1_df.show().await?;


    //aggreation 2 SQL: tip behavior by payment type
    println!("\n=== Aggregation 2 (SQL): Tip behavior by payment type ===");

    let agg2_sql = session.sql(
        "SELECT
            payment_type,
            COUNT(*) AS trip_count,
            AVG(tip_amount) AS avg_tip_amount,
            SUM(tip_amount) / SUM(total_amount) AS tip_rate
        FROM trips
        WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2025
            AND total_amount IS NOT NULL
            AND total_amount > 0
        GROUP BY payment_type
        ORDER BY trip_count DESC"
    ).await?;

    agg2_sql.show().await?;



    //aggregation 2 API: tip behavior by payment type
    println!("\n=== Aggregation 2 (DataFrame API): Tip behavior by payment type ===");

    let trips = session.table("trips").await?;

    let pickup_year = date_part(lit("year"), col("tpep_pickup_datetime"));

    // Step 1: filter + aggregate
    let agg = trips
    .filter(pickup_year.eq(lit(2025)))?
    // enforce total_amount > 0 to avoid divide-by-zero and nonsense rates
    .filter(col("total_amount").gt(lit(0)))?
    .aggregate(
        vec![col("payment_type")],
        vec![
            count(lit(1)).alias("trip_count"),
            avg(col("tip_amount")).alias("avg_tip_amount"),
            sum(col("tip_amount")).alias("sum_tip_amount"),
            sum(col("total_amount")).alias("sum_total_amount"),
        ],
    )?;

    // Step 2: compute tip_rate and select final columns
    let agg2_df = agg
    .select(vec![
        col("payment_type"),
        col("trip_count"),
        col("avg_tip_amount"),
        (col("sum_tip_amount") / col("sum_total_amount")).alias("tip_rate"),
    ])?
    .sort(vec![col("trip_count").sort(false, true)])?; // false = descending

    agg2_df.show().await?;


    Ok(())
}