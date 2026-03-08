# NYC Yellow Cab Data 2025 Aggregations in Rust
What this project does
- Using Data Fusion I loaded the 2025 yellow cab trip data from the Parquet files
- I made two aggregations using both the DataFrame API and SQL
- Print table of data from the aggregations
- Only using January through Novemebr becuse Decemebr data is not avaiable yet

# Data Source
- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
  
# Downloading Data
- I manually downloaded each month of yellow cab data into a folder called NYC_Yellow and read through each month from there.

# Running the Project
- cd C:\Users\mvoen\workspace-rust-de
- cargo build
- cargo run
# Aggregations
- Aggregation 1
- seperates each trip by the month and then gets Trip count, Total Revenue, Average fare.
  
