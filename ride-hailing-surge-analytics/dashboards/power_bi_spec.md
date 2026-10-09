# Power BI Dashboard Specification Build Sheet

This document specifies the exact dataset connections, tables, DAX measures, and visual configurations for building the `Ride_Hailing_Surge_Analytics.pbix` Power BI dashboard.

---

## 1. Data Connections

- **Primary Source (Databricks Gold / Athena):**
  - Connector: Databricks Connector / Amazon Athena ODBC
  - Import Mode: **DirectQuery** for real-time live heat map page auto-refresh.

---

## 2. Required Data Model & Tables

### Fact Table: `fact_trips`
- `trip_id` (String)
- `pickup_zone_key` (Integer -> `dim_zone.zone_key`)
- `dropoff_zone_key` (Integer)
- `time_key` (BigInt -> `dim_time.time_key`)
- `fare_amount` (Decimal)
- `distance_km` (Decimal)
- `surge_multiplier` (Decimal)
- `wait_time_min` (Decimal)
- `trip_status` (String)

### Aggregated Table: `gold_zone_minute_surge`
- `zone_id` (Integer)
- `window_end` (DateTime)
- `demand` (Integer)
- `supply` (Integer)
- `ratio` (Decimal)
- `surge_multiplier` (Decimal)

---

## 3. DAX Measures

```dax
// 1. Average Rider ETA (Target < 4.5 minutes)
Avg_Rider_ETA = AVERAGE(fact_trips[wait_time_min])

// 2. Driver Utilisation Rate (Target >= 78%)
Driver_Utilisation_Rate = 
DIVIDE(
    SUM(fact_trips[duration_min]),
    SUM(dim_driver[online_minutes]),
    0
)

// 3. Revenue per Kilometer
Revenue_Per_KM = 
DIVIDE(
    SUM(fact_trips[fare_amount]),
    SUM(fact_trips[distance_km]),
    0
)

// 4. Current Average Surge Multiplier
Current_Avg_Surge = AVERAGE(gold_zone_minute_surge[surge_multiplier])
```

---

## 4. Visual Layout Specifications

| Page | Visual Type | Fields & Measures | Target / Rule |
| --- | --- | --- | --- |
| **1. Live Heat Map** | **Shape Map Visual** | Location: `zone_id`<br>Color Saturation: `Current_Avg_Surge`<br>Shapefile: `taxi_zones.json` / TopoJSON | Auto-refresh every 60s.<br>Color gradient: Green (1.0x) -> Orange (1.8x) -> Red (3.0x). |
| **2. Utilisation Gauge** | **Radial Gauge** | Value: `Driver_Utilisation_Rate`<br>Target Value: `0.78` | Callout value turned on.<br>Red < 78%, Green >= 78%. |
| **3. Revenue Trends** | **Line & Clustered Column** | X-Axis: `hour`<br>Column: `Revenue_Per_KM`<br>Line: `Avg_Rider_ETA` | Target line for ETA fixed at 4.5 min threshold. |
| **4. Demand Forecast** | **Line Chart** | X-Axis: `time`<br>Values: Actual Demand vs `predicted_demand_next_30m` | Shaded error band for 30-min forecast window. |
