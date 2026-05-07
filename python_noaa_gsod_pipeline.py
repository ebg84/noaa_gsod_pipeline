"""
GSOD Weather Data Lifecycle Pipeline


INFO-I 535 MGMT ACCESS USE BIG DATA — Lifecycles and Pipelines Module

By: Ethan Gray

Pipeline Architecture:
1. Ingest: Read all 12 GSOD CSV files from Cloud Storage (Chicago, Truckee, Fairbanks, Miami, years 2019-2021

2. Validate: Apply data quality checks, populate validation_flag and quality_flag

3. Route: Split records into clean (for Transformation) and quarantine (straight to Store) based on validation_flag

4. Transform: Apply column renaming, FRSHTT parsing, drop ATTRIBUTES columns

5. Notify: Publish Pub/Sub message summarizing quarantine and quality flag results

6. Store: Write clean/quarantine records to Cloud Storage buckets (eg-gsod-clean from Transform stage and eg-gsod-quarantine from Route stage)

7. Archive: Push pipeline code to GitHub, provide .README (Outside of current notebook)
"""

# Required libraries
import pandas as pd
import os
import io
import logging
from datetime import datetime
from google.cloud import storage, pubsub_v1


# Project info
PROJECT_ID = "project_id"
RAW_BUCKET = "eg-gsod-raw"
CLEAN_BUCKET = "eg-gsod-clean"
QUARANTINE_BUCKET = "eg-gsod-quarantine"
PUBSUB_TOPIC = "pipeline-alerts"
RUN_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")

log = logging.getLogger(__name__)


# Stage 1: Ingest 12 CSVs (4 stations, each with years 2019, 2020, and 2021)
def ingest(bucket_name: str) -> pd.DataFrame:
    log.info(f"Stage 1: Ingesting from gs://{bucket_name}/")
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    csv_blobs = [b for b in blobs if b.name.endswith(".csv")]
    log.info(f"Found {len(csv_blobs)} CSV files across all station subfolders")

    frames = []
    for blob in csv_blobs:
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        df["_source_file"] = blob.name
        frames.append(df)
        log.info(f"  Read {len(df)} records from gs://{bucket_name}/{blob.name}")

    combined = pd.concat(frames, ignore_index=True)
    
    # Per station summary
    for station, count in combined.groupby("NAME").size().items():
        log.info(f"  Station total: {station} -> {count} records")
    
    return combined


# Stage 2: Validate records
def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data quality checks to each record.
    Populates validation_flag (hard failures -> quarantine)
    and quality_flag (soft warnings -> clean with flag).

    Validation logic interpreted from NOAA GSOD readme documentation
    and EDA from select four stations.
    """
    log.info("Stage 2: Validating records")

    df = df.copy()
    df["validation_flag"] = ""
    df["quality_flag"] = ""

    def append_flag(series, condition, message):
        """Append a flag message to existing flag values."""
        mask = condition & (series != "")
        series = series.copy()
        series[condition & (series == "")] = message
        series[mask] = series[mask] + " | " + message
        return series

    # Quarantine checks (validation_flag -> eg-gsod-quarantine)
    # Missing checks are indicated by a float value of 9's
    # Unobserved checks indicate that a measurement has been recorded
        # but there is no indication in the data that there were any
        # observations that day to record the value
    
    # Temperature missing
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        df["TEMP"] == 9999.9,
        "FAIL_MISSING_TEMP")

    # Temperature unobserved
        
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        (df["TEMP_ATTRIBUTES"] == 0) & (df["TEMP"] != 9999.9),
        "FAIL_UNOBSERVED_TEMP")

    # Dew point missing
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        df["DEWP"] == 9999.9,
        "FAIL_MISSING_DEWP")

    # Dew point unobserved
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        (df["DEWP_ATTRIBUTES"] == 0) & (df["DEWP"] != 9999.9),
        "FAIL_UNOBSERVED_DEWP")

    # Station pressure missing (either sea level or direct station pressure)
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        (df["STP"] == 9999.9) & (df["SLP"] == 9999.9),
        "FAIL_MISSING_PRESSURE")

    # Station pressure physically impossible: below 600mb or above 1075mb
        # 600mb is rough minimum possible pressure at highest US airport elevations
        # 1075mb is roughly maximum recorded surface pressure recorded ever globally
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        ((df["STP"] < 600) | (df["STP"] > 1075)) & (df["STP"] != 9999.9) & 
        ((df["SLP"] < 600) | (df["SLP"] > 1075)) & (df["SLP"] != 9999.9),
        "FAIL_INVALID_PRESSURE")

    # Visibility missing
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        df["VISIB"] == 999.9,
        "FAIL_MISSING_VISIB")

    # Visibility unobserved
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        (df["VISIB_ATTRIBUTES"] == 0) & (df["VISIB"] != 999.9),
        "FAIL_UNOBSERVED_VISIB")

    # Wind speed missing
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        df["WDSP"] == 999.9,
        "FAIL_MISSING_WDSP")

    # Wind speed unobserved
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        (df["WDSP_ATTRIBUTES"] == 0) & (df["WDSP"] != 999.9),
        "FAIL_UNOBSERVED_WDSP")

    # Max sustained wind speed missing
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        df["MXSPD"] == 999.9,
        "FAIL_MISSING_MXSPD")

    # Max temperature missing
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        df["MAX"] == 9999.9,
        "FAIL_MISSING_MAX_TEMP")

    # Min temperature missing
    df["validation_flag"] = append_flag(
        df["validation_flag"],
        df["MIN"] == 9999.9,
        "FAIL_MISSING_MIN_TEMP")

    # quality_flag checks (pass through clean)
    # Records with sparse observations pass to clean but carry a warning.
    # Records with fewer than 12 hourly observations indicates less than half the day's
        # measurements are recorded. Downstream analysis use will determine
        # if these records should be kept or not

    # Sparse temperature observations
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        (df["TEMP_ATTRIBUTES"] > 0) & (df["TEMP_ATTRIBUTES"] < 12),
        "FLAG_SPARSE_TEMP_OBSERVATIONS")
    
    # Sparse dew point observations
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        (df["DEWP_ATTRIBUTES"] > 0) & (df["DEWP_ATTRIBUTES"] < 12),
        "FLAG_SPARSE_DEWP_OBSERVATIONS")

    # Sparse station pressure observations
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        (df["STP_ATTRIBUTES"] > 0) & (df["STP_ATTRIBUTES"] < 12),
        "FLAG_SPARSE_STP_OBSERVATIONS")
    
    # Missing station pressure
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        df["STP"] == 9999.9,
        "FLAG_MISSING_STP")
 
    # Physically impossible station pressure
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        ((df["STP"] < 600) | (df["STP"] > 1075)) & (df["STP"] != 9999.9) & 
        (df["SLP"] >= 600) & (df["SLP"] <= 1075) & (df["SLP"] != 9999.9),
        "FLAG_ANOMALY_STP")
    
    # Unobserved station pressure
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        (df["STP_ATTRIBUTES"] == 0) & (df["STP"] != 9999.9) & 
        (df["SLP"] >= 600) & (df["SLP"] <= 1075) & (df["SLP"] != 9999.9),
        "FLAG_UNOBSERVED_STP")

    # Sparse sea level pressure observations
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        (df["SLP_ATTRIBUTES"] > 0) & (df["SLP_ATTRIBUTES"] < 12),
        "FLAG_SPARSE_SLP_OBSERVATIONS")
   
    # Missing sea level pressure
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        df["SLP"] == 9999.9,
        "FLAG_MISSING_SLP")

    # Physically impossible sea level pressure
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        ((df["SLP"] < 600) | (df["SLP"] > 1075)) & (df["SLP"] != 9999.9) & 
        (df["STP"] >= 600) & (df["STP"] <= 1075) & (df["STP"] != 9999.9),
        "FLAG_ANOMALY_SLP")
    
    # Unobserved sea level pressure
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        (df["SLP_ATTRIBUTES"] == 0) & (df["SLP"] != 9999.9) & 
        (df["STP"] >= 600) & (df["STP"] <= 1075) & (df["STP"] != 9999.9),
        "FLAG_UNOBSERVED_SLP")

    # Sparse visibility observations
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        (df["VISIB_ATTRIBUTES"] > 0) & (df["VISIB_ATTRIBUTES"] < 12),
        "FLAG_SPARSE_VISIB_OBSERVATIONS")

    # Sparse wind speed observations
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        (df["WDSP_ATTRIBUTES"] > 0) & (df["WDSP_ATTRIBUTES"] < 12),
        "FLAG_SPARSE_WDSP_OBSERVATIONS")

    # Precipitation: 99.99 or 0.0 are both acceptable indications of
    # no rain reported, though some 99.99 instances may be genuinely missing records
    # instead of not being reported, so quality_flag will be raised
    df["quality_flag"] = append_flag(
        df["quality_flag"],
        df["PRCP"] == 99.99,
        "FLAG_MISSING_PRCP")

    # Summary
    quarantine_count = (df["validation_flag"] != "").sum()
    clean_count = (df["validation_flag"] == "").sum()
    flagged_count = (df["quality_flag"] != "").sum()
    log.info(f"Validation complete -> Clean: {clean_count} | Quarantine: {quarantine_count} | Quality flagged: {flagged_count}")

    return df


# Stage 3: Route clean and quarantine records
def route(df: pd.DataFrame) -> tuple:
    """
    Split records into clean and quarantine based on validation_flag.
    Records are routed individually and not by file so a
    station file may contribute records to both buckets simultaneously.
    """
    log.info("Stage 3: Routing records to clean or quarantine")
    clean = df[df["validation_flag"] == ""].copy()
    quarantine = df[df["validation_flag"] != ""].copy()
    log.info(f"Clean records: {len(clean)} | Quarantine records: {len(quarantine)}")
    return clean, quarantine


# Stage 4: Transform clean records
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to clean records only.
    - Parse FRSHTT binary string into six boolean weather event columns
    - Rename columns to descriptive lowercase names with units
    - Drop ATTRIBUTES columns (quality signal already captured in quality_flag)
    """
    log.info("Stage 4: Transforming clean records")
    df = df.copy()
    input_columns = set(df.columns)

    # Padding 6 characters to FRSHTT to parse into boolean columns
    # These leading 0's get stripped when read into excel
    df["FRSHTT"] = df["FRSHTT"].astype(str).str.zfill(6)
    df["fog"] = df["FRSHTT"].str[0] == "1"
    df["rain_or_drizzle"] = df["FRSHTT"].str[1] == "1"
    df["snow_or_ice_pellets"] = df["FRSHTT"].str[2] == "1"
    df["hail"] = df["FRSHTT"].str[3] == "1"
    df["thunder"] = df["FRSHTT"].str[4] == "1"
    df["tornado_or_funnel_cloud"] = df["FRSHTT"].str[5] == "1"
    df.drop(columns=["FRSHTT"], inplace=True)

    # Rename columns to be lowercase and in plain english with units for clarity
    rename_map = {
        "STATION": "station",
        "DATE": "date",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "ELEVATION": "elevation_meters",
        "NAME": "name",
        "TEMP": "temperature_f",
        "DEWP": "dewpoint_f",
        "SLP": "sea_level_pressure_mb",
        "STP": "station_pressure_mb",
        "VISIB": "visibility_miles",
        "WDSP": "wind_speed_knots",
        "MXSPD": "max_wind_speed_knots",
        "GUST": "wind_gust_knots",
        "MAX": "max_temperature_f",
        "MIN": "min_temperature_f",
        "PRCP": "precipitation_inches",
        "SNDP": "snow_depth_inches",}
    
    df.rename(columns=rename_map, inplace=True)

    # ATTRIBUTES columns are dropped, issues are appended to quality_flag
    
    attributes_columns = [
        "TEMP_ATTRIBUTES", "DEWP_ATTRIBUTES", "SLP_ATTRIBUTES",
        "STP_ATTRIBUTES", "VISIB_ATTRIBUTES", "WDSP_ATTRIBUTES",
        "MAX_ATTRIBUTES", "MIN_ATTRIBUTES", "PRCP_ATTRIBUTES"]
    
    existing_attributes = [column for column in attributes_columns if column in df.columns]
    df.drop(columns=existing_attributes, inplace=True)
    df.drop(columns=["validation_flag"], inplace=True)
    df.drop(columns=["_source_file"], inplace=True)

    output_columns = set(df.columns)
    added = output_columns - input_columns
    removed = input_columns - output_columns
    renamed = {old: new for old, new in rename_map.items() if old in input_columns}
    log.info(f"Transformation complete: {len(df.columns)} output columns")
    log.info(f"Columns added ({len(added)}): {sorted(added)}")
    log.info(f"Columns removed ({len(removed)}): {sorted(removed)}")
    log.info(f"Columns renamed ({len(renamed)}): {renamed}")
    
    return df


# Stage 5: Notify with GCS Pub/Sub

def notify(quarantine: pd.DataFrame, validated: pd.DataFrame) -> None:
    """
    Publish a Pub/Sub message summarizing pipeline results.
    Always publishes regardless of quarantine count, including
    quality flag distribution across clean records.
    """
    log.info("Stage 5: Publishing Pub/Sub notification")

    # Quarantine summary
    if len(quarantine) > 0:
        station_summary = quarantine.groupby("NAME")["validation_flag"].count().to_dict()
        failure_reasons = (quarantine["validation_flag"].str.split("|").explode().str.strip().loc[lambda x: x.str.len() > 0].value_counts().to_dict())
        
    else:
        station_summary = {}
        failure_reasons = {}

    # Quality flag summary across all validated records
    quality_flagged = validated[validated["quality_flag"] != ""]
    quality_flag_counts = (quality_flagged["quality_flag"].str.split("|").explode().str.strip().loc[lambda x: x.str.len() > 0].value_counts().to_dict())
    
    quality_by_station = quality_flagged.groupby("NAME")["quality_flag"].count().to_dict() if len(quality_flagged) > 0 else {}

    message = (
        f"GSOD Pipeline Run Summary: {RUN_TIMESTAMP}\n"
        f"Total records processed: {len(validated)}\n"
        f"\n--- Quarantine Summary ---\n"
        f"Total quarantined records: {len(quarantine)}\n"
        f"Quarantine by station: {station_summary}\n"
        f"Failure reason counts: {failure_reasons}\n"
        f"\n--- Quality Flag Summary ---\n"
        f"Total quality flagged records: {len(quality_flagged)}\n"
        f"Quality flags by station: {quality_by_station}\n"
        f"Quality flag type counts: {quality_flag_counts}"
    )

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    future = publisher.publish(topic_path, message.encode("utf-8"))
    message_id = future.result()
    log.info(f"Pub/Sub message published --> message ID: {message_id}")
    log.info(f"Message content:\n{message}")
    
    
# Stage 6: Write and store to eg-gsod-clean and eg-gsod-quarantine
def write_to_gcs(df: pd.DataFrame, bucket_name: str, prefix: str) -> None:
    """Write a dataframe as CSV to a Cloud Storage bucket."""
    if len(df) == 0:
        log.info(f"No records to write to gs://{bucket_name}/{prefix}")
        return

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    filename = f"{prefix}/part-{RUN_TIMESTAMP}.csv"
    blob = bucket.blob(filename)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
    log.info(f"Written {len(df)} records to gs://{bucket_name}/{filename}")

    
# Pipeline run stages 1-6
def main():
    log.info("GSOD Lifecycle Pipeline: Starting")
    log.info(f"Run timestamp: {RUN_TIMESTAMP}")

    # Stage 1: Ingest
    raw = ingest(RAW_BUCKET)
    log.info(f"Stage 1 complete: {len(raw)} records ingested")

    # Stage 2: Validate
    validated = validate(raw)
    log.info(f"Stage 2 complete: {len(validated)} records validated — input/output match: {len(raw) == len(validated)}")

    # Stage 3: Route
    clean, quarantine = route(validated)
    log.info(f"Quarantine by station: {quarantine.groupby('NAME').size().to_dict()}")
    log.info(f"Clean by station: {clean.groupby('NAME').size().to_dict()}")
    log.info(f"Stage 3 complete: {len(clean)} clean + {len(quarantine)} quarantine = {len(clean) + len(quarantine)} total — match: {len(clean) + len(quarantine) == len(validated)}")

    # Stage 4: Transform
    transformed_clean = transform(clean)
    log.info(f"Stage 4 complete: {len(transformed_clean)} records transformed — input/output match: {len(transformed_clean) == len(clean)}")

    # Stage 5: Notify
    notify(quarantine, validated)
    log.info(f"Stage 5 complete: Pub/Sub notification published")

    # Stage 6: Store
    log.info("Stage 6: Writing outputs to Cloud Storage")
    write_to_gcs(transformed_clean, CLEAN_BUCKET, "clean")
    write_to_gcs(quarantine, QUARANTINE_BUCKET, "quarantine")
    log.info(f"Stage 6 complete: {len(transformed_clean)} clean + {len(quarantine)} quarantine records written")

    # Final summary
    log.info("Pipeline completed successfully")
    log.info(f"Total records processed: {len(raw)}")
    log.info(f"Clean records written: {len(transformed_clean)}")
    log.info(f"Quarantine records: {len(quarantine)}")
    log.info(f"Soft flagged records: {(validated['quality_flag'] != '').sum()}")
    log.info(f"Record integrity check: input {len(raw)} == output {len(transformed_clean) + len(quarantine)} -> {len(raw) == len(transformed_clean) + len(quarantine)}")

if __name__ == "__main__":
    main()
