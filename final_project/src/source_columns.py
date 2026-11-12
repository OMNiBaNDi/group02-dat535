"""Input column requirements shared by Bronze and the offline check."""

# Extra columns are fine; these are the ones we need later.
required_columns = {
    ("epa", "sites"): [
        "State Code", "County Code", "Site Number",
        "Latitude", "Longitude", "Datum",
    ],
    ("epa", "monitors"): [
        "State Code", "County Code", "Site Number",
        "Parameter Code", "POC",
        "First Year of Data", "Last Sample Date",
    ],
    ("epa", "hourly_ozone"): [
        "State Code", "County Code", "Site Num", "Parameter Code", "POC",
        "Date GMT", "Time GMT", "Sample Measurement", "Units of Measure",
        "Qualifier", "MDL", "Method Type", "Method Code",
    ],
    ("noaa", "stations"): [
        "GHCN_ID", "LATITUDE", "LONGITUDE", "ELEVATION", "ISO_CODE",
    ],
    ("noaa", "hourly_weather"): [
        "STATION", "DATE", "temperature",
        "temperature_Measurement_Code", "temperature_Quality_Code",
        "temperature_Report_Type", "temperature_Source_Code",
        "temperature_Source_Station_ID",
    ],
}

def check_columns(source, columns):
    required = required_columns[(source["source"], source["dataset"])]
    missing = sorted(set(required) - set(columns))
    return {
        "contract_version": "bronze-columns-v2",
        "observed_columns": columns,
        "status": "blocked" if missing else "compatible",
        "findings": [f"Missing required columns: {missing}"] if missing else [],
    }
