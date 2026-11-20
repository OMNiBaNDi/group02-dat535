"""Input column requirements shared by Bronze and the offline check."""

# These columns must be present. Extra columns are allowed.
required_columns = {
    ("epa", "sites"): [
        "State Code", "County Code", "Site Number",
        "Latitude", "Longitude", "Datum", "Elevation",
        "Site Established Date", "Site Closed Date", "Extraction Date",
        "Local Site Name", "State Name", "County Name", "Land Use", "Location Setting",
    ],
    ("epa", "monitors"): [
        "State Code", "County Code", "Site Number",
        "Parameter Code", "POC",
        "First Year of Data", "Last Sample Date", "Extraction Date",
        "Latitude", "Longitude", "Datum", "Parameter Name", "Monitor Type",
        "Measurement Scale", "Last Method Code", "Last Method",
    ],
    ("epa", "hourly_ozone"): [
        "State Code", "County Code", "Site Num", "Parameter Code", "POC",
        "Date GMT", "Time GMT", "Sample Measurement", "Units of Measure",
        "Qualifier", "MDL", "Method Type", "Method Code", "Method Name",
    ],
    ("noaa", "stations"): [
        "GHCN_ID", "LATITUDE", "LONGITUDE", "ELEVATION", "ISO_CODE", "NAME", "STATE", "ICAO",
    ],
    ("noaa", "hourly_weather"): [
        "STATION", "DATE", "temperature",
        "temperature_Measurement_Code", "temperature_Quality_Code",
        "temperature_Report_Type", "temperature_Source_Code",
        "temperature_Source_Station_ID",
    ],
}

def check_columns(source, columns):
    """Check column names and return a compatible or blocked status."""
    required = required_columns[(source["source"], source["dataset"])]
    # Silver checks the field values.
    missing = sorted(set(required) - set(columns))
    return {
        "status": "blocked" if missing else "compatible",
    }
