import pandas as pd
import os
import json
from .config import DATA_PATH


def detect_data_type(series):
    """Return a simplified data type for a pandas Series."""
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    elif pd.api.types.is_float_dtype(series):
        return "float"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    else:
        return "string"


def is_picklist(series, threshold=20):
    """
    Determine if a column behaves like a picklist:
    - Low number of distinct values relative to dataset size.
    """
    distinct = series.dropna().nunique()
    total = len(series)

    return distinct <= threshold or distinct / total < 0.2


def profile_application(application_name, filename):
    """
    Profile a CMS CSV file:
    - Detect schema
    - Detect picklists
    - Generate field metadata
    - Generate picklist values
    - Generate application profile
    """

    file_path = os.path.join(DATA_PATH, filename)
    df = pd.read_csv(file_path)

    schema_metadata = []
    picklist_metadata = []
    profile_metadata = []

    total_rows = len(df)

    for col in df.columns:
        series = df[col]

        # Detect data type
        dtype = detect_data_type(series)

        # Pick sample values
        sample = series.dropna().unique()[:5]
        sample_values = ", ".join([str(v) for v in sample])

        # Basic counts
        null_count = series.isna().sum()
        distinct_count = series.dropna().nunique()

        # Schema profile (for application_schema_profile)
        profile_metadata.append((
            application_name,
            col,
            dtype,
            sample_values,
            int(null_count),
            int(distinct_count),
            int(total_rows)
        ))

        # Field catalog metadata (for field_catalog)
        schema_metadata.append((
            application_name,
            col,
            col.lower(),  # naive standardisation for now
            dtype,
            bool(series.isna().any()),
            bool(is_picklist(series)),
            None,
            None
        ))

        # Picklist extraction (if applicable)
        if is_picklist(series):
            value_counts = series.value_counts(dropna=True)
            for value, count in value_counts.items():
                picklist_metadata.append((
                    application_name,
                    col,
                    str(value),
                    int(count)
                ))
    # Convert metadata to JSON-friendly dicts
    schema_json = [
        {
            "application_name": m[0],
            "raw_field_name": m[1],
            "standardised_field_name": m[2],
            "data_type": m[3],
            "is_nullable": bool(m[4]),
            "is_picklist": bool(m[5]),
            "description": m[6],
            "gisds_mapping": m[7],
        }
        for m in schema_metadata
    ]

    picklist_json = [
        {
            "application_name": p[0],
            "field_name": p[1],
            "raw_value": p[2],
            "value_count": int(p[3]),
        }
        for p in picklist_metadata
    ]

    profile_json = [
        {
            "application_name": p[0],
            "field_name": p[1],
            "data_type": p[2],
            "sample_values": p[3],
            "null_count": int(p[4]),
            "distinct_count": int(p[5]),
            "total_rows": int(p[6]),
        }
        for p in profile_metadata
    ]

    # Save documentation JSON
    output_path = os.path.join(
        os.path.dirname(DATA_PATH),
        "documentation",
        "profiles",
        f"{application_name}_profile.json"
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump({
            "application": application_name,
            "schema": schema_json,
            "picklists": picklist_json,
            "profile": profile_json
        }, f, indent=4)
        
    return schema_metadata, picklist_metadata, profile_metadata
