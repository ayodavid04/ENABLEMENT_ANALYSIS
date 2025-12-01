from .profiler import profile_application
from .db import (
    insert_field_catalog,
    insert_picklist_catalog,
    insert_schema_profile,
)

import os

def run_metadata_pipeline():
    applications = {
        "cms_app_a": "cms_app_a.csv",
        "cms_app_b": "cms_app_b.csv",
        "cms_app_c": "cms_app_c.csv",
    }
    
    print ("\n=== Starting Metadeata Extractinon Pipeline ===\n")
    
    for app_name, filename in applications.items():
        print(f"Processing application: {app_name}")

        schema_meta, picklist_meta, profile_meta = profile_application(
            app_name, filename
        )

        print(f"  -> Extracted {len(schema_meta)} schema fields")
        print(f"  -> Extracted {len(picklist_meta)} picklist values")
        print(f"  -> Extracted {len(profile_meta)} profile rows")

        # Load into governance tables
        print(f"  -> Loading metadata into governance tables...")
        insert_field_catalog(schema_meta)
        insert_picklist_catalog(picklist_meta)
        insert_schema_profile(profile_meta)

        print(f"  -> Completed: {app_name}\n")

    print("=== Metadata Extraction Pipeline Completed ===")


if __name__ == "__main__":
    run_metadata_pipeline()