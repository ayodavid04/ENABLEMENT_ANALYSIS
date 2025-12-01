import psycopg2
from psycopg2.extras import execute_values
from .config import DB_CONFIG


def get_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )


def insert_field_catalog(records):
    """
    Insert multiple records into governance.field_catalog.
    records = list of tuples
    """
    query = """
        INSERT INTO governance.field_catalog (
            application_name, raw_field_name, standardised_field_name,
            data_type, is_nullable, is_picklist, description, gisds_mapping
        )
        VALUES %s;
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        execute_values(cur, query, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error inserting into field_catalog:", e)
    finally:
        cur.close()
        conn.close()


def insert_picklist_catalog(records):
    query = """
        INSERT INTO governance.picklist_catalog (
            application_name, field_name, raw_value, value_count
        )
        VALUES %s;
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        execute_values(cur, query, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error inserting into picklist_catalog:", e)
    finally:
        cur.close()
        conn.close()


def insert_schema_profile(records):
    query = """
        INSERT INTO governance.application_schema_profile (
            application_name, field_name, data_type,
            sample_values, null_count, distinct_count, total_rows
        )
        VALUES %s;
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        execute_values(cur, query, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error inserting into application_schema_profile:", e)
    finally:
        cur.close()
        conn.close()
