"""
Standalone loader: apply DDL + seed genealogy data into Postgres.
Use this instead of the Dagster asset when running without a Dagster instance.

Usage:
    DATABASE_URL=postgresql://... python pipeline/seed.py
"""

import os
import sys
import psycopg2
from urllib.parse import urlparse, unquote
from pathlib import Path
from pipeline import prod_guard
from pipeline.generate_genealogy import generate_all


def _parse_url(url):
    """Return psycopg2 connect kwargs from a postgres URL, percent-decoding password."""
    p = urlparse(url)
    return dict(
        host=p.hostname,
        port=p.port or 5432,
        dbname=p.path.lstrip("/"),
        user=p.username,
        password=unquote(p.password) if p.password else None,
    )
DDL_PATH = Path(__file__).parent.parent / "data" / "schema" / "genealogy_ddl.sql"


def apply_ddl(conn):
    ddl = DDL_PATH.read_text()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        print("DDL applied.")
    except Exception as e:
        conn.rollback()
        print(f"DDL skipped (already applied): {e}")


def load_data(conn, data):
    cur = conn.cursor()

    for row in data["retailers"]:
        cur.execute("""
            INSERT INTO raw.retailers (retailer_id, name, store_doors)
            VALUES (%(retailer_id)s, %(name)s, %(store_doors)s)
            ON CONFLICT (retailer_id) DO NOTHING
        """, row)

    for row in data["product_master"]:
        cur.execute("""
            INSERT INTO raw.product_master (sku, product_name, product_line, cases_per_pallet)
            VALUES (%(sku)s, %(product_name)s, %(product_line)s, %(cases_per_pallet)s)
            ON CONFLICT (sku) DO NOTHING
        """, row)

    for row in data["shipments"]:
        cur.execute("""
            INSERT INTO raw.shipments (shipment_id, order_id, retailer_id, ship_date, cases_shipped)
            VALUES (%(shipment_id)s, %(order_id)s, %(retailer_id)s, %(ship_date)s, %(cases_shipped)s)
            ON CONFLICT (shipment_id) DO NOTHING
        """, row)

    for row in data["co_packers"]:
        cur.execute("""
            INSERT INTO genealogy.co_packers (co_packer_id, name, lot_code_format, primary_lines)
            VALUES (%(co_packer_id)s, %(name)s, %(lot_code_format)s, %(primary_lines)s)
            ON CONFLICT (co_packer_id) DO NOTHING
        """, row)

    for row in data["ingredients"]:
        cur.execute("""
            INSERT INTO genealogy.ingredients (ingredient_id, name, category, is_ftl_upstream, unit)
            VALUES (%(ingredient_id)s, %(name)s, %(category)s, %(is_ftl_upstream)s, %(unit)s)
            ON CONFLICT (ingredient_id) DO NOTHING
        """, row)

    for row in data["ingredient_lots"]:
        cur.execute("""
            INSERT INTO genealogy.ingredient_lots
              (ingredient_lot_id, ingredient_id, co_packer_id, supplier_name,
               supplier_lot_code, co_packer_lot_code, quantity_lbs,
               received_date, best_by_date, status)
            VALUES (%(ingredient_lot_id)s, %(ingredient_id)s, %(co_packer_id)s,
                    %(supplier_name)s, %(supplier_lot_code)s, %(co_packer_lot_code)s,
                    %(quantity_lbs)s, %(received_date)s, %(best_by_date)s, %(status)s)
            ON CONFLICT (ingredient_lot_id) DO NOTHING
        """, row)

    for row in data["production_batches"]:
        cur.execute("""
            INSERT INTO genealogy.production_batches
              (batch_id, sku_id, co_packer_id, production_date,
               batch_quantity_cases, co_packer_batch_code, status)
            VALUES (%(batch_id)s, %(sku_id)s, %(co_packer_id)s, %(production_date)s,
                    %(batch_quantity_cases)s, %(co_packer_batch_code)s, %(status)s)
            ON CONFLICT (batch_id) DO NOTHING
        """, row)

    for row in data["batch_ingredient_map"]:
        cur.execute("""
            INSERT INTO genealogy.batch_ingredient_map
              (batch_id, ingredient_lot_id, quantity_used_lbs)
            VALUES (%(batch_id)s, %(ingredient_lot_id)s, %(quantity_used_lbs)s)
            ON CONFLICT (batch_id, ingredient_lot_id) DO NOTHING
        """, row)

    for row in data["fg_lots"]:
        cur.execute("""
            INSERT INTO genealogy.fg_lots
              (fg_lot_id, batch_id, sku_id, internal_lot_code, co_packer_lot_code,
               quantity_cases, production_date, best_by_date, status)
            VALUES (%(fg_lot_id)s, %(batch_id)s, %(sku_id)s, %(internal_lot_code)s,
                    %(co_packer_lot_code)s, %(quantity_cases)s, %(production_date)s,
                    %(best_by_date)s, %(status)s)
            ON CONFLICT (fg_lot_id) DO NOTHING
        """, row)

    for row in data["packaging_lots"]:
        cur.execute("""
            INSERT INTO genealogy.packaging_lots
              (packaging_lot_id, packaging_type, supplier_name, lot_code,
               quantity_units, received_date)
            VALUES (%(packaging_lot_id)s, %(packaging_type)s, %(supplier_name)s,
                    %(lot_code)s, %(quantity_units)s, %(received_date)s)
            ON CONFLICT (packaging_lot_id) DO NOTHING
        """, row)

    for row in data["batch_packaging_map"]:
        cur.execute("""
            INSERT INTO genealogy.batch_packaging_map (batch_id, packaging_lot_id)
            VALUES (%(batch_id)s, %(packaging_lot_id)s)
            ON CONFLICT (batch_id, packaging_lot_id) DO NOTHING
        """, row)

    for row in data["shipment_lot_map"]:
        cur.execute("""
            INSERT INTO genealogy.shipment_lot_map
              (shipment_id, fg_lot_id, cases_shipped, cases_in_channel, cases_sold_through)
            VALUES (%(shipment_id)s, %(fg_lot_id)s, %(cases_shipped)s,
                    %(cases_in_channel)s, %(cases_sold_through)s)
            ON CONFLICT (shipment_id, fg_lot_id) DO NOTHING
        """, row)

    for row in data["scenarios"]:
        cur.execute("""
            INSERT INTO genealogy.scenarios
              (scenario_id, title, description, root_node_type, root_node_id)
            VALUES (%(scenario_id)s, %(title)s, %(description)s,
                    %(root_node_type)s, %(root_node_id)s)
            ON CONFLICT (scenario_id) DO UPDATE SET
              title          = EXCLUDED.title,
              description    = EXCLUDED.description,
              root_node_type = EXCLUDED.root_node_type,
              root_node_id   = EXCLUDED.root_node_id
        """, row)

    conn.commit()
    cur.close()

    print(f"Loaded: {len(data['ingredient_lots'])} ingredient lots, "
          f"{len(data['production_batches'])} batches, "
          f"{len(data['fg_lots'])} FG lots, "
          f"{len(data['shipment_lot_map'])} shipment links, "
          f"{len(data['scenarios'])} scenarios")


if __name__ == "__main__":
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print(
            "Error: DATABASE_URL environment variable is required. "
            "Set it to your Postgres connection string (see .env.example).",
            file=sys.stderr,
        )
        sys.exit(1)

    # Generate data before opening the DB connection — Fly proxy drops idle connections
    print("Generating seed data...")
    data = generate_all()
    for table, rows in data.items():
        print(f"  {table}: {len(rows)} rows")

    prod_guard.check(database_url)  # refuses a fly tunnel to production
    try:
        conn = psycopg2.connect(**_parse_url(database_url))
    except Exception as e:
        print(f"DB connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        apply_ddl(conn)
        print("Loading to DB...")
        load_data(conn, data)
        print("Done.")
    finally:
        conn.close()
