import os
import psycopg2
from urllib.parse import urlparse, unquote
from dagster import asset, AssetExecutionContext
from pipeline import prod_guard
from pipeline.generate_genealogy import generate_all


def _get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required. "
            "Set it to your Postgres connection string (see .env.example)."
        )
    prod_guard.check(database_url)  # refuses a fly tunnel to production
    p = urlparse(database_url)
    return psycopg2.connect(
        host=p.hostname,
        port=p.port or 5432,
        dbname=p.path.lstrip("/"),
        user=p.username,
        password=unquote(p.password) if p.password else None,
    )


@asset
def genealogy_seed(context: AssetExecutionContext):
    """Load genealogy seed data (seed=400) into Postgres."""
    data = generate_all()
    conn = _get_conn()
    cur  = conn.cursor()

    # Platform stubs
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

    # Genealogy tables
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
    conn.close()

    context.log.info(f"Loaded genealogy seed: "
                     f"{len(data['ingredient_lots'])} ingredient lots, "
                     f"{len(data['production_batches'])} batches, "
                     f"{len(data['fg_lots'])} FG lots, "
                     f"{len(data['shipment_lot_map'])} shipment links")


@asset(deps=[genealogy_seed])
def genealogy_graph(context: AssetExecutionContext):
    """
    Build NetworkX graph from fct_blast_radius and pre-compute the three
    preset scenario JSON responses for fast API serving.
    """
    import json
    from pathlib import Path
    from pipeline.graph import (
        build_graph, subgraph_for_root, aggregate_shipments,
        packaging_lot_rows, packaging_lot_scope,
        scope_row_to_panel, graph_to_api_format, _empty_scope,
    )

    conn = _get_conn()
    cur  = conn.cursor()

    # Load all blast radius rows (ingredient-lot roots)
    cur.execute("""
        SELECT root_lot_id, node_type, node_id, label, depth, parent_id
        FROM public.fct_blast_radius
    """)
    br_rows = cur.fetchall()

    # Load scope data keyed by root_lot_id
    cur.execute("SELECT * FROM public.fct_blast_radius_scope")
    cols = [d[0] for d in cur.description]
    scope_by_root = {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}

    # Load scenario definitions
    cur.execute("""
        SELECT scenario_id, title, description, root_node_type, root_node_id
        FROM genealogy.scenarios
        ORDER BY scenario_id
    """)
    scenarios = [
        {"id": r[0], "title": r[1], "description": r[2],
         "root_node_type": r[3], "root_node_id": r[4]}
        for r in cur.fetchall()
    ]
    cur.close()

    # Build full ingredient-lot graph
    G = build_graph(br_rows)
    context.log.info(
        f"Full graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges"
    )

    # Pre-compute per-scenario sub-graphs
    scenario_responses = []

    for scenario in scenarios:
        sid          = scenario["id"]
        root_id      = scenario["root_node_id"]
        root_type    = scenario["root_node_type"]

        if root_type == "packaging_lot":
            # Scenario C: traverse from batch_packaging_map directly
            pkg_rows = packaging_lot_rows(conn, root_id)
            sub_G    = build_graph(pkg_rows)
            scope    = packaging_lot_scope(conn, root_id, pkg_rows)
        else:
            sub_G = subgraph_for_root(G, root_id)
            scope = scope_row_to_panel(scope_by_root[root_id]) \
                    if root_id in scope_by_root else _empty_scope()

        sub_G = aggregate_shipments(sub_G)

        result = graph_to_api_format(sub_G, root_id, scope)
        scenario_responses.append({
            "id":          sid,
            "title":       scenario["title"],
            "description": scenario["description"],
            "result":      result,
        })
        context.log.info(
            f"Scenario {sid}: {sub_G.number_of_nodes()} nodes, "
            f"{sub_G.number_of_edges()} edges, "
            f"{scope['cases_in_channel']} cases in channel"
        )

    conn.close()

    # Cache to disk — the FastAPI /scenarios endpoint reads this file
    cache_dir = Path(__file__).parent / "cache"
    cache_dir.mkdir(exist_ok=True)
    cache_path = cache_dir / "scenario_graphs.json"
    cache_path.write_text(json.dumps(scenario_responses, indent=2, default=str))
    context.log.info(f"Cached {len(scenario_responses)} scenarios → {cache_path}")


@asset(deps=[genealogy_graph])
def scenario_graphs(context: AssetExecutionContext):
    """Validate that the cached scenario JSON is present and well-formed."""
    import json
    from pathlib import Path

    cache_path = Path(__file__).parent / "cache" / "scenario_graphs.json"
    if not cache_path.exists():
        raise FileNotFoundError(f"Scenario cache missing: {cache_path}")

    data = json.loads(cache_path.read_text())
    for s in data:
        assert s["id"] in ("A", "B", "C"), f"Unexpected scenario id: {s['id']}"
        assert s["result"]["nodes"], f"Scenario {s['id']} has no nodes"
        assert s["result"]["edges"], f"Scenario {s['id']} has no edges"

    context.log.info(
        f"Scenario cache OK: "
        + ", ".join(
            f"{s['id']} ({len(s['result']['nodes'])} nodes)"
            for s in data
        )
    )
