from ocre_database_analysis.utilities.topsy import Topsy
import ocre_database_analysis.constants as c

import pandas as pd


if __name__ == "__main__":
    print("Connecting to database...")
    client = Topsy.try_postgres_connection("ocre")

    print("Starting to export tables...")
    schema_name = "fnd_web_scrape"
    table_export_list = (
        ("fnd_coins", "coin_id"),
        ("fnd_denominations", "coin_id"),
        ("fnd_issuer_names", "coin_id"),
        ("fnd_materials", "coin_id"),
        ("fnd_mints", "coin_id"),
        ("fnd_regions", "coin_id"),
        ("fnd_authority_names", "coin_id"),
        ("fnd_entities", "coin_id"),
        ("fnd_examples", "examples_id"),
        ("fnd_examples_images", "examples_images_id"),
    )
    for table, sort_col in table_export_list:
        print(f"Exporting `{table}` table...")
        df = client.table_to_pd(schema_name, table)
        df.sort_values(sort_col).to_csv(
            c.DATA_FOLDER / "export" / (table + ".csv"), index=False
        )

    print("Closing database connection...")
    client.close_connection()
