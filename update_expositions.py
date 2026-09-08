"""
This script does a monthly update based on Suricate data.
it only impacts the back-end but affects the matching in the pipeline.
A person from Suricate provides an extraction in Excel format.
Script steps:
-> deletes the content of the expositions table
-> reads the Excel file,
-> cleans the data
-> writes data into expositions table.
"""


import os
import warnings

import pandas as pd
from sqlalchemy import text

from common.libs.db_manager import DbManager
from common.logging_utils import get_logger
from common.libs.s3 import S3


logger = get_logger()
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module='openpyxl.styles.stylesheet'
)


def main():
    logger.info("Script started.")

    updates_folder = os.environ["UPDATES_FOLDER"]
    suricate_file_name = os.environ["SURICATE_FILE_NAME"]
    suricate_file_path = f"{updates_folder}/{suricate_file_name}"

    s3 = S3()
    logger.info("Downloading Suricate xlsx file.")
    s3.download(suricate_file_path)
    logger.info("Done, treating Suricate xlsx file.")
    xlsx_file_path = os.path.join(s3.download_path, suricate_file_path)

    logger.info("Reading data from input file.")
    df_suricate = pd.read_excel(
        xlsx_file_path,
        skiprows=8,
        usecols=[
            "Type de Tiers",
            "Identifiant groupe",
            "Libéllé groupe",
            "Identifiant sous-groupe",
            "Libellé sous-groupe",
            "Identifiant tiers",
            "SIREN",
            "Raison Sociale",
            "Exposition",
            "Segment Mac Donough",
        ],
    )
    logger.info("Reading data done")
    logger.info(f"df_suricate.shape : {df_suricate.shape}")
    df = df_suricate.copy()
    logger.info("Renaming columns")
    df.rename(
        columns={
            "Type de Tiers": "TYPE",
            "Identifiant groupe": "ID_GROUPE",
            "Libéllé groupe": "LIBELLE_GROUPE",
            "Identifiant sous-groupe": "ID_SOUS_GROUPE",
            "Libellé sous-groupe": "LIBELLE_SOUS_GROUPE",
            "Identifiant tiers": "ID_FEDERAL",
            "Raison Sociale": "RAISON_SOCIALE",
            "Exposition": "EXPOSITION",
            "Segment Mac Donough": "MAC_DONOUGH",
        },
        inplace=True,
    )

    # Replace NA values
    logger.info("Replacing NA values.")
    df = df[df["SIREN"] != "20SC22872"]
    for int_col in ["ID_GROUPE", "ID_SOUS_GROUPE", "ID_FEDERAL", "SIREN"]:
        df[int_col] = df[int_col].fillna(0).apply(float).apply(int)
    for str_col in [
        "LIBELLE_GROUPE", "LIBELLE_SOUS_GROUPE",
        "RAISON_SOCIALE", "MAC_DONOUGH"
    ]:
        df[str_col] = df[str_col].fillna("")

    logger.info("Deleting expositions table.")
    con = DbManager().get_db_connection()
    con.execute(text("DELETE FROM expositions;"))
    con.commit()
    con.close()
    logger.info("Done.")

    logger.info("Feeding expositions table with fresh data.")
    con = DbManager().get_db_connection()
    df.to_sql(
        name="expositions",
        con=con,
        if_exists="append",
        method="multi",
        index=False,
        chunksize=10000,
    )
    con.close()
    logger.info("Done.")
    logger.info("Script successfully executed.")


if __name__ == "__main__":
    main()
