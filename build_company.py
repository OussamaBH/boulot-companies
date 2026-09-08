import gc
import os
import json
import zipfile
import calendar
from pathlib import Path
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import requests
import traceback
import numpy as np
import pandas as pd
import pyarrow as pa
from tqdm.auto import tqdm
from sqlalchemy import text
import pyarrow.parquet as pq
import pyarrow.compute as pc

from rapidfuzz import fuzz, process
from common.libs.db_manager import DbManager
from common.libs.s3 import S3
from common.logging_utils import get_logger
from common.libs.solr import Solr


logger = get_logger()
s3 = S3()
workdir = os.path.join(os.environ["S3_DOWNLOAD_PATH"], "updates")
bpce_sectors_filename = "nomenclature_bpce.xls"
SOLR_COMPANIES_COLLECTION = 'companies_alias'

def log_error(logger, message: str, exception: Exception):
    """Simple error logging with file and line number"""
    tb = traceback.extract_tb(exception.__traceback__)[-1]
    logger.error(f"{message} | {type(exception).__name__}: {str(exception)} | {tb.filename}:{tb.lineno}")

def remove_companies_from_solr(company_ids_to_remove):
    logger.info(f"Removing {len(company_ids_to_remove)} company from Solr")
    solr = Solr(SOLR_COMPANIES_COLLECTION)
    ok, ko = (0, 0)
    for company_id in company_ids_to_remove:
        try:
            logger.info(f"Deleting company {company_id} from Solr.")
            solr.delete_document(str(company_id))
        except Exception as e:
            log_error(logger, f"Unable to delete company {company_id} from Solr.", e)
            ko += 1
            continue
        ok += 1
    logger.info(f"deleted companies (ok, ko) : {(ok, ko)}")


def remove_rft_duplicate_inplace(df_rft: pd.DataFrame, last_date_arrete):

    df_rft.drop_duplicates(subset=["ID_FEDERAL"], keep="first", inplace=True, ignore_index=True)

    federal_ids_to_remove = []

    # First, we remove the companies which are SUC or not JM
    df_rft_with_siren = df_rft.dropna(subset=["SIREN"])
    df_duplicates = df_rft_with_siren[df_rft_with_siren.duplicated(subset=["SIREN"], keep=False)].copy()
    print(f"{df_duplicates.shape=}")
    # Keep rows that are JM (non-SUC) or are grappage links with the latest date arrete.
    # This replaces groupby().apply() which has unstable index behaviour across pandas versions.
    mask_keep = (
        ((df_duplicates["PERSO_JURIDIQUE_CODE"] == "JM") & (df_duplicates["DETAIL_PERSO_JURIDIQUE"] != "SUC"))
        | (
            df_duplicates["NATURE_LIEN_GRAPPAGE_CODE"].isin(["01", "02"])
            & (df_duplicates["DATE_ARRETE"].map(str) == last_date_arrete)
        )
    )
    # Among the SIRENs that have at least one row matching mask_keep, drop the non-matching rows.
    # SIRENs where NO row matches are kept entirely (they will be handled by later dedup steps).
    sirens_with_kept_row = set(df_duplicates.loc[mask_keep, "SIREN"])
    df_processed = df_duplicates[
        mask_keep | ~df_duplicates["SIREN"].isin(sirens_with_kept_row)
    ].copy()
    federal_ids_to_remove.extend(set(df_duplicates["ID_FEDERAL"]).difference(df_processed["ID_FEDERAL"]))

    # Secondly, we remove companies that aren't up to date
    df_processed_duplicates = df_processed[df_processed.duplicated(subset="SIREN", keep=False)].copy()
    df_processed2 = df_processed_duplicates[
        (df_processed_duplicates["DATE_ARRETE"].map(str) == last_date_arrete)
        | (
            df_processed_duplicates["NATURE_LIEN_GRAPPAGE_CODE"].isin(["01", "02"])
            & (df_processed_duplicates["DATE_ARRETE"].map(str) == last_date_arrete)
        )
    ].copy()
    federal_ids_to_remove.extend(set(df_processed_duplicates["ID_FEDERAL"]).difference(df_processed2["ID_FEDERAL"]))

    # Lastly, we keep the companies having the oldest registration date
    df_processed2_duplicates = df_processed2[df_processed2.duplicated(subset="SIREN", keep=False)].copy()
    siren_to_federal_id = {}
    siren_to_min_date = {}
    for _, row in df_processed2_duplicates.iterrows():
        siren = row["SIREN"]
        row_date = row["DATE_IMMATRICULATION_ENTREPRISE"]
        if not row_date:
            row_date = date(9999, 1, 1)
        if row_date <= siren_to_min_date.get(siren, date(9999, 1, 1)):
            siren_to_min_date[siren] = row_date
            siren_to_federal_id[siren] = row["ID_FEDERAL"]
    df_processed3 = df_processed2_duplicates[
        df_processed2_duplicates["ID_FEDERAL"].isin(siren_to_federal_id.values())
        | (
            df_processed2_duplicates["NATURE_LIEN_GRAPPAGE_CODE"].isin(["01", "02"])
            & (df_processed2_duplicates["DATE_ARRETE"].map(str) == last_date_arrete)
        )
    ].copy()
    federal_ids_to_remove.extend(set(df_processed2_duplicates["ID_FEDERAL"]).difference(df_processed3["ID_FEDERAL"]))

    df_processed3_duplicates = df_processed3[df_processed3.duplicated(subset="SIREN", keep=False)].copy()
    federal_ids_to_null = df_processed3_duplicates[~df_processed3_duplicates["NATURE_LIEN_GRAPPAGE_CODE"].isin(["01", "02"])]["ID_FEDERAL"]
    df_rft.loc[df_rft[df_rft["ID_FEDERAL"].isin(federal_ids_to_null)].index, "SIREN"] = None
    indexes_to_remove = df_rft[df_rft["ID_FEDERAL"].isin(federal_ids_to_remove)].index
    df_rft.drop(index=indexes_to_remove, inplace=True)


def overwrite_col_vectorized(df: pd.DataFrame, col: str, d: dict) -> pd.Series:
    """Vectorized replacement of overwrite_col_with_dict: overwrites col with dict value when present and non-empty."""
    mapped = df["SIREN"].map(d)
    return mapped.where(mapped.notna() & (mapped != ""), df[col])


def update_col_vectorized(df: pd.DataFrame, col: str, d: dict) -> pd.Series:
    """Vectorized replacement of update_col_with_dict: fills col from dict only when col is currently empty."""
    mapped = df["SIREN"].map(d)
    is_empty = df[col] == ""
    return df[col].where(~is_empty, mapped.where(mapped.notna() & (mapped != ""), df[col]))


def update_aliases(row: pd.Series, d: dict, siren_rft_in):
    siren = row["SIREN"]
    aliases = list(row["ALIAS_INSEE"])
    if siren in siren_rft_in and siren in d:
        aliases.extend(d[siren])
    return aliases


def update_rs(row: pd.Series, d: dict, siren_rft_in):
    siren = row["SIREN"]
    rs = row["RAISON_SOCIALE"]
    if siren in siren_rft_in and siren in d:
        rs = d[siren]
    return rs


# Taille maximale du batch de choices passé à chaque appel cdist.
# RAM par appel = n_queries x CDIST_CHOICES_BATCH x 4 octets (float32).
# Exemple : 10 000 queries x 50 000 choices x 4 = 2 Go par appel.
# Avec CDIST_WORKERS=2 : pic à ~4 Go pour cette étape.
# Réduire CDIST_CHOICES_BATCH si la mémoire est insuffisante.
CDIST_CHOICES_BATCH: int = int(os.environ.get("CDIST_CHOICES_BATCH", 50_000))
CDIST_WORKERS: int = int(os.environ.get("CDIST_WORKERS", 1))


def match_aliases(df_contreparties, df_factiva):
    """Match chaque raison sociale contre les descripteurs Factiva via cdist par batch de choices.

    L'axe choices est découpé en tranches de CDIST_CHOICES_BATCH entrées maximum,
    ce qui borne la RAM allouée par appel à (n_queries x CDIST_CHOICES_BATCH x 4 octets)
    quel que soit le nombre total de descripteurs Factiva.
    CDIST_WORKERS contrôle le parallélisme interne à chaque appel cdist.
    Le meilleur match global est conservé pour chaque query.
    Les deux paramètres sont configurables via variables d'environnement.
    """
    queries = df_contreparties["RAISON_SOCIALE"].apply(str.lower).tolist()
    choices_all = df_factiva["Descriptor"].apply(str.lower).tolist()
    n_choices = len(choices_all)

    # Meilleur score et index global pour chaque query.
    best_score = np.zeros(len(queries), dtype=np.float32)
    best_choice_idx = np.full(len(queries), -1, dtype=np.intp)

    for batch_start in range(0, n_choices, CDIST_CHOICES_BATCH):
        choices_batch = choices_all[batch_start: batch_start + CDIST_CHOICES_BATCH]

        # scores : matrice (n_queries x len(choices_batch)), dtype float32
        scores = process.cdist(
            queries=queries,
            choices=choices_batch,
            score_cutoff=96,
            workers=CDIST_WORKERS,
        )

        batch_max = scores.max(axis=1)        # meilleur score du batch par query
        batch_argmax = scores.argmax(axis=1)  # index local dans le batch
        del scores                            # libère la matrice avant le prochain batch

        improved = batch_max > best_score
        best_score = np.where(improved, batch_max, best_score)
        best_choice_idx = np.where(
            improved,
            batch_start + batch_argmax,  # conversion en index global
            best_choice_idx,
        )

    for row_idx, aliases_idx in enumerate(best_choice_idx):
        if aliases_idx == -1 or not df_contreparties.at[row_idx, "RAISON_SOCIALE"].strip():
            continue
        factiva_row = df_factiva.iloc[aliases_idx]
        aliases = df_contreparties.at[row_idx, "ALIAS"]
        aliases.append(factiva_row["Descriptor"])
        aliases.append(factiva_row["Company_code"].strip())
        aliases.extend([a.strip() for a in factiva_row["Aliases"].split("| ") if a])
        aliases.extend([a.strip() for a in factiva_row["AutoAliases"].split("| ") if a])
        df_contreparties.at[row_idx, "ALIAS"] = aliases

    return df_contreparties


def clean_aliases(row: pd.Series, col: str) -> list:
    return list(set(x.strip() for x in row[col] if x.strip() and x.upper() != row["RAISON_SOCIALE"].upper()))


def _df_chunks(df: pd.DataFrame, chunk_size: int):
    """Yield successive DataFrame chunks of exactly chunk_size rows."""
    df = df.reset_index(drop=True)
    for start in range(0, max(len(df), 1), chunk_size):
        yield df.iloc[start:start + chunk_size]


def add_aliases(df_contreparties: pd.DataFrame, df_factiva_companies_fr, df_factiva_companies_other) -> pd.DataFrame:
    """Add Factiva aliases to companies by fuzzy-matching RAISON_SOCIALE against Factiva descriptors.

    FR and non-FR companies are matched against their respective Factiva subsets.
    Chunks are yielded lazily (not pre-materialised) to avoid doubling memory.
    """
    df_contreparties_fr = df_contreparties[df_contreparties["PAYS_RESIDENCE_CODE"] == "FR"].copy()
    # Lazy generator — do not call list() to avoid materialising all chunks at once
    df_contreparties_fr = pd.concat(
        [match_aliases(c.reset_index(drop=True), df_factiva_companies_fr)
         for c in tqdm(_df_chunks(df_contreparties_fr, 100_000))],
        axis=0,
    ).reset_index(drop=True)

    df_contreparties_other = df_contreparties[df_contreparties["PAYS_RESIDENCE_CODE"] != "FR"].copy()
    # Lazy generator — do not call list() to avoid materialising all chunks at once
    df_contreparties_other = pd.concat(
        [match_aliases(c.reset_index(drop=True), df_factiva_companies_other)
         for c in tqdm(_df_chunks(df_contreparties_other, 10_000))],
        axis=0,
    ).reset_index(drop=True)

    return pd.concat([df_contreparties_fr, df_contreparties_other], axis=0).reset_index(drop=True)


def download_file(url, target_path):
    logger.info(f"Downloading file {url}")
    try:
        Path(target_path).parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(target_path, 'wb') as fichier:
            for chunk in response.iter_content(chunk_size=8192):
                fichier.write(chunk)
        logger.info(f"File successfully downloaded : {target_path}")
        return True
    except requests.exceptions.RequestException as e:
        log_error(logger, "Error when downloading file", e)
        return False


def create_and_populate_tmp_company_update(df_company_to_update: pd.DataFrame) -> None:
    """
    Create temporary table and populate it with data in chunks to avoid replication issues
    """
    logger.info("Starting tmp_company_update table creation and population")

    # Create the temporary table
    _create_tmp_company_update_table()

    # Insert data in chunks
    _insert_data_in_chunks(df_company_to_update)

    logger.info("tmp_company_update table creation and population completed")


def _create_tmp_company_update_table() -> None:
    """
    Drop and recreate the tmp_company_update table
    """
    con = DbManager().get_db_connection()
    try:
        logger.info("Dropping table tmp_company_update if exists")
        con.execute(text("DROP TABLE IF EXISTS tmp_company_update;"))
        con.commit()

        logger.info("Creating the temporary table tmp_company_update")
        con.execute(text("""
             CREATE TABLE tmp_company_update
             (
                 ID_COMPANY               bigint                 NOT NULL,
                 ID_FEDERAL               bigint      DEFAULT 0  NOT NULL,
                 SIREN                    bigint      DEFAULT 0  NOT NULL,
                 RAISON_SOCIALE           nvarchar(200)  DEFAULT '' NOT NULL,
                 CODE_GROUPE              bigint      DEFAULT 0  NOT NULL,
                 LIBELLE_GROUPE           nvarchar(200)  DEFAULT '' NOT NULL,
                 SECTEUR                  int NULL,
                 PAYS_RESIDENCE_CODE      varchar(2)  DEFAULT '' NOT NULL,
                 CODE_POSTAL              nvarchar(50)  DEFAULT '' NOT NULL,
                 CHIFFRE_AFFAIRES         float       DEFAULT 0  NOT NULL,
                 CHIFFRE_AFFAIRES_C       float       DEFAULT 0  NOT NULL,
                 TIERS_PERE               bigint      DEFAULT 0  NOT NULL,
                 TIERS_PERE_ULTIME        bigint      DEFAULT 0  NOT NULL,
                 ALIAS                    longtext,
                 ALIAS_INSEE              nvarchar(250)  DEFAULT '' NOT NULL,
                 ETAT_ADMINISTRATIF       varchar(1)  DEFAULT '' NOT NULL,
                 EST_CLIENT_ACTIF         bit                    NOT NULL,
                 CATEGORIE_JURIDIQUE_CODE bigint                 NOT NULL,
                 CATEGORIE_JURIDIQUE_LIB  nvarchar(200)  NOT NULL,
                 CATEGORIE_ENTREPRISE     varchar(3)             NOT NULL,
                 DATE_CREATION            varchar(10) DEFAULT '' NOT NULL,
                 CONSTRAINT PK__tmp_company_update PRIMARY KEY (ID_COMPANY)
             );
        """))
        con.commit()
        logger.info("tmp_company_update table created successfully")
    except Exception as e:
        log_error(logger, "Error when creating tmp_company_update table", e)
    finally:
        con.close()


def _insert_data_in_chunks(df_company_to_update: pd.DataFrame) -> None:
    """
    Insert DataFrame data in chunks to avoid replication issues
    """
    # Get chunk size from environment variable with default fallback
    chunk_size = int(os.environ.get("CHUNK_UPDATE_COMPANIES", "200"))
    total_records = len(df_company_to_update)

    logger.info(f"Starting insertion of {total_records} records in chunks of {chunk_size}")
    con = DbManager().get_db_connection()
    # Process DataFrame in chunks
    for i in range(0, total_records, chunk_size):
        chunk_start = i
        chunk_end = min(i + chunk_size, total_records)
        chunk_df = df_company_to_update.iloc[chunk_start:chunk_end]

        logger.info(f"Inserting chunk {chunk_start // chunk_size + 1} | "
                    f"Records {chunk_start + 1}-{chunk_end} of {total_records}")

        try:
            chunk_df.to_sql(
                name="tmp_company_update",
                con=con,
                if_exists="append",
                method="multi",
                index=False,
                chunksize=100,  # Internal pandas chunking for SQL generation
            )
            logger.info(f"Successfully inserted chunk with {len(chunk_df)} records")

        except Exception as e:
            log_error(logger, f"Error inserting chunk {chunk_start}-{chunk_end}", e)
            con.close()
            raise

    con.close()
    logger.info(f"Data insertion completed. Total records inserted: {total_records}")


def update_companies_from_tmp_table() -> None:
    """
    Update companies in batches from tmp_company_update table to avoid replication issues
    """
    logger.info("Starting company updates from tmp_company_update table")

    try:
        # Get total number of records to update
        total_records = _get_tmp_table_record_count()
        if total_records == 0:
            logger.info("No records to update in tmp_company_update table")
            return

        # Update companies in batches
        _update_companies_in_batches(total_records)

        # Clean up temporary table
        _drop_tmp_company_update_table()

        logger.info(f"Company updates completed successfully. Total records processed: {total_records}")

    except Exception as e:
        log_error(logger, "Error during company update process", e)
        raise


def _get_tmp_table_record_count() -> int:
    """
    Get the number of records in tmp_company_update table
    """
    con = DbManager().get_db_connection()
    try:
        result = con.execute(text("SELECT COUNT(*) FROM tmp_company_update")).fetchone()
        return result[0] if result else 0
    except Exception as e:
        log_error(logger, "Error when select count from tmp company update", e)
    finally:
        con.close()


def _update_companies_in_batches(total_records: int) -> None:
    """
    Update companies in batches to avoid replication issues
    """
    # Get batch size from environment variable with default fallback
    batch_size = int(os.environ.get("CHUNK_UPDATE_COMPANIES", "1000"))

    logger.info(f"Updating {total_records} companies in batches of {batch_size}")

    offset = 0
    batch_number = 1
    con = DbManager().get_db_connection()

    while offset < total_records:
        current_batch_size = min(batch_size, total_records - offset)

        logger.info(f"Processing batch {batch_number} | "
                    f"Records {offset + 1}-{offset + current_batch_size} of {total_records}")

        try:
            batch_limit = int(current_batch_size)
            current_offset = int(offset)
            # Update query with LIMIT and OFFSET to process in batches
            update_query = f"""
                           UPDATE Company
                               INNER JOIN (
                               SELECT * FROM tmp_company_update
                               ORDER BY ID_COMPANY
                               LIMIT {batch_limit} OFFSET {current_offset}
                               ) AS tmp_batch \
                           ON Company.ID_COMPANY = tmp_batch.ID_COMPANY
                               SET
                                   Company.ID_FEDERAL = tmp_batch.ID_FEDERAL, Company.SIREN = tmp_batch.SIREN, Company.RAISON_SOCIALE = tmp_batch.RAISON_SOCIALE, Company.CODE_GROUPE = tmp_batch.CODE_GROUPE, Company.LIBELLE_GROUPE = tmp_batch.LIBELLE_GROUPE, Company.SECTEUR = tmp_batch.SECTEUR, Company.PAYS_RESIDENCE_CODE = tmp_batch.PAYS_RESIDENCE_CODE, Company.CODE_POSTAL = tmp_batch.CODE_POSTAL, Company.CHIFFRE_AFFAIRES = tmp_batch.CHIFFRE_AFFAIRES, Company.CHIFFRE_AFFAIRES_C = tmp_batch.CHIFFRE_AFFAIRES_C, Company.TIERS_PERE = tmp_batch.TIERS_PERE, Company.TIERS_PERE_ULTIME = tmp_batch.TIERS_PERE_ULTIME, Company.ALIAS = tmp_batch.ALIAS, Company.ALIAS_INSEE = tmp_batch.ALIAS_INSEE, Company.ETAT_ADMINISTRATIF = tmp_batch.ETAT_ADMINISTRATIF, Company.EST_CLIENT_ACTIF = tmp_batch.EST_CLIENT_ACTIF, Company.CATEGORIE_JURIDIQUE_CODE = tmp_batch.CATEGORIE_JURIDIQUE_CODE, Company.CATEGORIE_JURIDIQUE_LIB = tmp_batch.CATEGORIE_JURIDIQUE_LIB, Company.CATEGORIE_ENTREPRISE = tmp_batch.CATEGORIE_ENTREPRISE, Company.DATE_CREATION = tmp_batch.DATE_CREATION, Company.indexed = 0 \
                           """

            result = con.execute(text(update_query))

            affected_rows = result.rowcount
            con.commit()

            logger.info(f"Batch {batch_number} completed successfully. "
                        f"Updated {affected_rows} companies")

        except Exception as e:
            log_error(logger, f"Error updating batch {batch_number} (offset {offset})", e)
            con.rollback()
            con.close()
            raise

        offset += current_batch_size
        batch_number += 1
    con.close()


def _drop_tmp_company_update_table() -> None:
    """
    Drop the temporary table after processing
    """
    con = DbManager().get_db_connection()
    try:
        logger.info("Dropping tmp_company_update table")
        con.execute(text("DROP TABLE IF EXISTS tmp_company_update"))
        con.commit()
        logger.info("tmp_company_update table dropped successfully")
    except Exception as e:
        log_error(logger, "Error when dropping tmp_company_update table", e)
    finally:
        con.close()


def delete_companies_by_ids(company_ids_to_remove: list) -> None:
    """
    Delete companies by IDs in batches to avoid replication issues
    """
    logger.info(f"Starting deletion of {len(company_ids_to_remove)} companies")

    if not company_ids_to_remove:
        logger.info("No companies to delete")
        return

    try:
        # Create temporary table and populate it
        _create_and_populate_tmp_company_delete(company_ids_to_remove)

        # Delete companies in batches
        _delete_companies_in_batches(len(company_ids_to_remove))

        # Remove from Solr
        remove_companies_from_solr(company_ids_to_remove)

        # Clean up temporary table
        _drop_tmp_company_delete_table()

        logger.info(f"Company deletion completed successfully. Total companies deleted: {len(company_ids_to_remove)}")

    except Exception as e:
        log_error(logger, "Error during company deletion process", e)
        raise


def _create_and_populate_tmp_company_delete(company_ids_to_remove: list) -> None:
    """
    Create temporary table and populate it with company IDs to delete
    """
    # Create the temporary table
    con = DbManager().get_db_connection()
    try:
        logger.info("Dropping table tmp_company_delete if exists")
        con.execute(text("DROP TABLE IF EXISTS tmp_company_delete;"))
        con.commit()

        logger.info("Creating tmp_company_delete table")
        con.execute(text("""
                         CREATE TABLE tmp_company_delete
                         (
                             ID_COMPANY bigint NOT NULL,
                             CONSTRAINT PK__tmp_company_delete PRIMARY KEY (ID_COMPANY)
                         );
                         """))
        con.commit()
        logger.info("tmp_company_delete table created successfully")
    except Exception as e:
        log_error(logger, "Error when Dropping table tmp_company_delete", e)
    finally:
        con.close()

    # Insert data in chunks
    _insert_delete_ids_in_chunks(company_ids_to_remove)


def _insert_delete_ids_in_chunks(company_ids_to_remove: list) -> None:
    """
    Insert company IDs in chunks to avoid replication issues
    """
    # Get chunk size from environment variable with default fallback
    chunk_size = int(os.environ.get("CHUNK_UPDATE_COMPANIES", "1000"))
    total_ids = len(company_ids_to_remove)

    logger.info(f"Deleting {total_ids} company IDs in chunks of {chunk_size}")
    con = DbManager().get_db_connection()
    # Process IDs in chunks
    for i in range(0, total_ids, chunk_size):
        chunk_start = i
        chunk_end = min(i + chunk_size, total_ids)
        chunk_ids = company_ids_to_remove[chunk_start:chunk_end]

        logger.info(f"Deleting chunk {chunk_start // chunk_size + 1} | "
                    f"IDs {chunk_start + 1}-{chunk_end} of {total_ids}")

        # Create DataFrame for this chunk
        df_chunk = pd.DataFrame(data={"ID_COMPANY": chunk_ids})

        try:
            df_chunk.to_sql(
                name="tmp_company_delete",
                con=con,
                if_exists="append",
                method="multi",
                index=False,
                chunksize=100,
            )
            logger.info(f"Successfully inserted chunk with {len(chunk_ids)} IDs")

        except Exception as e:
            log_error(logger, f"Error Deleting chunk {chunk_start}-{chunk_end}", e)
            logger.error()
            con.close()
            raise

    con.close()


def _delete_companies_in_batches(total_records: int) -> None:
    """
    Delete companies in batches to avoid replication issues
    """
    batch_size = int(os.environ.get("CHUNK_UPDATE_COMPANIES", "100"))
    logger.info(f"Deleting companies in batches of {batch_size}")

    offset = 0
    batch_number = 1
    total_deleted = 0
    con = DbManager().get_db_connection()

    while offset < total_records:
        current_batch_size = min(batch_size, total_records - offset)

        logger.info(f"Processing deletion batch {batch_number} | "
                    f"Records {offset + 1}-{offset + current_batch_size} of {total_records}")

        try:

            select_query = f"""
                           SELECT ID_COMPANY
                           FROM tmp_company_delete
                           ORDER BY ID_COMPANY
                               LIMIT {current_batch_size} OFFSET {offset}
                           """

            result = con.execute(text(select_query))
            ids_to_delete = [row[0] for row in result.fetchall()]

            if not ids_to_delete:
                break

            ids_string = ','.join(map(str, ids_to_delete))
            delete_query = f"""
                DELETE FROM Company
                WHERE ID_COMPANY IN ({ids_string})
            """
            result = con.execute(text(delete_query))

            affected_rows = result.rowcount
            con.commit()
            total_deleted += affected_rows

            logger.info(f"Deletion batch {batch_number} completed successfully. "
                        f"Deleted {affected_rows} companies (Total: {total_deleted})")

        except Exception as e:
            log_error(logger, f"Error deleting batch {batch_number} (offset {offset})", e)
            con.rollback()
            con.close()
            raise

        offset += current_batch_size
        batch_number += 1

    con.close()


def _drop_tmp_company_delete_table() -> None:
    """
    Drop the temporary delete table after processing
    """
    con = DbManager().get_db_connection()
    try:
        logger.info("Dropping tmp_company_delete table")
        con.execute(text("DROP TABLE IF EXISTS tmp_company_delete"))
        con.commit()
        logger.info("tmp_company_delete table dropped successfully")
    except Exception as e:
        log_error(logger, "Error when Dropping tmp_company_delete table", e)
    finally:
        con.close()


def add_companies_in_batches(df_company_to_append: pd.DataFrame) -> None:
    """
    Add new companies in batches to avoid replication issues
    """
    if df_company_to_append.empty:
        logger.info("No companies to add")
        return

    total_records = len(df_company_to_append)
    logger.info(f"Starting addition of {total_records} new companies")

    try:
        # Clean the dataframe
        df_cleaned = _clean_dataframe_for_insertion(df_company_to_append)

        # Insert companies in batches
        _insert_companies_in_batches(df_cleaned, total_records)

        logger.info(f"Company addition completed successfully. Total companies added: {total_records}")

    except Exception as e:
        log_error(logger, "Error during company addition process", e)
        raise


def _clean_dataframe_for_insertion(df_company_to_append: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the dataframe by replacing infinite values
    """
    logger.info("Cleaning dataframe: replacing infinite values with 0")
    df_cleaned = df_company_to_append.replace([np.inf, -np.inf], 0)
    return df_cleaned


def _insert_companies_in_batches(df_company_to_append: pd.DataFrame, total_records: int) -> None:
    """
    Insert companies in batches to avoid replication issues
    """
    # Get batch size from environment variable with default fallback
    batch_size = int(os.environ.get("CHUNK_UPDATE_COMPANIES", "1000"))

    logger.info(f"Inserting {total_records} companies in batches of {batch_size}")
    con = DbManager().get_db_connection()
    # Process DataFrame in chunks
    for i in range(0, total_records, batch_size):
        chunk_start = i
        chunk_end = min(i + batch_size, total_records)
        chunk_df = df_company_to_append.iloc[chunk_start:chunk_end]

        batch_number = chunk_start // batch_size + 1
        logger.info(f"Inserting batch {batch_number} | "
                    f"Records {chunk_start + 1}-{chunk_end} of {total_records}")

        try:
            chunk_df.to_sql(
                name="company",
                con=con,
                if_exists="append",
                method="multi",
                index=False,
                chunksize=100,  # Internal pandas chunking for SQL generation
            )

            logger.info(f"Successfully inserted batch {batch_number} with {len(chunk_df)} companies")

        except Exception as e:
            log_error(logger, f"Error inserting batch {batch_number} (records {chunk_start}-{chunk_end})", e)
            con.close()
            raise
    con.close()

def main():

    s3.download(f"updates/{bpce_sectors_filename}")
    df_bpce_sectors = pd.read_excel(
        os.path.join(workdir, bpce_sectors_filename)
    )
    df_bpce_sectors = df_bpce_sectors.rename(
        columns={
            "Code NAF V2": "codeNAF",
            "NEW Nouveau sous-secteur": "sousSecteurBPCE",
            "NEW Nouveau secteur": "secteurBPCE",
            "NEW Code sous-secteur": "codeSousSecteurBPCE"
        }
    )
    df_bpce_sectors["secteurBPCE"] = df_bpce_sectors["secteurBPCE"].replace("TRANSPORT", "TRANSPORT - AUTRES")
    df_bpce_sectors = df_bpce_sectors.map(lambda x: str.strip(x) if isinstance(x, str) else x)
    logger.info(f"df_bpce_sectors.shape: {df_bpce_sectors.shape}")
    naf_to_sector = dict(zip(df_bpce_sectors["codeNAF"], df_bpce_sectors["secteurBPCE"]))
    subsector_code_to_sector = dict(zip(df_bpce_sectors["codeSousSecteurBPCE"], df_bpce_sectors["secteurBPCE"]))
    subsector_code_to_sector["A5"]

    # ### Load the revenues data generated from the INPI batch

    # Read revenues extracted from INPI
    siren_to_revenues_filename = "siren_to_revenues.json"
    s3.download(f"updates/{siren_to_revenues_filename}")

    siren_to_revenues = json.load(open(
        os.path.join(workdir, siren_to_revenues_filename), "r"
    ))
    siren_to_revenues = {int(k):v for k, v in siren_to_revenues.items()}

    logger.info(f"len(siren_to_revenues) : {len(siren_to_revenues)}")
    df_revenues = pd.DataFrame(data=siren_to_revenues).T
    df_revenues.reset_index(inplace=True)
    df_revenues.rename(
        columns={
            "index": "siren",
            0: "date_cloture",
            1: "durée_exercice",
            2: "chiffre_affaires",
            3: "date_dépôt",
            4: "devise"
        },
        inplace=True
    )

    # Same thing but with consolidated revenues
    siren_to_consolidated_revenues_filename = "siren_to_consolidated_revenues.json"
    s3.download(f"updates/{siren_to_consolidated_revenues_filename}")
    siren_to_consolidated_revenues = json.load(
        open(
            os.path.join(workdir, siren_to_consolidated_revenues_filename), "r"
        )
    )
    siren_to_consolidated_revenues = {
        int(k):v
        for k, v in siren_to_consolidated_revenues.items()
    }
    logger.info(f"len(siren_to_consolidated_revenues): {len(siren_to_consolidated_revenues)}")
    df_consolidated_revenues = pd.DataFrame(data=siren_to_consolidated_revenues).T
    df_consolidated_revenues.reset_index(inplace=True)
    df_consolidated_revenues.rename(
        columns={
            "index": "siren",
            0: "date_cloture",
            1: "durée_exercice",
            2: "chiffre_affaires",
            3: "date_dépôt",
            4: "devise"
        },
        inplace=True
    )

    df_revenues.head(2)

    # Download the INSEE datasets
    # This code downloads the zip files from the INSEE and unzip them. They each contain a csv file
    # Since this code was written, INSEE now also publishes parquet files.
    """
    urls = [
        "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissement_utf8.zip",
        "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockUniteLegaleHistorique_utf8.zip",
        "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockUniteLegale_utf8.zip",
    ]
    for url in urls:
        archive_name = os.path.basename(url)
        archive_path = os.path.join(workdir, archive_name)
        download_file(url, archive_path)
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            unzipped_file = zip_ref.namelist()[0]
            zip_ref.extractall(workdir)
        os.remove(archive_path)
    """
    dataset_filenames = [
        os.environ["STOCK_ESTABLISHMENT_FILE_NAME"],
        os.environ["STOCK_LEGAL_UNIT_HISTORY_FILE_NAME"],
        os.environ["STOCK_LEGAL_UNIT_FILE_NAME"]
    ]
    for archive_name in dataset_filenames:
        logger.info(f"Downloading archive {archive_name} from S3.")
        s3.download(f"updates/{archive_name}")
        archive_path = os.path.join(workdir, archive_name)
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            logger.info("Unzipping archive.")
            zip_ref.extractall(workdir)
        os.remove(archive_path)

    # Load and prepare the INSEE datasets
    # In this section, the three files downloaded from INSEE are parsed and merged together
    # in order to obtain a final dataframe, `df_insee`.
    # Load INSEE's country codes nomenclature
    # This shouldn't change over time
    countries_filename = "v_pays_2023.csv"
    s3.download(f"updates/{countries_filename}")
    df_insee_countries = pd.read_csv(
        os.path.join(workdir, countries_filename)
    )
    df_insee_countries = df_insee_countries[df_insee_countries["ACTUAL"] == 1]
    code_to_isocode2 = dict(zip(df_insee_countries["COG"], df_insee_countries["CODEISO2"]))
    logger.info(f"len(code_to_isocode2): {len(code_to_isocode2)}")

    # --- Étape 1 : StockUniteLegaleHistorique (9,43 Go CSV) ---
    # Lecture par chunks : on ne conserve qu'une ligne par siren en mémoire (la plus récente).
    logger.info("Chargement de StockUniteLegaleHistorique (chunked, 1 ligne/siren)...")
    ul_hist_usecols = [
        "siren", "denominationUniteLegale", "dateFin", "activitePrincipaleUniteLegale",
        "categorieJuridiqueUniteLegale", "etatAdministratifUniteLegale", "nicSiegeUniteLegale"
    ]
    last_rows: dict = {}
    for chunk in pd.read_csv(
        filepath_or_buffer=os.path.join(workdir, "StockUniteLegaleHistorique_utf8.csv"),
        usecols=ul_hist_usecols,
        chunksize=500_000,
        dtype={"siren": "int64"},
        low_memory=False,
    ):
        chunk.sort_values(["siren", "dateFin"], inplace=True)
        chunk_last = chunk.drop_duplicates("siren", keep="last")
        for row in chunk_last.itertuples(index=False):
            existing = last_rows.get(row.siren)
            if existing is None or str(row.dateFin) >= str(existing["dateFin"]):
                last_rows[row.siren] = row._asdict()
    df_unite_legale_hist = pd.DataFrame(last_rows.values())
    df_unite_legale_hist.sort_values("siren", inplace=True, ignore_index=True)
    del last_rows
    gc.collect()
    logger.info(f"df_unite_legale_hist.shape: {df_unite_legale_hist.shape}")

    # Keep only the last updated version of a legal entity
    df_ul = df_unite_legale_hist.drop_duplicates("siren", keep="last").copy()
    del df_unite_legale_hist
    gc.collect()

    # Remove individual entrepreneur
    df_ul = df_ul[df_ul["categorieJuridiqueUniteLegale"] != 1000.0]
    # Remove legal entities without a valid legal name
    df_ul = df_ul[df_ul["denominationUniteLegale"] != "[ND]"]
    df_ul = df_ul[~df_ul["denominationUniteLegale"].isna()]

    # --- Étape 2 : StockUniteLegale (3,94 Go CSV) ---
    # Lecture par chunks → dicts directs, sans DataFrame intermédiaire en mémoire.
    logger.info("Chargement de StockUniteLegale via dicts (chunked)...")
    siren_to_date_creation: dict = {}
    siren_to_categorie_entreprise: dict = {}
    for chunk in pd.read_csv(
        filepath_or_buffer=os.path.join(workdir, "StockUniteLegale_utf8.csv"),
        usecols=["siren", "dateCreationUniteLegale", "categorieEntreprise"],
        chunksize=500_000,
        dtype={"siren": "int64", "dateCreationUniteLegale": "str", "categorieEntreprise": "str"},
        low_memory=False,
    ):
        siren_to_date_creation.update(zip(chunk["siren"], chunk["dateCreationUniteLegale"]))
        siren_to_categorie_entreprise.update(zip(chunk["siren"], chunk["categorieEntreprise"]))
    logger.info(f"len(siren_to_date_creation): {len(siren_to_date_creation)}")

    df_ul["dateCreationUniteLegale"] = df_ul["siren"].map(siren_to_date_creation).fillna("")
    df_ul["categorieEntreprise"] = df_ul["siren"].map(siren_to_categorie_entreprise).fillna("").astype("category")
    del siren_to_date_creation, siren_to_categorie_entreprise
    gc.collect()

    # Apply the sectorial nomenclature
    df_ul["activitePrincipaleUniteLegale"] = df_ul["activitePrincipaleUniteLegale"].str.replace(".", "")
    df_ul["secteurBPCE"] = df_ul["activitePrincipaleUniteLegale"].map(naf_to_sector).fillna("")

    # Build the siret number (to merge with the establishments dataset)
    df_ul["siret"] = df_ul["siren"].apply(str) + df_ul["nicSiegeUniteLegale"].astype(int).apply(str).apply(
        lambda x: "0" * (5 - len(x)) + x
    )
    df_ul["siret"] = df_ul["siret"].map(int)

    df_ul.drop(columns=["activitePrincipaleUniteLegale", "dateFin", "nicSiegeUniteLegale"], inplace=True)
    df_ul = df_ul.rename(
        columns={
            "etatAdministratifUniteLegale": "ETAT_ADMINISTRATIF",
            "denominationUniteLegale": "RAISON_SOCIALE",
            "secteurBPCE": "SECTEUR",
            "siren": "SIREN",
            "categorieJuridiqueUniteLegale": "CATEGORIE_JURIDIQUE_CODE",
            "dateCreationUniteLegale": "DATE_CREATION",
            "categorieEntreprise": "CATEGORIE_ENTREPRISE"
        }
    )
    logger.info(f"df_ul.shape: {df_ul.shape}")

    # --- Étape 3 : StockEtablissement (9,26 Go CSV) ---
    # Lecture par chunks filtrés sur les sirets utiles + écriture dans un parquet temporaire
    # pour éviter d'accumuler tous les chunks filtrés en RAM avant le concat.
    #
    # INSEE occasionally renames columns between publications. We detect the actual column
    # names from the CSV header before starting the chunked read to avoid a misleading
    # pandas IndexError (pandas bug #44106) when a usecols entry is missing.
    etab_csv_path = os.path.join(workdir, "StockEtablissement_utf8.csv")
    etab_header = pd.read_csv(etab_csv_path, nrows=0).columns.tolist()
    logger.info(f"StockEtablissement columns (first 20): {etab_header[:20]}")

    # enseigne1Etablissement was renamed to enseigne1UniteLegale in some releases
    _ENSEIGNE_CANDIDATES = ["enseigne1Etablissement", "enseigne1UniteLegale"]
    col_enseigne = next((c for c in _ENSEIGNE_CANDIDATES if c in etab_header), None)
    if col_enseigne is None:
        raise ValueError(
            f"Cannot find enseigne column in StockEtablissement. "
            f"Tried: {_ENSEIGNE_CANDIDATES}. Available columns: {etab_header}"
        )

    # denominationUsuelleEtablissement is stable but guard it too
    _DENOM_CANDIDATES = ["denominationUsuelleEtablissement", "denominationUsuelle1Etablissement"]
    col_denom = next((c for c in _DENOM_CANDIDATES if c in etab_header), None)
    if col_denom is None:
        raise ValueError(
            f"Cannot find denomination column in StockEtablissement. "
            f"Tried: {_DENOM_CANDIDATES}. Available columns: {etab_header}"
        )

    logger.info(f"Using StockEtablissement columns: enseigne='{col_enseigne}', denomination='{col_denom}'")

    sirets_to_keep = set(df_ul["siret"].values)
    logger.info(f"Nombre de sirets à filtrer : {len(sirets_to_keep)}")
    etab_tmp_path = os.path.join(workdir, "_etab_tmp.pq")
    etab_writer = None
    for chunk in pd.read_csv(
        filepath_or_buffer=etab_csv_path,
        usecols=[
            "siret", col_enseigne, col_denom,
            "codePostalEtablissement", "codePaysEtrangerEtablissement"
        ],
        chunksize=500_000,
        dtype={"siret": "int64", "codePostalEtablissement": "str"},
        low_memory=False,
    ):
        chunk = chunk[chunk["siret"].isin(sirets_to_keep)]
        if chunk.empty:
            continue
        chunk["PAYS_RESIDENCE_CODE"] = (
            chunk["codePaysEtrangerEtablissement"]
            .fillna(0).apply(int).apply(str)
            .map(code_to_isocode2).fillna("FR")
        )
        chunk.drop(columns=["codePaysEtrangerEtablissement"], inplace=True)
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if etab_writer is None:
            etab_writer = pq.ParquetWriter(etab_tmp_path, table.schema)
        etab_writer.write_table(table)
    if etab_writer:
        etab_writer.close()
    del sirets_to_keep
    gc.collect()

    df_etab = pd.read_parquet(etab_tmp_path) if etab_writer else pd.DataFrame()
    os.remove(etab_tmp_path)
    df_etab = df_etab[df_etab["siret"].isin(df_ul["siret"])]
    df_etab = df_etab.rename(
        columns={
            col_enseigne: "ENSEIGNE_SIEGE_SOCIAL",
            col_denom: "DENOMINATION_USUELLE_SIEGE_SOCIAL",
            "codePostalEtablissement": "CODE_POSTAL"
        }
    )
    logger.info(f"df_etab.shape: {df_etab.shape}")

    # --- Étape 4 : merge et construction de df_insee ---
    df_insee = df_ul.merge(right=df_etab, how="left", on="siret")
    del df_ul, df_etab
    gc.collect()

    # This is necessary because not all postal codes are integers
    df_insee["CODE_POSTAL"] = df_insee["CODE_POSTAL"].fillna("").replace("[ND]", "").apply(
        lambda x: str(int(x)) if isinstance(x, float) else str(x)
    )

    df_insee["CATEGORIE_JURIDIQUE_CODE"] = df_insee["CATEGORIE_JURIDIQUE_CODE"].astype(int)

    # Clean and fill columns
    for str_col in [
        "ENSEIGNE_SIEGE_SOCIAL", "PAYS_RESIDENCE_CODE", "DENOMINATION_USUELLE_SIEGE_SOCIAL",
        "CATEGORIE_ENTREPRISE", "DATE_CREATION"
    ]:
        df_insee[str_col] = df_insee[str_col].fillna("")

    df_insee["ALIAS_INSEE"] = df_insee.apply(
        lambda x: [x["ENSEIGNE_SIEGE_SOCIAL"], x["DENOMINATION_USUELLE_SIEGE_SOCIAL"]],
        axis=1
    )
    df_insee["ALIAS_INSEE"] = df_insee["ALIAS_INSEE"].apply(lambda x: [e for e in x if e])

    df_insee["CHIFFRE_AFFAIRES"] = pd.to_numeric(
        df_insee["SIREN"].apply(lambda x: siren_to_revenues.get(x, [0, 1, 0])[2]), errors="coerce"
    ).fillna(0)
    df_insee["DUREE_EXERCICE"] = pd.to_numeric(
        df_insee["SIREN"].apply(lambda x: siren_to_revenues.get(x, [0, 1, 0])[1]), errors="coerce"
    ).fillna(1).replace(0, 1).astype(int)
    df_insee["CHIFFRE_AFFAIRES"] = df_insee["CHIFFRE_AFFAIRES"] * (12 / df_insee["DUREE_EXERCICE"])
    df_insee["CHIFFRE_AFFAIRES_C"] = pd.to_numeric(
        df_insee["SIREN"].apply(lambda x: siren_to_consolidated_revenues.get(x, [0, 1, 0])[2]), errors="coerce"
    ).fillna(0)
    df_insee["DUREE_EXERCICE_C"] = pd.to_numeric(
        df_insee["SIREN"].apply(lambda x: siren_to_consolidated_revenues.get(x, [0, 1, 0])[1]), errors="coerce"
    ).fillna(1).replace(0, 1).astype(int)
    df_insee["CHIFFRE_AFFAIRES_C"] = df_insee["CHIFFRE_AFFAIRES_C"] * (12 / df_insee["DUREE_EXERCICE_C"])

    df_insee.drop(
        columns=[
            "siret", "ENSEIGNE_SIEGE_SOCIAL", "DENOMINATION_USUELLE_SIEGE_SOCIAL", "DUREE_EXERCICE", "DUREE_EXERCICE_C"
        ],
        inplace=True
    )
    logger.info(f"df_insee.shape: {df_insee.shape}")

    # Load and prepare RFT
    filepath = os.path.join(workdir, "data/risk/df_rft.pq")
    table = pq.read_table(filepath).select(["ID_FEDERAL", "DACREE"])
    table = table.filter(
        pc.greater_equal(table["DACREE"], pa.scalar(pd.Timestamp.min))
    )
    federal_id_to_dacree = table.to_pandas().set_index("ID_FEDERAL").to_dict()["DACREE"]

    df_rft_all = pd.read_parquet(
        path=filepath,
        columns=[
            "SOUS_SECTEUR_ACTIVITE_CODE", "CHIFFRE_AFFAIRES", "DUREE_EXERCICE", "DATE_BILAN", "CHIFFRE_AFFAIRES_C",
            "DUREE_EXERCICE_C", "DATE_BILAN_C", "ID_FEDERAL", "SIREN", "RAISON_SOCIALE", "NOM_USAGE_BPCE", "NAF_2_CODE",
            "CODE_GROUPE", "LIBELLE_GROUPE", "TIERS_PERE", "TIERS_PERE_ULTIME", "PAYS_RESIDENCE_CODE", "CODE_POSTAL",
            "DATE_ARRETE", "CATEGORIE_JURIDIQUE_CODE", "CLASSIFICATION_TAILLE_ENT_CODE", "DATE_IMMATRICULATION_ENTREPRISE",
            "PERSO_JURIDIQUE_CODE", "DETAIL_PERSO_JURIDIQUE", "NATURE_LIEN_GRAPPAGE_CODE"
        ],
    )
    df_rft_all["DACREE"] = df_rft_all["ID_FEDERAL"].map(federal_id_to_dacree)
    df_rft_all.rename(columns={"CLASSIFICATION_TAILLE_ENT_CODE": "CATEGORIE_ENTREPRISE"}, inplace=True)
    logger.info(f"df_rft_all.shape: {df_rft_all.shape}")

    month = (datetime.now() - relativedelta(months=1)).month
    year = (datetime.now() - relativedelta(months=1)).year
    day = calendar.monthrange(year, month)[1]
    last_date_arrete = datetime(
        day=day,
        month=month,
        year=year,
    ).strftime("%Y-%m-%d")
    logger.info(f"last_date_arrete: {last_date_arrete}")

    df_rft_all["DATE_ARRETE"].max()
    # Déduplication en place sur df_rft_all directement : évite de doubler la mémoire avec .copy()
    remove_rft_duplicate_inplace(df_rft_all, last_date_arrete)
    df_rft = df_rft_all  # simple réaffectation, pas de copie
    del df_rft_all
    gc.collect()
    df_rft = df_rft[df_rft["SIREN"] != "20SC22872"]
    df_rft["ID_FEDERAL"] = df_rft["ID_FEDERAL"].map(int)

    # Add DATE_CREATION
    df_rft["DATE_IMMATRICULATION_ENTREPRISE"] = df_rft["DATE_IMMATRICULATION_ENTREPRISE"].map(
        lambda x: x.strftime("%Y-%m-%d"),
        na_action="ignore"
    )
    df_rft["DATE_IMMATRICULATION_ENTREPRISE"] = df_rft["DATE_IMMATRICULATION_ENTREPRISE"].fillna("")
    df_rft["DACREE"] = df_rft["DACREE"].apply(lambda x: x.strftime("%Y-%m-%d") if not pd.isnull(x) else "")
    df_rft["DATE_CREATION"] = df_rft.apply(
        lambda x: x["DACREE"] if x["DACREE"] != "" else x["DATE_IMMATRICULATION_ENTREPRISE"],
        axis=1
    )
    df_rft["CATEGORIE_ENTREPRISE"] = df_rft["CATEGORIE_ENTREPRISE"].replace("TPE", "PME")
    df_rft["CATEGORIE_ENTREPRISE"] = df_rft["CATEGORIE_ENTREPRISE"].replace("PE", "PME")
    df_rft["CATEGORIE_ENTREPRISE"] = df_rft["CATEGORIE_ENTREPRISE"].replace("ME", "PME")
    # Clean and fill columns
    for int_col in ["SIREN", "TIERS_PERE", "TIERS_PERE_ULTIME", "CODE_GROUPE", "CATEGORIE_JURIDIQUE_CODE"]:
        df_rft[int_col] = df_rft[int_col].fillna(0).apply(int)
    for str_col in [
        "RAISON_SOCIALE", "LIBELLE_GROUPE", "PAYS_RESIDENCE_CODE",
        "CODE_POSTAL", "DATE_ARRETE", "DATE_CREATION", "CATEGORIE_ENTREPRISE"
    ]:
        df_rft[str_col] = df_rft[str_col].fillna("")
    df_rft["CODE_POSTAL"] = df_rft["CODE_POSTAL"].apply(lambda x: str(int(x)) if isinstance(x, float) else str(x))

    df_rft = df_rft[df_rft["ID_FEDERAL"] > 3]

    # Apply sectorial nomenclature
    df_rft["SECTEUR"] = df_rft["SOUS_SECTEUR_ACTIVITE_CODE"].map(subsector_code_to_sector).fillna(
        df_rft["NAF_2_CODE"].map(naf_to_sector)
    )
    df_rft["ALIAS"] = df_rft["NOM_USAGE_BPCE"].fillna("").apply(lambda s: [s] if s else [])
    df_rft["ETAT_ADMINISTRATIF"] = ""

    def extract_year(series: pd.Series) -> pd.Series:
        """Robustly extract the 4-digit year from a date column.

        DATE_BILAN / DATE_BILAN_C come from the RFT parquet and may contain:
          - proper date strings like "2024-06-30"
          - the sentinel "0000-01-01" (filled for missing values)
          - raw numeric 0 / 0.0 that survived fillna (not NaN, so fillna didn't catch them)
          - other malformed values
        We parse with pd.to_datetime and fall back to year 0 on failure so that
        INPI revenues always win over a missing/unknown bilan date.
        """
        parsed = pd.to_datetime(series, errors="coerce", format="%Y-%m-%d")
        return parsed.dt.year.fillna(0).astype(int)

    # Update CHIFFRE_AFFAIRES and DUREE_EXERCICE from INPI values
    df_tmp = df_rft.reset_index().merge(df_revenues, left_on="SIREN", right_on="siren", how="left").set_index("index").copy()
    df_tmp.dropna(subset=["date_cloture"], inplace=True)
    df_tmp["DATE_BILAN"] = df_tmp["DATE_BILAN"].fillna("0000-01-01").astype(str)
    bilan_year = extract_year(df_tmp["DATE_BILAN"])
    cloture_year = pd.to_datetime(df_tmp["date_cloture"], errors="coerce").dt.year.fillna(0).astype(int)
    new_revenues_idx = df_tmp.index[bilan_year < cloture_year]
    df_rft.loc[new_revenues_idx, "CHIFFRE_AFFAIRES"] = pd.to_numeric(df_tmp.loc[new_revenues_idx, "chiffre_affaires"], errors="coerce")
    df_rft.loc[new_revenues_idx, "DUREE_EXERCICE"] = pd.to_numeric(df_tmp.loc[new_revenues_idx, "durée_exercice"], errors="coerce")

    # Update CHIFFRE_AFFAIRES_C and DUREE_EXERCICE_C from INPI values
    df_tmp = df_rft.reset_index().merge(df_consolidated_revenues, left_on="SIREN", right_on="siren", how="left").set_index("index").copy()
    df_tmp.dropna(subset=["date_cloture"], inplace=True)
    df_tmp["DATE_BILAN_C"] = df_tmp["DATE_BILAN_C"].fillna("0000-01-01").astype(str)
    bilan_c_year = extract_year(df_tmp["DATE_BILAN_C"])
    cloture_c_year = pd.to_datetime(df_tmp["date_cloture"], errors="coerce").dt.year.fillna(0).astype(int)
    new_consolidated_revenues_idx = df_tmp.index[bilan_c_year < cloture_c_year]
    df_rft.loc[new_consolidated_revenues_idx, "CHIFFRE_AFFAIRES_C"] = pd.to_numeric(df_tmp.loc[new_consolidated_revenues_idx, "chiffre_affaires"], errors="coerce")
    df_rft.loc[new_consolidated_revenues_idx, "DUREE_EXERCICE_C"] = pd.to_numeric(df_tmp.loc[new_consolidated_revenues_idx, "durée_exercice"], errors="coerce")

    # Normalize the revenues on a 12 month period
    df_rft["CHIFFRE_AFFAIRES"] = pd.to_numeric(df_rft["CHIFFRE_AFFAIRES"], errors="coerce").fillna(0)
    df_rft["DUREE_EXERCICE"] = pd.to_numeric(df_rft["DUREE_EXERCICE"], errors="coerce").fillna(1).replace(0, 1).astype(int)
    df_rft["CHIFFRE_AFFAIRES"] = df_rft["CHIFFRE_AFFAIRES"] * (12 / df_rft["DUREE_EXERCICE"])
    df_rft["CHIFFRE_AFFAIRES_C"] = pd.to_numeric(df_rft["CHIFFRE_AFFAIRES_C"], errors="coerce").fillna(0)
    df_rft["DUREE_EXERCICE_C"] = pd.to_numeric(df_rft["DUREE_EXERCICE_C"], errors="coerce").fillna(1).replace(0, 1).astype(int)
    df_rft["CHIFFRE_AFFAIRES_C"] = df_rft["CHIFFRE_AFFAIRES_C"] * (12 / df_rft["DUREE_EXERCICE_C"])
    df_rft["EST_CLIENT_ACTIF"] = df_rft["DATE_ARRETE"].apply(
        lambda d: d.strftime("%Y-%m-%d") if not pd.isnull(d) else ""
    ) == last_date_arrete
    df_rft.drop(
        columns=[
            "NAF_2_CODE", "NOM_USAGE_BPCE", "DUREE_EXERCICE", "DUREE_EXERCICE_C", "DATE_IMMATRICULATION_ENTREPRISE",
            "DACREE", "DATE_ARRETE", "DATE_BILAN", "DATE_BILAN_C", "SOUS_SECTEUR_ACTIVITE_CODE", "PERSO_JURIDIQUE_CODE",
            "DETAIL_PERSO_JURIDIQUE", "NATURE_LIEN_GRAPPAGE_CODE"
        ],
        inplace=True,
    )
    logger.info(f"df_rft.shape: {df_rft.shape}")
    df_rft["EST_CLIENT_ACTIF"].value_counts(dropna=False)

    # Update RFT values with INSEE values
    # Réaffectation sans copie : df_rft n'est plus utilisé après ce point
    df_rft_updated = df_rft
    del df_rft
    gc.collect()
    df_rft_updated["ALIAS_INSEE"] = [[] for _ in range(len(df_rft_updated))]

    # Extraire les dicts depuis df_insee AVANT de le splitter,
    # puis construire df_insee_out directement — df_insee_in n'est jamais matérialisé.
    siren_set_rft = set(df_rft_updated["SIREN"])
    siren_to_postal_code = dict(zip(df_insee["SIREN"], df_insee["CODE_POSTAL"]))
    siren_to_admin_state = dict(zip(df_insee["SIREN"], df_insee["ETAT_ADMINISTRATIF"]))
    siren_to_rs = dict(zip(df_insee["SIREN"], df_insee["RAISON_SOCIALE"]))
    siren_to_aliases = dict(zip(df_insee["SIREN"], df_insee["ALIAS_INSEE"]))
    siren_to_creation_date = dict(zip(df_insee["SIREN"], df_insee["DATE_CREATION"]))
    siren_to_ce = dict(zip(df_insee["SIREN"], df_insee["CATEGORIE_ENTREPRISE"]))
    # Restreindre les dicts aux sirens présents dans RFT (pour update_aliases / update_rs)
    siren_insee_in_set = set(df_insee["SIREN"]) & siren_set_rft

    df_insee_out = df_insee[~df_insee["SIREN"].isin(siren_set_rft)].copy()
    logger.info(f"df_insee_out.shape: {df_insee_out.shape}")
    del df_insee
    gc.collect()

    # Mise à jour vectorisée des colonnes (remplace les apply row-by-row)
    df_rft_updated["CODE_POSTAL"] = overwrite_col_vectorized(df_rft_updated, "CODE_POSTAL", siren_to_postal_code)
    df_rft_updated["ETAT_ADMINISTRATIF"] = overwrite_col_vectorized(df_rft_updated, "ETAT_ADMINISTRATIF", siren_to_admin_state)
    df_rft_updated["DATE_CREATION"] = overwrite_col_vectorized(df_rft_updated, "DATE_CREATION", siren_to_creation_date)
    df_rft_updated["CATEGORIE_ENTREPRISE"] = update_col_vectorized(df_rft_updated, "CATEGORIE_ENTREPRISE", siren_to_ce)

    # siren_rft_in : sirens RFT présents dans INSEE une seule fois (pas de doublon SIREN côté RFT)
    df_rft_in_tmp = df_rft_updated[df_rft_updated["SIREN"].isin(siren_insee_in_set)]
    logger.info(f"df_rft_in_tmp.shape (avant dedup): {df_rft_in_tmp.shape}")
    siren_rft_in = set(df_rft_in_tmp.drop_duplicates(subset="SIREN", keep=False)["SIREN"])
    del df_rft_in_tmp
    logger.info(f"len(siren_rft_in): {len(siren_rft_in)}")

    df_rft_updated["ALIAS_INSEE"] = df_rft_updated.apply(update_aliases, args=(siren_to_aliases, siren_rft_in), axis=1)
    df_rft_updated["RAISON_SOCIALE"] = df_rft_updated.apply(update_rs, args=(siren_to_rs, siren_rft_in), axis=1)

    logger.info(f"df_rft_updated.shape: {df_rft_updated.shape}")
    df_rft_updated = df_rft_updated[df_rft_updated["RAISON_SOCIALE"] != ""]
    logger.info(f"df_rft_updated.shape (après filtre RAISON_SOCIALE vide): {df_rft_updated.shape}")

    for col in ["ID_FEDERAL", "TIERS_PERE", "TIERS_PERE_ULTIME"]:
        df_insee_out[col] = 0
    for col in ["LIBELLE_GROUPE"]:
        df_insee_out[col] = ""
    df_insee_out["CODE_GROUPE"] = 0
    df_insee_out["ALIAS"] = ""
    df_insee_out["ALIAS"] = df_insee_out["ALIAS"].apply(list)
    df_insee_out["EST_CLIENT_ACTIF"] = False

    # ### Create the CATEGORIE_JURIDIQUE_LIB column
    juridic_categories_filename = "cj_septembre_2022.xls"
    s3.download(f"updates/{juridic_categories_filename}")
    cj_nomenclature = pd.read_excel(
        os.path.join(workdir, juridic_categories_filename),
        sheet_name="Niveau III",
        skiprows=3,
        usecols=["Code", "Libellé"]
    )
    cj_code_to_lib = dict(zip(cj_nomenclature["Code"], cj_nomenclature["Libellé"]))
    for df in [df_rft_updated, df_insee_out]:
        df["CATEGORIE_JURIDIQUE_LIB"] = df["CATEGORIE_JURIDIQUE_CODE"].map(cj_code_to_lib).fillna("")

    df_rft_updated["CATEGORIE_JURIDIQUE_CODE"].apply(type).value_counts(dropna=False)
    df_insee_out["CATEGORIE_JURIDIQUE_CODE"].apply(type).value_counts(dropna=False)

    set(df_rft_updated.columns).difference(df_insee_out.columns), set(df_insee_out.columns).difference(df_rft_updated.columns)

    # ### Match and add Factiva aliases (RAISON_SOCIALE <-> Descriptor)
    factiva_companies_filename = "factiva_companies.xlsx"
    s3.download(f"updates/{factiva_companies_filename}")
    df_factiva_companies = pd.read_excel(
        os.path.join(workdir, factiva_companies_filename),
        sheet_name=0,
        engine="openpyxl",
        usecols=["Descriptor", "Aliases", "AutoAliases", "Region", "Company_code"]
    )
    df_factiva_companies["Aliases"] = df_factiva_companies["Aliases"].fillna("")
    df_factiva_companies["AutoAliases"] = df_factiva_companies["AutoAliases"].fillna("")
    logger.info(f"df_factiva_companies.shape: {df_factiva_companies.shape}")

    df_factiva_companies_fr = df_factiva_companies[df_factiva_companies["Region"] == "FRA"].reset_index(drop=True)
    logger.info(f"df_factiva_companies_fr.shape: {df_factiva_companies_fr.shape}")
    df_factiva_companies_other = df_factiva_companies[df_factiva_companies["Region"] != "FRA"].reset_index(drop=True)
    logger.info(f"df_factiva_companies_other.shape: {df_factiva_companies_other.shape}")

    df_rft_updated = add_aliases(df_rft_updated, df_factiva_companies_fr, df_factiva_companies_other)
    df_insee_out = add_aliases(df_insee_out, df_factiva_companies_fr, df_factiva_companies_other)

    df_rft_updated["ALIAS"] = df_rft_updated.apply(clean_aliases, args=("ALIAS",), axis=1)
    df_rft_updated["ALIAS_INSEE"] = df_rft_updated.apply(clean_aliases, args=("ALIAS_INSEE",), axis=1)
    df_insee_out["ALIAS"] = df_insee_out.apply(clean_aliases, args=("ALIAS",), axis=1)
    df_insee_out["ALIAS_INSEE"] = df_insee_out.apply(clean_aliases, args=("ALIAS_INSEE",), axis=1)

    set(df_rft_updated.columns).difference(df_insee_out.columns), set(df_insee_out.columns).difference(df_rft_updated.columns)
    df_rft_updated["EST_CLIENT_ACTIF"].sum()
    df_insee_out[df_insee_out["ALIAS_INSEE"] != ""]["ALIAS_INSEE"]

    # Convert list columns to strings and save the data to a parquet
    for df, name in zip([df_rft_updated, df_insee_out], ["df_rft_updated.pq", "df_insee_out.pq"]):
        df["ALIAS"] = df["ALIAS"].apply(lambda x: "| ".join(sorted(set(a.strip() for a in x if a.strip()))))
        df["ALIAS_INSEE"] = df["ALIAS_INSEE"].apply(lambda x: "| ".join(sorted(set(a.strip() for a in x if a.strip()))))
        logger.info(df.loc[0, "RAISON_SOCIALE"])
        logger.info(df.loc[0, "ALIAS"])
        logger.info(df.loc[0, "ALIAS_INSEE"])
        df.to_parquet(os.path.join(workdir, name))
    set(df_rft_updated.columns).difference(df_insee_out.columns), set(df_insee_out.columns).difference(df_rft_updated.columns)
    df_rft_updated.head()

    # Compare old vs new company
    df_rft_updated = pd.read_parquet(os.path.join(workdir, "df_rft_updated.pq"))
    df_insee_out = pd.read_parquet(os.path.join(workdir, "df_insee_out.pq"))
    con = DbManager().get_db_connection()
    df_sectors = pd.read_sql("SELECT * FROM secteurs", con=con)

    sector_lib_to_id = dict(zip(df_sectors["libelle"], df_sectors["id"]))
    df_rft_updated["SECTEUR"] = df_rft_updated["SECTEUR"].map(sector_lib_to_id)
    df_insee_out["SECTEUR"] = df_insee_out["SECTEUR"].map(sector_lib_to_id)
    df_insee_out["SECTEUR"].value_counts(dropna=False)
    # Avoid copying 12M-row DataFrames: use the originals directly and rely on
    # boolean indexing (which already returns views/copies on demand).
    df_insee_out_original = df_insee_out
    df_rft_updated_original = df_rft_updated
    df_company_old = pd.read_sql("SELECT * FROM company", con=con)
    con.close()

    df_company_old.drop(columns=["VALID_FROM", "VALID_TO"], inplace=True)
    df_company_old["ALIAS"] = df_company_old["ALIAS"].apply(lambda x: x.split("| ")).apply(
        lambda x: "| ".join(sorted(set(a.strip() for a in x if a.strip())))
    )
    df_company_old["ALIAS_INSEE"] = df_company_old["ALIAS_INSEE"].apply(lambda x: x.split("| ")).apply(
        lambda x: "| ".join(sorted(set(a.strip() for a in x if a.strip())))
    )
    df_company_old.drop(
        index=df_company_old[(df_company_old["ID_FEDERAL"] >= 200) & (df_company_old["ID_FEDERAL"] <= 473)].index,
        inplace=True
    )
    columns = list(df_company_old.columns)
    columns.remove("ID_COMPANY")
    columns.remove("indexed")

    df_insee_old = df_company_old[df_company_old["ID_FEDERAL"] == 0].copy()
    df_insee_new_old = df_insee_out_original[df_insee_out_original["SIREN"].isin(df_insee_old["SIREN"])].copy()
    df_insee_new_new = df_insee_out_original[~df_insee_out_original["SIREN"].isin(df_insee_old["SIREN"])].copy()
    # We need to delete INSEE companies that disappeared.
    company_ids_to_remove = df_insee_old[~(df_insee_old["SIREN"].isin(df_insee_out_original["SIREN"]) | df_insee_old["SIREN"].isin(df_rft_updated_original["SIREN"]))]["ID_COMPANY"].tolist()
    logger.info(f"len(company_ids_to_remove): {len(company_ids_to_remove)}")
    df_insee_old = df_insee_old[~df_insee_old["ID_COMPANY"].isin(company_ids_to_remove)]
    # We will process the sirens that went from insee to rft later.
    df_insee_old_in_rft = df_insee_old[~df_insee_old["SIREN"].isin(df_insee_new_old["SIREN"])]
    assert len(df_insee_old_in_rft["SIREN"].unique()) == len(df_rft_updated_original[df_rft_updated_original["SIREN"].isin(df_insee_old_in_rft["SIREN"])]["SIREN"].unique())
    df_insee_old.drop(index=df_insee_old_in_rft.index, inplace=True)
    assert df_insee_old.shape[0] == df_insee_new_old.shape[0]
    assert df_insee_new_old.shape[0] == df_insee_old.shape[0]
    assert len(set(df_insee_new_old["SIREN"]).difference(df_insee_old["SIREN"])) == 0
    df_insee_new_old.sort_values("SIREN", inplace=True, ignore_index=True)
    df_insee_old.sort_values("SIREN", inplace=True, ignore_index=True)
    df_comp_insee = df_insee_old[columns] != df_insee_new_old[columns]
    df_comp_insee["SECTEUR"] = (df_insee_old["SECTEUR"] != df_insee_new_old["SECTEUR"]) & ((~df_insee_old["SECTEUR"].isna()) & (~df_insee_new_old["SECTEUR"].isna()))
    df_comp_insee.sum()
    df_insee_new_old[df_comp_insee["CATEGORIE_ENTREPRISE"] & (df_insee_old["CATEGORIE_ENTREPRISE"] == "") & (df_insee_new_old["CATEGORIE_ENTREPRISE"] == "ETI")]
    df_company_to_update = df_insee_new_old[df_comp_insee.any(axis=1)].merge(df_insee_old[["SIREN", "ID_COMPANY"]], on="SIREN", how="inner")
    df_company_to_append = df_insee_new_new.copy()
    df_rft_old = df_company_old[df_company_old["ID_FEDERAL"] != 0]
    df_rft_new_old = df_rft_updated[df_rft_updated["ID_FEDERAL"].isin(df_rft_old["ID_FEDERAL"])]
    df_rft_new_new = df_rft_updated[~df_rft_updated["ID_FEDERAL"].isin(df_rft_old["ID_FEDERAL"])]
    deleted_from_rft = df_rft_old[~df_rft_old["ID_FEDERAL"].isin(df_rft_new_old["ID_FEDERAL"])]["ID_COMPANY"].tolist()
    company_ids_to_remove.extend(deleted_from_rft)
    df_rft_old = df_rft_old[~df_rft_old["ID_COMPANY"].isin(deleted_from_rft)]
    assert df_rft_old.shape[0] == df_rft_new_old.shape[0]
    logger.info(
        f"len(company_ids_to_remove), len(deleted_from_rft): {len(company_ids_to_remove)}, {len(deleted_from_rft)}"
    )
    df_rft_old.sort_values("ID_FEDERAL", inplace=True, ignore_index=True)
    df_rft_new_old.sort_values("ID_FEDERAL", inplace=True, ignore_index=True)

    # We now need to process the SIRENs that were in the INSEE companies but now are in RFT.
    # Since multiple RFT companies can have the same SIREN, this is annoying. Like very annoying.
    # The SIREN can be added to already existing RFT companies, or to new ones, or both.
    num_reused_sirens = df_insee_old_in_rft.shape[0]
    num_reused_sirens_in_old_rft = len(
        df_insee_old_in_rft[df_insee_old_in_rft["SIREN"].isin(df_rft_new_old["SIREN"])]["SIREN"].unique()
    )
    num_reused_sirens_in_new_rft = len(
        df_insee_old_in_rft[df_insee_old_in_rft["SIREN"].isin(df_rft_new_new["SIREN"])]["SIREN"].unique()
    )
    logger.info(f"num_reused_sirens: {num_reused_sirens}")
    logger.info(f"num_reused_sirens_in_old_rft: {num_reused_sirens_in_old_rft}")
    logger.info(f"num_reused_sirens_in_new_rft: {num_reused_sirens_in_new_rft}")
    logger.info(
        f"num_reused_sirens_in_old_rft + num_reused_sirens_in_new_rft: {num_reused_sirens_in_old_rft + num_reused_sirens_in_new_rft}"
    )

    # If the SIREN is now in old RFT R_bar (and possibly new RFT R_bar_bar), then just drop those SIREN from INSEE.
    logger.info(f"len(company_ids_to_remove): {len(company_ids_to_remove)}")
    tmp_company_ids = df_insee_old_in_rft[df_insee_old_in_rft["SIREN"].isin(df_rft_new_old["SIREN"])]["ID_COMPANY"].unique().tolist()
    company_ids_to_remove += tmp_company_ids
    logger.info(f"len(company_ids_to_remove) {len(company_ids_to_remove)}")
    df_insee_old_in_rft = df_insee_old_in_rft[~df_insee_old_in_rft["ID_COMPANY"].isin(tmp_company_ids)]
    logger.info(f"len(company_ids_to_remove): {len(company_ids_to_remove)}")

    # If the SIREN is added only 1 time to a new RFT company, then we need to REUSE the old INSEE id_company for the new RFT company
    df_rft_new_new_counts = df_rft_new_new["SIREN"].value_counts()
    df_tmp = df_rft_new_new[(
            df_rft_new_new["SIREN"].isin(df_rft_new_new_counts[df_rft_new_new_counts == 1].index)
            &
            df_rft_new_new["SIREN"].isin(df_insee_old_in_rft["SIREN"])
    )].copy()
    siren_to_company_id = dict(zip(df_insee_old_in_rft["SIREN"], df_insee_old_in_rft["ID_COMPANY"]))
    df_tmp["ID_COMPANY"] = df_tmp["SIREN"].map(siren_to_company_id)
    logger.info(f"df_company_to_update.shape: {df_company_to_update.shape}")
    df_company_to_update = pd.concat([
            df_company_to_update,
            df_tmp,
        ],
        ignore_index=True,
    )
    logger.info(f"df_company_to_update.shape: {df_company_to_update.shape}")
    logger.info(f"df_rft_new_new.shape: {df_rft_new_new.shape}")
    df_rft_new_new.drop(index=df_tmp.index, inplace=True)
    logger.info(f"df_rft_new_new.shape: {df_rft_new_new.shape}")
    logger.info(f"df_company_to_append.shape: {df_company_to_append.shape}")
    df_company_to_append = pd.concat([
            df_company_to_append,
            df_rft_new_new,
        ],
        ignore_index=True,
    )
    logger.info(f"df_company_to_append.shape: {df_company_to_append.shape}")
    # If the SIREN is added multiple times to new RFT companies, we should reuse the old id_company 1 time only,
    # on the closest new RFT company (compare RAISON_SOCIALE ?)
    # This didn't happen this time and I hope it will never, I still add the assertion for future updates.
    df_sirens_multiples = df_insee_old_in_rft[
        (
            df_insee_old_in_rft["SIREN"].isin(df_rft_new_new["SIREN"])
            &
            df_insee_old_in_rft["SIREN"].isin(df_rft_new_new_counts[df_rft_new_new_counts >= 2].index)
        )
    ]
    for _, row in df_sirens_multiples.iterrows():
        df_filter_old = df_rft_new_old[df_rft_new_old["SIREN"] == row["SIREN"]]
        df_filter_new = df_rft_new_new[df_rft_new_new["SIREN"] == row["SIREN"]]
        logger.info(
            f'row["ID_COMPANY"], row["RAISON_SOCIALE"], df_filter_old.shape[0], df_filter_new.shape[0]: {row["ID_COMPANY"], row["RAISON_SOCIALE"], df_filter_old.shape[0], df_filter_new.shape[0]}'
        )
    assert df_sirens_multiples.shape[0] == 0
    df_comp_rft = df_rft_old[columns] != df_rft_new_old[columns]
    df_comp_rft["SECTEUR"] = (df_rft_old["SECTEUR"] != df_rft_new_old["SECTEUR"]) & ((~df_rft_old["SECTEUR"].isna()) & (~df_rft_new_old["SECTEUR"].isna()))
    logger.info(f"df_rft_new_old.shape, df_rft_old.shape: {df_rft_new_old.shape}, {df_rft_old.shape}")
    df_comp_rft.sum()
    df_tmp = df_rft_new_old[df_comp_rft.any(axis=1)].copy()
    federal_id_to_company_id = dict(zip(df_rft_old["ID_FEDERAL"], df_rft_old["ID_COMPANY"]))
    df_tmp["ID_COMPANY"] = df_tmp["ID_FEDERAL"].map(federal_id_to_company_id)
    logger.info(f"df_tmp.shape: {df_tmp.shape}")
    assert df_tmp["ID_COMPANY"].isna().sum() == 0
    logger.info(f"df_company_to_update.shape: {df_company_to_update.shape}")
    df_company_to_update = pd.concat([
            df_company_to_update,
            df_tmp,
        ],
        ignore_index=True,
    )
    logger.info(f"df_company_to_update.shape: {df_company_to_update.shape}")
    logger.info(f"len(company_ids_to_remove): {len(company_ids_to_remove)}")
    logger.info(f"df_company_to_update.shape: {df_company_to_update.shape}")
    logger.info(f"df_company_to_append.shape: {df_company_to_append.shape}")

    # Create the temporary tables
    create_and_populate_tmp_company_update(df_company_to_update)
    update_companies_from_tmp_table()

    # Remove companies
    delete_companies_by_ids(company_ids_to_remove)

    # Add new companies
    add_companies_in_batches(df_company_to_append)

    logger.info("Done.")
