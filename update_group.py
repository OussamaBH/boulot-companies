import os
from common.libs.s3 import S3
from common.logging_utils import get_logger
from common.libs.db_manager import DbManager
from updates.update_companies.query_risk_warehouse import get_connection
from sqlalchemy import text
import pandas as pd
import traceback

logger = get_logger()
#rft_connection = get_connection("RFT")
goldeneye_db_connexion = DbManager().get_db_connection()
s3 = S3()
workdir = os.path.join(os.environ["S3_DOWNLOAD_PATH"], "updates")

def log_error(logger, message: str, exception: Exception):
    """Simple error logging with file and line number"""
    tb = traceback.extract_tb(exception.__traceback__)[-1]
    logger.error(f"{message} | {type(exception).__name__}: {str(exception)} | {tb.filename}:{tb.lineno}")


def truncate_and_insert(df: pd.DataFrame, table_name: str, connection):
    """
    Truncate table and insert DataFrame data preserving table structure
    """
    if df.empty:
        logger.info(f"No data to insert into {table_name}")
        return

    try:
        # Truncate table to preserve structure
        logger.info(f"Truncating table {table_name}")
        connection.execute(text(f"TRUNCATE TABLE {table_name}"))
        connection.commit()

        # Insert data using append to preserve structure
        logger.info(f"Inserting {len(df)} records into {table_name}")
        df.to_sql(table_name, connection, if_exists='append', index=False)
        connection.commit()

        logger.info(f"Successfully inserted {len(df)} records into {table_name}")

    except Exception as e:
        log_error(logger, f"Error in truncate_and_insert for {table_name}", e)
        raise

def get_local_companies():
    query = """
        SELECT
            ID_COMPANY, ID_FEDERAL, CODE_GROUPE, TIERS_PERE, TIERS_PERE_ULTIME
        FROM 
            COMPANY
        WHERE 
            CODE_GROUPE != 0;
    """
    return pd.read_sql(query, goldeneye_db_connexion)


def query_rft_group():
    """creation of rft_groups"""
    logger.info('Getting query_rft_group_result.pq from S3.')
    '''
    query = """
            SELECT DISTINCT
                rg.CODE_GROUPE AS group_id,
                rg.LIBELLE_GROUPE AS name,
                rgn.TIERS_PERE_ULTIME AS head_company_federal_id
            FROM 
                IRFT.RFT_GROUPE rg
            JOIN 
                IRFT.RFT_GRAPPAGE_NATIONAL rgn
            ON 
                rgn.CODE_GROUPE = rg.CODE_GROUPE
    """
    return pd.read_sql(query, rft_connection)
    '''
    filename = "query_rft_group_result.pq"
    s3.download(f"updates/{filename}")
    return pd.read_parquet(os.path.join(workdir, filename))


def update_rft_group_table(rft_groups, companies):
    logger.info('In update_rft_groups_table')
    rft_groups['HEAD_COMPANY_FEDERAL_ID'] = pd.to_numeric(
        rft_groups['HEAD_COMPANY_FEDERAL_ID'], errors='coerce'
    ).astype('Int64')
    companies['ID_FEDERAL'] = pd.to_numeric(
        companies['ID_FEDERAL'], errors='coerce'
    ).astype('Int64')
    rft_groups = rft_groups.merge(
        companies[['ID_FEDERAL', 'ID_COMPANY']],
        left_on='HEAD_COMPANY_FEDERAL_ID',
        right_on='ID_FEDERAL',
        how='left'
    )
    rft_groups = rft_groups.drop(columns=['HEAD_COMPANY_FEDERAL_ID', 'ID_FEDERAL'])
    rft_groups = rft_groups.rename(columns={
        'ID_COMPANY': 'head_company',
        'GROUP_ID': 'group_id',
        'NAME': 'name'
    })
    rft_groups = rft_groups.dropna(subset=['name'])
    rft_groups['head_company'] = rft_groups['head_company'].fillna(0).astype('Int64')
    truncate_and_insert(rft_groups, 'rft_group', goldeneye_db_connexion)


def query_rft_subgroup():
    logger.info('Getting query_rft_subgroup_result.pq from S3.')
    '''
    query = """            
        SELECT DISTINCT
            rg.CODE_GROUPE AS group_id,
            rgn.CODE_SOUS_GROUPE AS subgroup_id,
            rg.LIBELLE_SOUS_GROUPE AS name,
            rgn.TIERS_PERE AS head_company_federal_id
        FROM
            IRFT.RFT_GROUPE rg
        JOIN
            IRFT.RFT_GRAPPAGE_NATIONAL rgn ON rgn.CODE_SOUS_GROUPE = TO_CHAR(rg.CODE_GROUPE) || '_' || TO_CHAR(rg.CODE_SOUS_GROUPE)
        WHERE
            rg.SOUS_GROUPE_ACTIF = 1
            AND rgn.NATURE_LIEN_GRAPPAGE_CODE != '02'
    """
    return pd.read_sql(query, rft_connection)
    '''
    filename = "query_rft_subgroup_result.pq"
    s3.download(f"updates/{filename}")
    return pd.read_parquet(os.path.join(workdir, filename))


def update_rft_subgroup_table(rft_subgroup, companies):
    logger.info('In update_rft_subgroups_table')
    rft_subgroup['HEAD_COMPANY_FEDERAL_ID'] = pd.to_numeric(
        rft_subgroup['HEAD_COMPANY_FEDERAL_ID'], errors='coerce'
    ).astype('Int64')
    companies['ID_FEDERAL'] = pd.to_numeric(
        companies['ID_FEDERAL'], errors='coerce'
    ).astype('Int64')
    rft_subgroup = rft_subgroup.merge(
        companies[['ID_FEDERAL', 'ID_COMPANY']],
        left_on='HEAD_COMPANY_FEDERAL_ID',
        right_on='ID_FEDERAL',
        how='left'
    )
    rft_subgroup = rft_subgroup.drop(columns=['HEAD_COMPANY_FEDERAL_ID', 'ID_FEDERAL'])
    rft_subgroup = rft_subgroup.rename(columns={
        'ID_COMPANY': 'head_company',
        'GROUP_ID': 'group_id',
        'SUBGROUP_ID': 'subgroup_id',
        'NAME': 'name',
    })
    #
    rft_subgroup = rft_subgroup.dropna(subset=['name'])
    rft_subgroup = rft_subgroup.dropna(subset=['subgroup_id'])
    rft_subgroup['subgroup_id'] = rft_subgroup['subgroup_id'].astype(str)
    rft_subgroup = rft_subgroup.drop_duplicates(subset=['subgroup_id'], keep='first')
    rft_subgroup['group_id'] = pd.to_numeric(
        rft_subgroup['group_id'], errors='coerce'
    ).astype('Int64')
    rft_subgroup['head_company'] = rft_subgroup['head_company'].fillna(0).astype('Int64')
    existing_groups = pd.read_sql("SELECT group_id FROM rft_group", goldeneye_db_connexion)
    existing_groups['group_id'] = pd.to_numeric(
        existing_groups['group_id'], errors='coerce'
    ).astype('Int64')
    orphans_mask = ~rft_subgroup['group_id'].isin(existing_groups['group_id'])
    if orphans_mask.any():
        logger.warning(f'Dropping {orphans_mask.sum()} subgroups with unknown group_id')
        rft_subgroup = rft_subgroup[~orphans_mask]
    logger.info(f'Inserting {len(rft_subgroup)} subgroups into rft_subgroup')
    truncate_and_insert(rft_subgroup, 'rft_subgroup', goldeneye_db_connexion)


def insert_dataframe_in_batches(df: pd.DataFrame, table_name: str, connection, truncate_first=True, index=False):
    """
    Insert DataFrame in batches using CHUNK_UPDATE_COMPANIES
    """
    if df.empty:
        logger.info(f"No data to insert into {table_name}")
        return

    # Handle 'replace' mode by dropping table first
    if truncate_first:
        try:
            logger.info(f"Truncating table {table_name}")
            connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            connection.commit()
        except Exception as e:
            log_error(logger, f"Error truncating table {table_name}", e)
            raise

    # Get batch size and insert in chunks
    batch_size = int(os.environ.get("CHUNK_UPDATE_COMPANIES", "1000"))
    total_records = len(df)

    for i in range(0, total_records, batch_size):
        chunk_df = df.iloc[i:i + batch_size]
        chunk_df.to_sql(
            name=table_name,
            con=connection,
            if_exists='append',
            index=index,
            method="multi",
            chunksize=100
        )

        current_batch = i // batch_size + 1
        total_batches = (total_records + batch_size - 1) // batch_size
        remaining_batches = total_batches - current_batch

        logger.info(
            f"Inserted batch {current_batch}/{total_batches}: {len(chunk_df)} records into {table_name} | {remaining_batches} batches remaining")

def link_company_to_groups():
    query = """
        SELECT DISTINCT
            ID_COMPANY AS 'company_id',
            CODE_GROUPE AS 'group_id'
        FROM
            company
        WHERE
            CODE_GROUPE != 0;
    """
    company_groups = pd.read_sql(query, goldeneye_db_connexion)
    insert_dataframe_in_batches(
        company_groups,
        'rft_group_company',
        goldeneye_db_connexion,
        truncate_first=True,
        index=False
    )
    goldeneye_db_connexion.connection.commit()


def query_rft_company_subgroup():
    logger.info('Getting query_rft_company_subgroup_result.pq from S3.')
    '''
    query = """
        SELECT
            rgn.TIERS_FILS, rgn.CODE_SOUS_GROUPE
        FROM 
            IRFT.RFT_GRAPPAGE_NATIONAL rgn
        WHERE CODE_SOUS_GROUPE IS NOT NULL
    """
    return pd.read_sql(query, rft_connection)
    '''

    filename = "query_rft_company_subgroup_result.pq"
    s3.download(f"updates/{filename}")
    return pd.read_parquet(os.path.join(workdir, filename))


def link_company_to_subgroups(rft_company_subgroup, companies):
    logger.info('In link_company_to_subgroups')
    rft_company_subgroup['TIERS_FILS'] = pd.to_numeric(
        rft_company_subgroup['TIERS_FILS'], errors='coerce'
    ).astype('Int64')
    companies['ID_FEDERAL'] = pd.to_numeric(
        companies['ID_FEDERAL'], errors='coerce'
    ).astype('Int64')
    company_subgroups = rft_company_subgroup.merge(
        companies[['ID_FEDERAL', 'ID_COMPANY']],
        left_on='TIERS_FILS',
        right_on='ID_FEDERAL',
        how='left'
    )
    company_subgroups = company_subgroups.drop(columns=['TIERS_FILS', 'ID_FEDERAL'])
    company_subgroups = company_subgroups.rename(columns={
        'ID_COMPANY': 'company_id',
        'CODE_SOUS_GROUPE': 'subgroup_id',
    })
    before = len(company_subgroups)
    company_subgroups = company_subgroups.dropna(subset=['company_id'])
    dropped = before - len(company_subgroups)
    if dropped > 0:
        logger.warning(f'Dropping {dropped} rows with no matching company_id')

    company_subgroups['company_id'] = company_subgroups['company_id'].astype('Int64')
    company_subgroups['subgroup_id'] = company_subgroups['subgroup_id'].astype(str)

    existing_subgroups = pd.read_sql("SELECT subgroup_id FROM rft_subgroup", goldeneye_db_connexion)
    orphans_mask = ~company_subgroups['subgroup_id'].isin(existing_subgroups['subgroup_id'])
    if orphans_mask.any():
        logger.warning(f'Dropping {orphans_mask.sum()} rows with unknown subgroup_id')
        company_subgroups = company_subgroups[~orphans_mask]

    company_subgroups = company_subgroups.drop_duplicates(subset=['company_id', 'subgroup_id'], keep='first')
    logger.info(f'Inserting {len(company_subgroups)} rows into rft_subgroup_company')
    truncate_and_insert(company_subgroups, 'rft_subgroup_company', goldeneye_db_connexion)


def main():
    logger.info('In update_groups step')
    try:
        companies = get_local_companies()
        update_rft_group_table(query_rft_group(), companies)
        update_rft_subgroup_table(query_rft_subgroup(), companies)
        link_company_to_groups()
        link_company_to_subgroups(query_rft_company_subgroup(), companies)
        logger.info('Done')
    except Exception as e:
        log_error(logger, "Error in update_groups", e)
    finally:
        goldeneye_db_connexion.close()
    #    rft_connection.close()

