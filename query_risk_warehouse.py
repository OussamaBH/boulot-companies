import os
from warnings import filterwarnings

import oracledb
import pandas as pd

from common.logging_utils import get_logger


filterwarnings(
    "ignore", category=UserWarning,
    message=".*pandas only supports SQLAlchemy connectable.*"
)
logger = get_logger()
workdir = os.path.join(os.environ["S3_DOWNLOAD_PATH"], "updates")


def get_connection(target_db: str):
    logger.info(f"Getting connection to {target_db}")
    server = os.environ["RFT_SERVER"]
    port = os.environ["RFT_PORT"]
    db_name = os.environ["RFT_DATABASE"]

    user = os.environ[f"{target_db}_USERNAME"]
    pw = os.environ[f"{target_db}_PASSWORD"]

    con_string = f"{server}:{port}/{db_name}"
    connection = oracledb.connect(user=user, password=pw, dsn=con_string, disable_oob=True)
    logger.info("Done")
    return connection

def extract_rft(rft_connection):
    os.makedirs(
        os.path.join(workdir, "data/rft_golden"), exist_ok=True
    )
    rft_golden_iter = pd.read_sql(
        """
WITH rft_golden_max_dates AS (
           SELECT
               ID_FEDERAL,
               MAX(DATE_ARRETE) AS MAX_DATE_ARRETE
           FROM
               irft.H_rft_tiers_golden
           WHERE
               EST_ACTIF = 'Y'
               AND SEGMENT_RISQUE_CODE NOT IN ('1050', '1011')
               AND BANQUE_REFERENTE_CODE NOT IN ('MOTEUR', '99999', 'BACKUP', 'BVD', 'NIE')
           GROUP BY
               ID_FEDERAL
           )
       SELECT
           rft_golden.ID_FEDERAL,
           rft_golden.date_arrete,
           rft_golden.PERSO_JURIDIQUE_CODE,
           rft_golden.PERSO_JURIDIQUE_LIB,
           rft_golden.DETAIL_PERSO_JURIDIQUE,
           rft_golden.DATE_IMMATRICULATION_ENTREPRISE,
           rft_golden.BANQUE_REFERENTE_CODE,
           rft_golden.BANQUE_REFERENTE_lib,
           rft_golden.OUTIL_NOTATION_CIBLE_CODE,
           rft_golden.CLASSIFICATION_TAILLE_ENT_CODE,
           rft_golden.NOM_USAGE_BPCE,
           rft_golden.RAISON_SOCIALE,
           rft_golden.NAF_2_CODE,
           rft_golden.NAF_2_LIB,
           rft_golden.bvd_id,
           rft_golden.CATEGORIE_JURIDIQUE_CODE,
           rft_golden.CATEGORIE_JURIDIQUE_LIB,
           rft_golden.ETABLISSEMENT_PRINCIPAL,
           rft_golden.PAYS_NATIONALITE_CODE,
           rft_golden.PAYS_RESIDENCE_CODE,
           rft_golden.SIREN,
           rft_golden.SEGMENT_RISQUE_CODE,
           rft_golden.PAYS_RISQUE_CODE,
           rft_golden.ID_NIE,
           rft_golden.SOUS_SECTEUR_ACTIVITE_CODE,
           rft_golden.SOUS_SECTEUR_ACTIVITE_LIB,
           grap_nat.TIERS_PERE,
           grap_nat.TIERS_PERE_ULTIME,
           grap_nat.NATURE_LIEN_GRAPPAGE_CODE,
           grap_nat.CODE_GROUPE,
           grap_nat.CODE_SOUS_GROUPE,
           adresse.CODE_POSTAL
       FROM
           irft.H_rft_tiers_golden rft_golden
       INNER JOIN irft.rft_golden_max_dates rft_md
           ON rft_golden.ID_FEDERAL = rft_md.ID_FEDERAL
           AND rft_golden.DATE_ARRETE = rft_md.MAX_DATE_ARRETE
       LEFT JOIN irft.H_RFT_GRAPPAGE_NATIONAL grap_nat
           ON rft_golden.id_federal = grap_nat.tiers_fils
           AND TRUNC(rft_golden.DATE_ARRETE) = TRUNC(grap_nat.DATE_ARRETE)
       LEFT JOIN irft.H_RFT_ADRESSES_GOLDEN adresse
           ON adresse.ID_FEDERAL = rft_golden.ID_FEDERAL
           AND TRUNC(adresse.DATE_ARRETE) = TRUNC(rft_golden.DATE_ARRETE)
       WHERE
           rft_golden.EST_ACTIF = 'Y'
           AND rft_golden.SEGMENT_RISQUE_CODE NOT IN ('1050', '1011')
           AND rft_golden.BANQUE_REFERENTE_CODE NOT IN ('MOTEUR', '99999', 'BACKUP', 'BVD', 'NIE')
            """,
        rft_connection,
        chunksize=500_000,
    )
    for chunk_idx, chunk in enumerate(rft_golden_iter):
        logger.info(f"chunk_idx: {chunk_idx}, chunk.shape: {chunk.shape}")
        chunk.to_parquet(os.path.join(workdir, f"data/rft_golden/chunk{chunk_idx}.pq"))
    del chunk

    bilans = pd.read_sql(
        """
        SELECT
            bilans.ID_FEDERAL AS ID_FEDERAL,
            bilans.TYPE_BILAN_CODE AS TYPE_BILAN_CODE,
            bilans.DUREE_EXERCICE AS DUREE_EXERCICE,
            bilans.DATE_BILAN AS DATE_BILAN,
            bilans.DEVISE_CODE AS DEVISE_CODE,
            bilans.CHIFFRE_AFFAIRES AS CHIFFRE_AFFAIRES
        FROM irft.RFT_BILANS bilans
        """,
        rft_connection,
    )
    logger.info(f"bilans.shape: {bilans.shape}")
    bilans.sort_values(by=["ID_FEDERAL", "TYPE_BILAN_CODE", "DATE_BILAN"], inplace=True)
    bilans.drop_duplicates(["ID_FEDERAL", "TYPE_BILAN_CODE"], keep="last", inplace=True)
    logger.info(f"After removing duplicates bilans.shape: {bilans.shape}")
    bilans.to_parquet(os.path.join(workdir, "data/bilans.pq"))
    del bilans

    groupes = pd.read_sql(
        """
        SELECT
            groupe.code_groupe AS CODE_GROUPE,
            groupe.LIBELLE_GROUPE AS LIBELLE_GROUPE,
            groupe.DATE_ARRETE AS DATE_ARRETE
        FROM irft.H_RFT_GROUPE groupe WHERE ACTIF = 'Y'
        """,
        rft_connection,
    )
    logger.info(f"groupes.shape: {groupes.shape}")
    groupes.to_parquet(os.path.join(workdir, "data/groupes.pq"))
    del groupes

    sous_groupes = pd.read_sql(
        """
        SELECT
            CODE_GROUPE || '_' || CODE_SOUS_GROUPE AS JOIN_KEY,
            sous_groupe.DATE_ARRETE AS DATE_ARRETE,
            sous_groupe.LIBELLE_SOUS_GROUPE AS LIBELLE_SOUS_GROUPE
        FROM irft.H_RFT_GROUPE sous_groupe
        WHERE sous_groupe.SOUS_GROUPE_ACTIF = '1'
        """,
        rft_connection,
    )
    logger.info(f"sous_groupes.shape: {sous_groupes.shape}")
    sous_groupes.to_parquet(
        os.path.join(workdir, "data/sous_groupes.pq")
    )


def extract_corp(corp_connection):
    os.makedirs(
        os.path.join(workdir, "data/nie"), exist_ok=True
    )
    nie_iter = pd.read_sql(
        """
        WITH dernier_nie AS (
            SELECT COIDNT, COETNE, MAX(COD_PRD_REF) AS MAX_COD_PRD_REF
            FROM CORP.NIEMAT_HISTO
            WHERE COETNE = COIREF AND CEVAL = 1
            GROUP BY COIDNT, COETNE
        )
        SELECT n.COIDNT, n.COETNE, n.DACREE
        FROM CORP.NIEMAT_HISTO n
        INNER JOIN dernier_nie dn
            ON n.COIDNT = dn.COIDNT
            AND n.COETNE = dn.COETNE
            AND n.COD_PRD_REF = dn.MAX_COD_PRD_REF
        WHERE n.COETNE = n.COIREF AND n.CEVAL = 1
        """,
        corp_connection,
        chunksize=500_000,
    )
    for chunk_idx, chunk in enumerate(nie_iter):
        logger.info(f"chunk_idx: {chunk_idx}, chunk.shape: {chunk.shape}")
        chunk.to_parquet(
            os.path.join(workdir, f"data/nie/chunk{chunk_idx}.pq")
        )


def extract_sqldba(sqldba_connection):
    ref_bq_bfbp = pd.read_sql(
        """
        SELECT * FROM SQLDBA.ref_bq_bfbp
        """,
        sqldba_connection,
    )
    logger.info(f"ref_bq_bfbp.shape: {ref_bq_bfbp.shape}")
    ref_bq_bfbp.to_parquet(os.path.join(workdir, "data/ref_bq_bfbp.pq"))


def main():
    # Extract the RFT tables
    logger.info("Extracting RFT tables")
    rft_connection = get_connection("RFT")
    extract_rft(rft_connection)
    rft_connection.close()

    # Extract the CORP tables
    logger.info("Extracting CORP tables")
    corp_connection = get_connection("CORP")
    extract_corp(corp_connection)
    corp_connection.close()

    # Extract the SQLDBA tables
    logger.info("Extracting SQLDBA tables")
    sqldba_connection = get_connection("SQLDBA")
    extract_sqldba(sqldba_connection)
    sqldba_connection.close()


if __name__ == "__main__":
    main()
