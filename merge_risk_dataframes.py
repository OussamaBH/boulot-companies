import os

import pandas as pd

from common.logging_utils import get_logger


def main():
    logger = get_logger()
    workdir = os.path.join(os.environ["S3_DOWNLOAD_PATH"], "updates")
    rft_golden_path = os.path.join(workdir, "data/rft_golden/")
    rft_golden_files = [os.path.join(rft_golden_path, f) for f in os.listdir(rft_golden_path) if f.endswith(".pq")]
    df_rft_golden = pd.concat([pd.read_parquet(file) for file in rft_golden_files])
    logger.info(f"rft_golden chargé. Dimensions : {df_rft_golden.shape}")

    df_bilans = pd.read_parquet(
        os.path.join(workdir, "data/bilans.pq")
    )
    logger.info(f"bilans chargé. Dimensions : {df_bilans.shape}")

    nie_path = os.path.join(workdir, "data/nie/")
    nie_files = [os.path.join(nie_path, f) for f in os.listdir(nie_path) if f.endswith(".pq")]
    df_nie = pd.concat([pd.read_parquet(file) for file in nie_files])
    logger.info(f"nie chargé. Dimensions : {df_nie.shape}")

    df_ref_bq_bfbp = pd.read_parquet(
        os.path.join(workdir, "data/ref_bq_bfbp.pq")
    )
    logger.info(f"ref_bq_bfbp chargé. Dimensions : {df_ref_bq_bfbp.shape}")

    df_groupes = pd.read_parquet(
        os.path.join(workdir, "data/groupes.pq")
    )
    logger.info(f"groupe chargé. Dimensions : {df_groupes.shape}")

    df_sous_groupes = pd.read_parquet(
        os.path.join(workdir, "data/sous_groupes.pq")
    )
    logger.info(f"groupe chargé. Dimensions : {df_sous_groupes.shape}")

    df_bilan_social = df_bilans[df_bilans["TYPE_BILAN_CODE"] == "S"].copy()
    df_bilan_consolide = df_bilans[df_bilans["TYPE_BILAN_CODE"] == "C"].copy()
    df_bilan_consolide.rename(
        columns={
            "TYPE_BILAN_CODE": "TYPE_BILAN_CODE_C",
            "DUREE_EXERCICE": "DUREE_EXERCICE_C",
            "DATE_BILAN": "DATE_BILAN_C",
            "CHIFFRE_AFFAIRES": "CHIFFRE_AFFAIRES_C",
            "DEVISE_CODE": "DEVISE_CODE_C",
        },
        inplace=True,
    )

    df_final = df_rft_golden.copy()
    df_final = pd.merge(df_final, df_groupes, on=["CODE_GROUPE", "DATE_ARRETE"], how="left")
    df_final = pd.merge(
        df_final,
        df_sous_groupes,
        left_on=["CODE_SOUS_GROUPE", "DATE_ARRETE"],
        right_on=["JOIN_KEY", "DATE_ARRETE"],
        how="left",
    )
    df_final = pd.merge(df_final, df_bilan_social, on="ID_FEDERAL", how="left")
    df_final = pd.merge(df_final, df_bilan_consolide, on="ID_FEDERAL", how="left")
    df_final = pd.merge(
        df_final,
        df_ref_bq_bfbp[["COD_BQ", "COD_CLE2"]],
        left_on="BANQUE_REFERENTE_CODE",
        right_on="COD_BQ",
        how="left",
    )
    df_final = pd.merge(df_final, df_nie, left_on=["ID_NIE", "COD_CLE2"], right_on=["COIDNT", "COETNE"], how="left")
    columns_to_drop = ["COD_BQ", "COD_CLE2", "JOIN_KEY", "COETNE", "COIDNT"]
    df_final.drop(columns=[col for col in columns_to_drop if col in df_final.columns], inplace=True)

    df_final.rename(
        columns={
            "BANQUE_REFERENTE_CODE": "REMETTANT",
            "BANQUE_REFERENTE_lib": "LIB_REMETTANT",
        },
        inplace=True,
    )
    os.makedirs(
        os.path.join(workdir, "data/risk"), exist_ok=True
    )
    df_final.to_parquet(
        os.path.join(workdir, "data/risk/df_rft.pq")
    )

    logger.info(f"Dimensions du DataFrame final : {df_final.shape}")
    logger.info("\nColonnes du DataFrame final :")
    logger.info(df_final.columns)


if __name__ == "__main__":
    main()
