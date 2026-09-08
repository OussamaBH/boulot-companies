"""
These indicators are provided by INSEE without requiring authentication.
They are available in XML format at the following address:
https://bdm.insee.fr/series/sdmx/data/SERIES_BDM/.
This URL is already whitelisted in the BPCE proxy.

This script downloads all indicator data,
format it correctly, and insert it into the database.

Output tables from this script include:
inseebdm:
Contains all indicators and their metadata
(unit of measurement, description, etc.).
inseebdm_obs:
Holds observations of the indicators from the previous table.
secteur_inseebdm:
A join table indicating which sector(s) each indicator belongs to.
sous_secteur_inseebdm:
A join table indicating which sub-sector(s) each indicator belongs to.
INSEE updates the indicators during the first week of each month.
"""

import os
import json

import requests
import numpy as np
import pandas as pd
from lxml import etree
from typing import List
from tqdm.auto import tqdm
from dateutil import parser
from sqlalchemy import text
from dotenv import load_dotenv

from common.libs.db_manager import DbManager
from common.logging_utils import get_logger
from common.libs.s3 import S3


logger = get_logger()
load_dotenv()

os.environ["HTTP_PROXY"] = os.environ["PROXY_URL"]
os.environ["HTTPS_PROXY"] = os.environ["PROXY_URL"]


def read_sql(query: str) -> pd.DataFrame:
    con = DbManager().get_db_connection()
    result = pd.read_sql(sql=query, con=con)
    con.close()
    return result


def read_refs(insee_file_path, subsectors_mapping_file) -> pd.DataFrame:
    df_refs = pd.read_excel(
        insee_file_path,
        engine="openpyxl",
        dtype=str,
        usecols=["Sous-secteur", "idBank", "Intitulés des colonnes"],
    )
    df_refs.rename(
        columns={
            "idBank": "idbank",
            "Sous-secteur": "subsectors",
            "Intitulés des colonnes": "section",
        },
        inplace=True,
    )
    df_refs = df_refs[~df_refs["section"].isna()]
    df_refs["idbank"] = df_refs["idbank"].apply(
        lambda x: "0" * (9 - len(x)) + x
    )
    for col in df_refs.columns:
        df_refs[col] = df_refs[col].apply(str.strip)
    subsectors_mapping = json.load(open(subsectors_mapping_file, encoding="utf8"))
    df_refs["subsectors"] = df_refs["subsectors"].apply(
        lambda ss: subsectors_mapping.get(ss, ss)
    )

    df_grouped = df_refs.groupby("idbank", as_index=False).agg(
        {
            "section": list,
            "subsectors": list,
        },
    )
    assert all(df_grouped["section"].apply(
        lambda s: all(map(lambda x: x == s[0], s)))
    )
    df_grouped["section"] = df_grouped["section"].apply(lambda x: x[0])

    return df_grouped


def insee_query(url, headers):
    try:
        return requests.get(url, headers=headers)
    except ConnectionError:
        return insee_query(url, headers=headers)


def extract_insee(idbanks: List[str]) -> dict:
    headers = {
        "Accept": "application/xml",
    }
    num_chunks = len(idbanks) // 100
    num_chunks = max(num_chunks, 1)
    chunks = np.array_split(idbanks, num_chunks)

    data = {}

    for chunk in tqdm(chunks):
        chunk_idbanks = "+".join(chunk)
        url = f"https://bdm.insee.fr/series/sdmx/data/SERIES_BDM/{chunk_idbanks}?startPeriod=2018-10"
        response = insee_query(url, headers)
        if response.status_code != 200:
            logger.info(
                f"|{chunk_idbanks}|\n",
                response.content,
                response.status_code,
                sep="\n"
            )
            raise Exception(response.text)
        root = etree.fromstring(response.content)
        for series in root.iter("Series"):
            idbank = series.attrib["IDBANK"]
            data[idbank] = dict(series.attrib)
            del data[idbank]["IDBANK"]
            data[idbank]["OBS"] = []
            for obs in series.iter("Obs"):
                data[idbank]["OBS"].append(dict(obs.attrib))

    return data


def format_obs_date(time_period):
    if "Q" in time_period:  # 3 months
        time_period = time_period.split("-")
        time_period = time_period[0] + "-" + str(int(time_period[1][1]) * 3)
    elif "B" in time_period:  # 2 months
        time_period = time_period.split("-")
        time_period = time_period[0] + "-" + str(int(time_period[1][1]) * 2)
    elif "S" in time_period:  # 6 months
        time_period = time_period.split("-")
        time_period = time_period[0] + "-" + str(int(time_period[1][1]) * 2)
    elif "-" not in time_period:  # 12 months
        return time_period
    date = parser.parse(time_period)
    return date.strftime("%Y-%m-01")


def prepare_tables(df_refs: pd.DataFrame, data: dict):
    # Remove empty series
    data = {
        idbank: data[idbank]
        for idbank in data
        if not all(
            obs["OBS_VALUE"] == "NaN"
            for obs in data[idbank]["OBS"]
        )
    }
    df_refs = df_refs[df_refs["idbank"].isin(data)]

    # Map INSEE codes to BPCE names
    subsector_lib_to_subsector_id = (
        read_sql(
            "SELECT id, libelle FROM sous_secteurs"
        ).set_index("libelle")["id"].to_dict()
    )
    subsector_id_to_sector_id = (
        read_sql(
            "SELECT id, id_secteur FROM sous_secteurs"
        ).set_index("id")["id_secteur"].to_dict()
    )
    df_subsector_inseebdm = df_refs[["idbank", "subsectors"]].rename(
        columns={"subsectors": "id_sous_secteur"}
    )
    df_subsector_inseebdm = df_subsector_inseebdm.explode(
        "id_sous_secteur", ignore_index=True
    )
    df_subsector_inseebdm["id_sous_secteur"] = df_subsector_inseebdm["id_sous_secteur"].map(
        subsector_lib_to_subsector_id
    )
    df_subsector_inseebdm.drop_duplicates(inplace=True)
    df_sector_inseebdm = df_subsector_inseebdm.rename(
        columns={"id_sous_secteur": "id_secteur"}
    )
    df_sector_inseebdm["id_secteur"] = df_sector_inseebdm["id_secteur"].map(subsector_id_to_sector_id)
    df_sector_inseebdm.drop_duplicates(inplace=True, ignore_index=True)
    logger.info(f"{df_sector_inseebdm.shape=}")
    logger.info(f"{df_subsector_inseebdm.shape=}")

    # Prepare inseebdm and inseebdm_obs
    df_data = pd.DataFrame(data=data).T
    df_data["idbank"] = df_data.index
    df_data.reset_index(drop=True, inplace=True)
    df_data.columns = map(str.lower, df_data.columns)

    df_inseebdm = df_data.drop(columns=["obs"])
    for int_col in ["decimals", "unit_mult"]:
        df_inseebdm[int_col] = df_inseebdm[int_col].apply(int)
    df_inseebdm["section"] = df_inseebdm["idbank"].map(df_refs.set_index("idbank")["section"].to_dict())

    df_inseebdm_obs = df_data[["idbank", "obs"]].explode("obs", ignore_index=True)
    df_inseebdm_obs = pd.concat([df_inseebdm_obs["idbank"], df_inseebdm_obs["obs"].apply(pd.Series)], axis=1)
    df_inseebdm_obs.columns = map(str.lower, df_inseebdm_obs.columns)
    df_inseebdm_obs["time_period"] = df_inseebdm_obs["time_period"].apply(format_obs_date)
    df_inseebdm_obs["obs_rev"] = df_inseebdm_obs["obs_rev"].astype(float).fillna(0).astype(int)
    df_inseebdm_obs["obs_value"] = df_inseebdm_obs["obs_value"].astype(float)
    for nullable_col in ["obs_conf", "date_jo", "obs_value"]:
        if nullable_col not in df_inseebdm_obs.columns:
            df_inseebdm_obs[nullable_col] = None
        else:
            df_inseebdm_obs[nullable_col] = df_inseebdm_obs[nullable_col].replace(np.nan, None)
    df_inseebdm_obs.sort_values(["idbank", "time_period"], inplace=True)

    return df_inseebdm, df_inseebdm_obs, df_sector_inseebdm, df_subsector_inseebdm


def update_tables(
    df_inseebdm: pd.DataFrame,
    df_inseebdm_obs: pd.DataFrame,
    df_sector_inseebdm: pd.DataFrame,
    df_subsector_inseebdm: pd.DataFrame,
):
    con = DbManager().get_db_connection()
    con.execute(text("DELETE FROM inseebdm;"))
    con.commit()
    con.close()
    ko = 0
    for df, table_name in zip(
        [df_inseebdm, df_inseebdm_obs, df_sector_inseebdm, df_subsector_inseebdm],
        ["inseebdm", "inseebdm_obs", "secteur_inseebdm", "sous_secteur_inseebdm"],
    ):
        logger.info(f"Updating table {table_name}")
        try:
            df.to_sql(
                name=table_name,
                con=DbManager().get_db_connection(),
                if_exists="append",
                method="multi",
                index=False,
                chunksize=1,
            )
        except Exception as e:
            logger.error(f"  Error when updating {table_name} : {e}")
            ko += 1
        finally:
            con.commit()
            con.close()
        logger.error(f"{ko} KO lines when updating table {table_name}")


def main():

    updates_folder = os.environ["UPDATES_FOLDER"]
    insee_file_name = os.environ["INSEE_FILE_NAME"]
    subsectors_file_name = os.environ["SUBSECTORS_FILE_NAME"]
    insee_file_path = f"{updates_folder}/{insee_file_name}"
    subsectors_file_path = f"{updates_folder}/{subsectors_file_name}"

    s3 = S3()
    logger.info("Downloading insee xlsx file.")
    s3.download(insee_file_path)
    logger.info("Done.")
    logger.info("Downloading subsectors json file.")
    s3.download(subsectors_file_path)
    logger.info("Done.")

    insee_file_path, subsectors_file_path = [
        os.path.join(s3.download_path, _file)
        for _file in (insee_file_path, subsectors_file_path)
    ]
    # Read all the indicators we need to extract.
    # Each indicator is uniquely identified by an `idbank`.
    df_refs = read_refs(insee_file_path, subsectors_file_path)
    logger.info(df_refs.shape)
    idbanks = df_refs["idbank"].tolist()

    # Extract all the raw data from the INSEE website.
    data = extract_insee(idbanks)

    # Process and format the data before writing it to our tables.
    (
        df_inseebdm, df_inseebdm_obs,
        df_sector_inseebdm, df_subsector_inseebdm
    ) = prepare_tables(df_refs, data)
    for df in [df_inseebdm, df_inseebdm_obs, df_sector_inseebdm, df_subsector_inseebdm]:
        logger.info(df.shape)
    df_inseebdm_obs["date_jo"] = df_inseebdm_obs["date_jo"].replace(np.nan, None)
    df_inseebdm_obs["obs_value"] = df_inseebdm_obs["obs_value"].replace(np.nan, None)

    # Write the data to our tables.
    update_tables(
        df_inseebdm, df_inseebdm_obs, df_sector_inseebdm, df_subsector_inseebdm
    )


if __name__ == "__main__":
    main()
