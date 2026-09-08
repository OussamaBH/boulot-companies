import os
import json

import zipfile
from itertools import chain
import multiprocessing as mp
import pandas as pd

from common.logging_utils import get_logger
from common.libs.s3 import S3


logger = get_logger()

updates_folder = os.environ["UPDATES_FOLDER"]
inpi_file_name = os.environ["INPI_FILE_NAME"]
inpi_file_path = f"{updates_folder}/{inpi_file_name}"


def extract_json_data(args: tuple) -> list:
    zip_path, filename = args
    logger.info(f'Treating file {filename}')
    with zipfile.ZipFile(zip_path) as zf:
        file = zf.read(filename).decode("utf8")

    file = json.loads(file[:-1] + "]")
    file = [e.get("bilanSaisi").get("bilan") for e in file]

    data = []

    for d in file:
        if d["identite"]["codeTypeBilan"] != "C" and d["identite"]["codeTypeBilan"] != "K":
            continue
        identity = d["identite"]
        siren = identity["siren"]
        closure_date = identity["dateClotureExercice"]
        duration = identity["dureeExerciceN"]
        currency = identity["codeDevise"]
        report_type = identity["codeTypeBilan"]
        report_date = identity["dateDepot"]
        pages = [p for p in d.get("detail").get("pages") if p["numero"] == 3]
        if pages:
            page = pages[0]
            liasses = [
                liasse
                for liasse in page["liasses"]
                if liasse["code"] == "FJ"
            ]
            if liasses and liasses[0]["m3"]:
                revenues = int(liasses[0]["m3"])
                data.append([siren, closure_date, duration, revenues, currency, report_type, report_date])

    return data


def export_revenues(df: pd.DataFrame, filename: str) -> None:
    results = (
        df.set_index("siren")[["closure_date", "duration", "revenues", "report_date", "currency"]]
        .apply(tuple, axis=1)
        .to_dict()
    )
    s3 = S3()
    logger.info(f"Writing tmp_file {filename}")
    tmp_file = os.path.join(s3.download_path, updates_folder, filename)
    with open(tmp_file, "w") as fp:
        json.dump(results, fp, indent=2)
    logger.info(f"Uploading {filename} to s3")
    S3().upload(tmp_file, f"{updates_folder}/{filename}")


def main():

    logger.info("Script started.")
    s3 = S3()
    logger.info("Downloading INPI zip file.")
    s3.download(inpi_file_path)
    logger.info("Done.")
    zip_file_path = os.path.join(s3.download_path, inpi_file_path)

    with zipfile.ZipFile(zip_file_path) as zip_file:
        filenames = [f.filename for f in zip_file.filelist]
        num_jsons = len(filenames)
        logger.info(f"Number of JSON files in the ZIP: {num_jsons}")

    args_list = [(zip_file_path, filename) for filename in filenames]
    with mp.Pool(processes=mp.cpu_count()) as pool:
        df_revenues = pd.DataFrame(
            data=chain(*pool.map(extract_json_data, args_list)),
            columns=["siren", "closure_date", "duration", "revenues", "currency", "report_type", "report_date"],
        )

    logger.info(f"df_revenues.shape : {df_revenues.shape}")

    # Only keep the most updated data.
    df_revenues.sort_values(by=["siren", "report_type", "closure_date", "report_date"], inplace=True)
    df_revenues.drop_duplicates(subset=["siren", "report_type"], keep="last", inplace=True)
    logger.info(f"updated df_revenues.shape : {df_revenues.shape}")

    # Export the data into 2 JSON files.
    # One is for the company's revenues, the other for the consolidated revenues.
    df_revenues["siren"] = df_revenues["siren"].map(int)
    export_revenues(
        df=df_revenues[df_revenues["report_type"] == "C"],
        filename="siren_to_revenues.json",
    )
    export_revenues(
        df=df_revenues[df_revenues["report_type"] == "K"],
        filename="siren_to_consolidated_revenues.json",
    )
    logger.info("Script successfully executed.")


if __name__ == "__main__":
    main()
