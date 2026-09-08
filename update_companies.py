import os
import argparse
from datetime import datetime
from dotenv import load_dotenv
from updates.update_companies.query_risk_warehouse import main as query_risk
from updates.update_companies.merge_risk_dataframes import main as merge_risk_dataframes
from updates.update_companies.build_company import main as build_companies
from updates.update_companies.update_groups import main as update_groups
from common.logging_utils import get_logger
from common.libs.s3 import S3

load_dotenv()
logger = get_logger()

os.environ["HTTP_PROXY"] = os.environ["PROXY_URL"]
os.environ["HTTPS_PROXY"] = os.environ["PROXY_URL"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Update companies job (risk + S3 + build_companies + update_groups)"
    )

    parser.add_argument(
        "--ignore-check-day",
        action="store_true",
        dest="ignore_check_day",
        help="Ignore day check and force the script to run regardless of the scheduled day",
    )

    parser.add_argument(
        "--sunday-order",
        type=int,
        default=2,
        dest="sunday_order",
        help=(
            "Which Sunday of the month to run on: "
            "1 = first Sunday, 2 = second Sunday (default), etc."
        ),
    )

    return parser


def get_sunday_order(today: datetime) -> int:
    # weekday() : monday=0 ... sunday=6
    if today.weekday() != 6:
        return 0

    day = today.day  # 1..31
    order = (day - 1) // 7 + 1
    return order


def should_run_today(ignore_check_day: bool, sunday_order_expected: int) -> bool:
    if ignore_check_day:
        logger.info(
            "--ignore-check-day enabled: skipping day check, job will run."
        )
        return True

    today = datetime.today()
    weekday = today.weekday()  # Monday=0 ... Sunday=6

    if weekday != 6:
        logger.info("Today is not Sunday, job will not run.")
        return False

    sunday_order_actual = get_sunday_order(today)

    if sunday_order_actual == sunday_order_expected:
        logger.info(
            f"Today is the {sunday_order_actual}ᵗʰ Sunday of the month "
            f"(--sunday-order={sunday_order_expected}), job will run."
        )
        return True
    else:
        logger.info(
            f"Today is the {sunday_order_actual}ᵗʰ Sunday of the month, "
            f"but --sunday-order={sunday_order_expected}, job will not run."
        )
        return False


def run_job():
    """
    try:
        logger.info("querying risk")
        query_risk()
        logger.info("merging risk dataframes")
        merge_risk_dataframes()
    except Exception as e:
        logger.info(f"Errors while querying risk database: {e}, Downloading files from S3.")
        for _file in  (
            "updates/data/risk/df_rft.pq",
            "updates/data/bilans.pq",
            "updates/data/groupes.pq",
            "updates/data/ref_bq_bfbp.pq",
            "updates/data/sous_groupes.pq"
        ):
            logger.info(f"Downloading {_file} from S3")
            S3().download(_file)
    logger.info("building companies")
    build_companies()
    update_groups()
    """
    logger.info("Downloading files from S3.")
    for _file in (
        "updates/data/risk/df_rft.pq",
        "updates/data/bilans.pq",
        "updates/data/groupes.pq",
        "updates/data/ref_bq_bfbp.pq",
        "updates/data/sous_groupes.pq",
    ):
        logger.info(f"Downloading {_file} from S3")
        S3().download(_file)

    logger.info("building companies")
    build_companies()
    update_groups()


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not should_run_today(
        ignore_check_day=args.ignore_check_day,
        sunday_order_expected=args.sunday_order,
    ):
        logger.info("Calendar condition not met, job will not run and script will exit.")
        return

    run_job()


if __name__ == "__main__":
    main()
