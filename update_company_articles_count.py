import os
from datetime import datetime

from common.logging_utils import get_logger
from common.libs.solr import Solr
from updates.update_companies.utils.update_companies_manager import get_all_companies_articles_count, reset_companies_articles_count
from tqdm import tqdm


logger = get_logger()
BATCH_SIZE = int(os.environ['INDEX_COMPANIES_BATCH_SIZE'])
SEPARATOR = 60 * '='
START_TIME = datetime.now()
SOLR_COMPANIES_COLLECTION = "companies_alias"


def get_total_companies() -> int:
    """Get total number of companies in Solr to calculate total batches."""
    solr_companies = Solr(SOLR_COMPANIES_COLLECTION)
    try:
        results = solr_companies.search(query="*:*", rows=0)
        total = results.hits if hasattr(results, 'hits') else len(results)
        logger.info(f"Total companies in Solr : {total}")
        return total
    except Exception as e:
        logger.error(f"Unable to get total companies from Solr : {e}.")
        raise


def run_update_articles_count(reset: bool = False, full_reset: bool = False):
    """
    Update articles_count for companies in Solr.

    Modes :
    - reset=False, full_reset=False : update only companies with articles (fastest)
    - reset=True,  full_reset=False : reset companies with articles_count > 0 to 0, then update
    - reset=False, full_reset=True  : reset ALL companies to 0, then update
    """
    updated_companies = 0
    failed_companies = 0
    batch_number = 1

    logger.info("Starting articles count update for companies")

    # ── 1. Reset if needed ────────────────────────────────────────────────
    if full_reset:
        # Reset ALL companies to 0 (even those without articles)
        logger.info("Mode : full reset → resetting ALL companies articles_count to 0...")
        try:
            reset_companies_articles_count(query="*:*", batch_size=BATCH_SIZE)
            logger.info("Full reset completed successfully")
        except Exception as e:
            logger.error(f"Unable to full reset articles_count : {e}.")
            return

    elif reset:
        # Reset only companies with articles_count > 0
        logger.info("Mode : reset → resetting companies with articles_count > 0 to 0...")
        try:
            reset_companies_articles_count(query="articles_count:[1 TO *]", batch_size=BATCH_SIZE)
            logger.info("Reset completed successfully")
        except Exception as e:
            logger.error(f"Unable to reset articles_count : {e}.")
            return

    else:
        logger.info("Mode : update only → no reset, updating articles_count directly")

    # ── 2. Single request to get all articles counts via facets ───────────
    logger.info("Fetching articles count for all companies from Solr (facets)...")
    try:
        companies_articles_count = get_all_companies_articles_count()
        total_companies = len(companies_articles_count)
        total_batches = -(-total_companies // BATCH_SIZE)
        logger.info(f"Articles count fetched for {total_companies} companies | Total batches : {total_batches}")
    except Exception as e:
        logger.error(f"Unable to fetch articles count : {e}.")
        return

    # ── 3. Update directly by id_company ──────────────────────────────────
    items = list(companies_articles_count.items())
    chunks = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]

    for chunk in chunks:
        logger.info(f"{SEPARATOR} Batch {batch_number}/{total_batches}")

        solr_updates = []
        with tqdm(total=len(chunk), desc=f"Batch {batch_number}/{total_batches}", unit="company") as batch_pbar:
            for id_company, articles_count in chunk:
                solr_updates.append({
                    "id_company": id_company,
                    "articles_count": {"set": articles_count}
                })
                batch_pbar.update(1)

        try:
            solr_companies = Solr(SOLR_COMPANIES_COLLECTION)
            solr_companies.add_documents(solr_updates)
            updated_companies += len(solr_updates)
            logger.info(f"Updated {len(solr_updates)} companies in Solr")
        except Exception as e:
            failed_companies += len(solr_updates)
            logger.error(f"Unable to update companies in Solr : {e}.")

        batch_number += 1
        logger.info(f"Updated companies so far : {updated_companies}/{total_companies}")
        logger.info(f"Running since {datetime.now() - START_TIME}")

    logger.info(f"Updated companies : {updated_companies}")
    logger.info(f"Failed companies  : {failed_companies}")


def main(reset: bool = False, full_reset: bool = False):
    run_update_articles_count(reset=reset, full_reset=full_reset)


def cli():
    import argparse
    parser = argparse.ArgumentParser(
        description="Script to update articles_count for companies in Solr."
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help="Reset articles_count to 0 for companies with articles_count > 0 before updating."
    )
    parser.add_argument(
        '--full-reset',
        action='store_true',
        help="Reset articles_count to 0 for ALL companies before updating."
    )
    args = parser.parse_args()

    if args.reset and args.full_reset:
        logger.error("Cannot use --reset and --full-reset at the same time.")
        return

    return main(reset=args.reset, full_reset=args.full_reset)


if __name__ == "__main__":
    cli()
