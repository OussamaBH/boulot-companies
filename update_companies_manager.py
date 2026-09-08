from common.logging_utils import get_logger
from common.libs.solr import Solr

logger = get_logger()

SOLR_COMPANIES_COLLECTION = 'companies_alias'

def get_all_companies_articles_count() -> dict:
    """
    Returns a dict {id_company: articles_count}
    in a single Solr request using facets on main_companies_ids field.
    Uses main_companies_ids (reliable DB matched IDs) instead of companies field (unreliable text detection).
    """
    solr_articles = Solr("articles")
    try:
        results = solr_articles.search(
            query="*:*",
            rows=0,
            facet="on",
            **{
                "facet.field": "main_companies_ids",  # ← ID fiable au lieu de companies
                "facet.limit": -1,
                "facet.mincount": 1
            }
        )
        # Solr facets are returned as a flat list [value, count, value, count...]
        facets = results.facets['facet_fields']['main_companies_ids']
        # Convert flat list to dict {id_company: count} by stepping through pairs
        companies_count = {
            facets[i]: facets[i + 1]
            for i in range(0, len(facets), 2)
        }
        logger.info(f"Retrieved articles count for {len(companies_count)} companies")
        return companies_count
    except Exception as e:
        logger.error(f"Unable to get articles count from Solr facets : {e}.")
        raise


def get_all_companies_articles_count() -> dict:
    """
    Returns a dict {id_company: articles_count}
    in a single Solr request using facets on main_companies_ids field.
    Uses main_companies_ids (reliable DB matched IDs) instead of companies field (unreliable text detection).
    """
    solr_articles = Solr("articles")
    try:
        results = solr_articles.search(
            query="*:*",
            rows=0,
            facet="on",
            **{
                "facet.field": "main_companies_ids",  # ← ID fiable au lieu de companies
                "facet.limit": -1,
                "facet.mincount": 1
            }
        )
        # Solr facets are returned as a flat list [value, count, value, count...]
        facets = results.facets['facet_fields']['main_companies_ids']
        # Convert flat list to dict {id_company: count} by stepping through pairs
        companies_count = {
            facets[i]: facets[i + 1]
            for i in range(0, len(facets), 2)
        }
        logger.info(f"Retrieved articles count for {len(companies_count)} companies")
        return companies_count
    except Exception as e:
        logger.error(f"Unable to get articles count from Solr facets : {e}.")
        raise


def increment_companies_articles_count(companies_ids: list) -> None:
    """
    Increments by 1 the articles_count field in Solr for each company in the list.
    Used when a new article is indexed to keep articles_count up to date.
    Uses atomic update to only update the articles_count field.

    :param companies_ids: list of company IDs (from article's main_companies_ids field)
    """
    logger.info(f"companies_ids: {companies_ids}")
    if not companies_ids:
        return

    solr_companies = Solr(SOLR_COMPANIES_COLLECTION)

    try:
        # Search current articles_count for each company ID in Solr companies collection
        # Solr IDs are usually strings or integers, we query on the "id_company" field
        companies_query = " OR ".join([f'id_company:"{co_id}"' for co_id in companies_ids])
        logger.info(f'id_company:({companies_query})')
        results = solr_companies.search(
            query=f'id_company:({companies_query})',
            rows=len(companies_ids),
            fl="id_company,articles_count"
        )

        if not results or len(results) == 0:
            logger.warning(f"No companies found in Solr for IDs: {companies_ids}")
            return

        # Build atomic update +1 for each company found
        solr_updates = [
            {
                "id_company": company.get("id_company"),
                "articles_count": {"inc": 1}  # atomic increment by 1
            }
            for company in results
        ]

        solr_companies.add_documents(solr_updates)
        logger.info(f"Incremented articles_count for {len(solr_updates)} companies")

    except Exception as e:
        logger.error(f"Unable to increment articles_count for companies {companies_ids} : {e}.")
        raise

def get_companies_articles_count(companies_ids: list) -> dict:
    """
    Returns a dict {id_company: articles_count} for a specific list of company IDs.
    Uses Solr facets on main_companies_ids field (all articles) and filters
    the result in Python to only keep the requested company IDs.
    Avoids "too many boolean clauses" Solr error by not using OR-based fq.

    :param companies_ids: list of company IDs to get articles count for
    :return: dict {id_company: articles_count}
    """
    if not companies_ids:
        return {}

    solr_articles = Solr("articles")
    try:
        # Fetch ALL facets without fq filter to avoid "too many boolean clauses"
        results = solr_articles.search(
            query="*:*",
            rows=0,
            facet="on",
            **{
                "facet.field": "main_companies_ids",
                "facet.limit": -1,
                "facet.mincount": 1
            }
        )

        # Solr facets are returned as a flat list [value, count, value, count...]
        facets = results.facets['facet_fields']['main_companies_ids']

        # Convert flat list to dict {id_company: count} by stepping through pairs
        all_companies_count = {
            facets[i]: facets[i + 1]
            for i in range(0, len(facets), 2)
        }

        # Filter in Python to only keep requested IDs (cast to str for safety)
        requested_ids = {str(co_id) for co_id in companies_ids}
        companies_count = {
            co_id: count
            for co_id, count in all_companies_count.items()
            if str(co_id) in requested_ids
        }

        logger.info(f"Retrieved articles count for {len(companies_count)} / {len(companies_ids)} companies")
        return companies_count

    except Exception as e:
        logger.error(f"Unable to get articles count from Solr facets for IDs {companies_ids} : {e}.")
        raise


def reset_companies_articles_count(query: str = "articles_count:[1 TO *]", batch_size=1000) -> None:
    """
    Resets articles_count to 0 for companies matching the given query.

    :param query: Solr query to filter companies to reset
                  - "articles_count:[1 TO *]" → only companies with articles_count > 0
                  - "*:*" → ALL companies
    """
    solr_companies = Solr(SOLR_COMPANIES_COLLECTION)
    reset_count = 0
    failed_count = 0
    continue_treatments = True
    batch_number = 1

    logger.info(f"Starting articles_count reset for companies matching query : '{query}'")

    # ── Get total companies to reset for progress tracking ────────────────
    try:
        total_results = solr_companies.search(query=query, rows=0)
        total = total_results.hits if hasattr(total_results, 'hits') else len(total_results)
        total_batches = -(-total // batch_size)
        logger.info(f"Total companies to reset : {total} | Total batches : {total_batches}")
    except Exception as e:
        logger.error(f"Unable to get total companies to reset : {e}.")
        raise

    while continue_treatments:
        logger.info(f"Batch {batch_number}/{total_batches}")

        # ── 1. Always start=0 because after reset, companies leave the query ──
        # "articles_count:[1 TO *]" → after reset to 0, they won't match anymore
        # so we always fetch from the top of the result list
        # For "*:*" query, we use offset normally since docs don't leave the result
        offset = 0 if query != "*:*" else (batch_number - 1) * batch_size

        try:
            companies = solr_companies.search(
                query=query,
                rows=batch_size,
                start=offset,
                fl="id_company"  # only id_company needed for reset
            )

            if not companies or len(companies) == 0:
                logger.info("No more companies to reset, stopping.")
                continue_treatments = False
                break

        except Exception as e:
            logger.error(f"Unable to fetch companies from Solr : {e}.")
            continue_treatments = False
            break

        # ── 2. Build atomic updates to reset articles_count to 0 ──────────
        solr_updates = [
            {
                "id_company": company.get("id_company"),
                "articles_count": {"set": 0}
            }
            for company in companies
            if company.get("id_company")
        ]

        # ── 3. Send atomic updates to Solr ────────────────────────────────
        try:
            solr_companies.add_documents(solr_updates)
            reset_count += len(solr_updates)
            logger.info(f"Reset {len(solr_updates)} companies | Total reset so far : {reset_count}/{total}")
        except Exception as e:
            failed_count += len(solr_updates)
            logger.error(f"Unable to reset articles_count for batch {batch_number} : {e}.")

        batch_number += 1

    logger.info(f"Reset completed : {reset_count} companies reset")
    logger.info(f"Failed : {failed_count} companies")
