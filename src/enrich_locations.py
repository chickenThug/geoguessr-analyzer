import sys
from pathlib import Path
import logging

# Add src directory to Python path
src_path = Path(__file__).parent
sys.path.append(str(src_path))

from data_pipeline.utils.db_manager import DatabaseManager
from data_pipeline.utils.location_enricher import LocationEnricher


def setup_logging():
    """Configure basic logging"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main():
    """Main enrichment execution"""
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        db_manager = DatabaseManager()
        location_enricher = LocationEnricher()

        success = db_manager.enrich_team_duel_rounds(location_enricher)

        if success:
            logger.info("Location enrichment completed successfully")
        else:
            logger.error("Location enrichment failed")

    except Exception as e:
        logger.error(f"Enrichment failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
