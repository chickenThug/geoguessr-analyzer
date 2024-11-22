import os
from dotenv import load_dotenv
from data_pipeline.game_file_extraction import GameFileExtractor
import logging

def setup_logging():
    """Configure basic logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def load_environment_variables():
    """Load and validate required environment variables"""
    load_dotenv()
    
    required_vars = ['PLAYER_ID', 'NFSC_COOKIE']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return {
        'player_id': os.getenv('PLAYER_ID'),
        'nfsc_cookie': os.getenv('NFSC_COOKIE')
    }

def main():
    """Main pipeline execution"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Load environment variables
        env_vars = load_environment_variables()
        
        # Initialize and run extractor
        extractor = GameFileExtractor(
            player_id=env_vars['player_id'],
            nfsc_cookie=env_vars['nfsc_cookie']
        )
        
        success = extractor.run_extraction()
        if success:
            logger.info("Data pipeline completed successfully")
        else:
            logger.error("Data pipeline failed")
            
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
