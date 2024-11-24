import sys
from pathlib import Path


# Add src directory to Python path
src_path = Path(__file__).parent
sys.path.append(str(src_path))

from data_pipeline.data_pipeline import main, test_db_manager

if __name__ == "__main__":
    test_db_manager()
