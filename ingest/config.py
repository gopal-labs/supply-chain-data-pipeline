from pathlib import Path

# Project root is two levels up from this file (ingest/config.py -> project root)
BASE_DIR = Path(__file__).resolve().parent.parent

# Put your full DataCo/Evoastra CSV here
SOURCE_CSV = BASE_DIR / "data" / "evoastra_full.csv"

# Bronze layer output: partitioned by batch_date
RAW_DIR = BASE_DIR / "raw"

LOG_DIR = BASE_DIR / "logs"

# The column we slice batches on
DATE_COLUMN = "order date (DateOrders)"

# The DataCo dataset is commonly saved with latin-1 encoding, not utf-8.
# If you get a UnicodeDecodeError with this set to "latin1", try "utf-8" instead.
ENCODING = "latin1"