import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv


parent_path = Path(__file__).parent.absolute()

load_dotenv(f"{parent_path}/.env")


MAX_PROCESS_LIFE_TIME = os.environ["MAX_PROCESS_LIFE_TIME"]
PROCESS_RESTART_DELAY_TIME = os.environ["PROCESS_RESTART_DELAY_TIME"]
