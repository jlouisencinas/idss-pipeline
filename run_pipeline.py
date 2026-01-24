import sys
import subprocess
from download_IDSS import step1_download_latest_idss

if __name__ == "__main__":
    # Step 1: download latest IDSS ZIPs (atomic)
    step1_download_latest_idss()

    # Step 2: process AFTER downloads complete
    subprocess.run(
        [sys.executable, "processIDSS.py"],
        check=True
    )
