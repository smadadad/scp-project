"""
run_pipeline.py
───────────────
Convenience runner — starts all pipeline components in separate processes.

Usage:
    # Full pipeline (all 3 layers + API)
    python run_pipeline.py --all

    # Individual components
    python run_pipeline.py --producer      # Kinesis producer only
    python run_pipeline.py --consumer      # S3 consumer only
    python run_pipeline.py --speed         # Speed layer only
    python run_pipeline.py --api           # Serving API only
    python run_pipeline.py --setup         # Provision AWS resources
"""

import argparse
import subprocess
import sys
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE = os.path.dirname(os.path.abspath(__file__))


def run(script: str, label: str) -> subprocess.Popen:
    logger.info(f"Starting {label}...")
    proc = subprocess.Popen(
        [sys.executable, os.path.join(BASE, script)],
        cwd=BASE
    )
    return proc


def main():
    parser = argparse.ArgumentParser(description="Dublin Bus Analytics Pipeline Runner")
    parser.add_argument("--all", action="store_true", help="Start all components")
    parser.add_argument("--setup", action="store_true", help="Provision AWS infrastructure")
    parser.add_argument("--producer", action="store_true", help="Start Kinesis producer")
    parser.add_argument("--consumer", action="store_true", help="Start S3 consumer")
    parser.add_argument("--speed", action="store_true", help="Start speed layer processor")
    parser.add_argument("--api", action="store_true", help="Start serving API")
    args = parser.parse_args()

    if args.setup:
        subprocess.run([sys.executable, os.path.join(BASE, "infrastructure/setup_aws.py")])
        return

    procs = []

    if args.all or args.producer:
        procs.append(run("ingestion/producer.py", "Kinesis Producer"))
        time.sleep(2)

    if args.all or args.consumer:
        procs.append(run("ingestion/s3_consumer.py", "S3 Consumer"))

    if args.all or args.speed:
        procs.append(run("speed_layer/stream_processor.py", "Speed Layer"))

    if args.all or args.api:
        procs.append(run("serving_layer/serving_api.py", "Serving API"))

    if not procs:
        parser.print_help()
        return

    logger.info(f"✅ {len(procs)} component(s) running. Press Ctrl+C to stop all.")

    try:
        while True:
            for proc in procs:
                if proc.poll() is not None:
                    logger.warning(f"Process {proc.pid} exited with code {proc.returncode}")
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Stopping all components...")
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait()
        logger.info("All components stopped.")


if __name__ == "__main__":
    main()
