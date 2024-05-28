import asyncio
import logging
import os
import hashlib
import aiofiles
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import argparse
from typing import Dict, Tuple, Set, List

logger = logging.getLogger(__name__)

async def md5sum_async(file_path: str) -> str:
    hash_md5 = hashlib.md5()
    async with aiofiles.open(file_path, 'rb') as f:
        while True:
            chunk = await f.read(4096)
            if not chunk:
                break
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

async def process_file(file_path: str) -> Tuple[str, str | None]:
    try:
        checksum = await md5sum_async(file_path)
        return file_path, checksum
    except Exception as e:
        logger.exception(f"Error processing file {file_path}", exc_info=e)
        return file_path, None

async def generate_manifest(directory: str, output_file: str) -> None:
    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        tasks = []
        logger.info(f"Walking directory {directory} to gather all file paths.")
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                tasks.append(loop.run_in_executor(executor, process_file, file_path))

        with open(output_file, 'w') as f:
            for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Computing md5sums", unit="file"):
                result = await task
                file_path, checksum = await result
                relative_path = os.path.relpath(file_path, directory)
                f.write(f"{relative_path} {checksum}\n")
    logger.info("Finished generating md5sum manifest file.")

def load_manifest(manifest_file: str) -> Dict[str, str]:
    manifest = {}
    with open(manifest_file, 'r') as f:
        for line in f:
            relative_path, checksum = line.strip().split(' ', 1)
            manifest[relative_path] = checksum
    return manifest

def compare_manifests(source_manifest: Dict[str, str], destination_manifest: Dict[str, str]) -> Tuple[Set[str], Set[str], Set[str]]:
    source_files = set(source_manifest.keys())
    destination_files = set(destination_manifest.keys())

    missing_files = source_files - destination_files
    extra_files = destination_files - source_files
    mismatched_files = {file for file in source_files & destination_files if source_manifest[file] != destination_manifest[file]}

    return missing_files, extra_files, mismatched_files

def compare(source_manifest_file: str, destination_manifest_file: str, output_csv: str | None) -> pd.DataFrame:
    source_manifest = load_manifest(source_manifest_file)
    destination_manifest = load_manifest(destination_manifest_file)

    missing_files, extra_files, mismatched_files = compare_manifests(source_manifest, destination_manifest)

    print(f"Files only in source: {len(missing_files)}")
    for file in missing_files:
        print(file)

    print(f"Files only in destination: {len(extra_files)}")
    for file in extra_files:
        print(file)

    print(f"Files with different md5sum values: {len(mismatched_files)}")
    for file in mismatched_files:
        print(file)

    data = {
        "missing": list(missing_files),
        "extra": list(extra_files),
        "hash_mismatch": list(mismatched_files)
    }
    df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in data.items()]))
    if output_csv:
        df.to_csv(output_csv, index=False)
        print(f"Results written to {output_csv}")

    return df

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and compare md5sum manifests.")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    generate_parser = subparsers.add_parser("generate", help="Generate an md5sum manifest for a directory")
    generate_parser.add_argument("directory", type=str, help="Directory to scan")
    generate_parser.add_argument("output_file", type=str, help="Output file to store the manifest")

    compare_parser = subparsers.add_parser("compare", help="Compare two md5sum manifests")
    compare_parser.add_argument("source_manifest", type=str, help="Source manifest file")
    compare_parser.add_argument("destination_manifest", type=str, help="Destination manifest file")
    compare_parser.add_argument("--output_csv", type=str, help="Output CSV file to store the comparison results", default=None)

    args = parser.parse_args()

    if args.command == "generate":
        asyncio.run(generate_manifest(args.directory, args.output_file))
    elif args.command == "compare":
        compare(args.source_manifest, args.destination_manifest, args.output_csv)
    else:
        parser.print_help()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()