#! /usr/bin/env python3

import argparse
import os
import shutil
import toml
import datetime
from typing import List
from lib.download import Downloader
from lib.transform import Transformer
from lib.sql import Database
from lib.server import start_server

def download_weather_data(db: Database, station_list: List[str], start_date: str, end_date: str, api_key: str, is_cache_enabled: bool, data_dir: str, max_workers: int) -> None:
    """Download weather data from the given paths."""
    downloader = Downloader(db, api_key, is_cache_enabled, data_dir, max_workers)
    downloader.download_weather_data(station_list, start_date, end_date)


def run_transformers(station_list: List[str], transformer_list: List[str]) -> None:
    """Run transformers on the downloaded weather data."""
    run_transformers(station_list, transformer_list)


def list_transformers() -> None:
    """List available transformers."""
    list_transformers()


def load_config(config_path: str = "config.toml") -> dict:
    """Load configurations from a TOML file."""
    try:
        with open(config_path, "r") as config_file:
            return toml.load(config_file)
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
        return {}
    except toml.TomlDecodeError:
        print(f"Error decoding TOML file: {config_path}")
        return {}


def main() -> None:
    db = Database()
    # Load configurations
    config = load_config()
    station_list = config.get("station_list", [])
    api_key = config.get("api_key", "")
    data_dir = config.get("data_dir", "./data")
    is_cache_enabled = config.get("is_cache_enabled", "")
    max_workers = config.get("max_workers", 5)

    parser = argparse.ArgumentParser(description="Generate summarized data for visualizer frontend")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Download subcommand
    download_parser = subparsers.add_parser("download", help="Download weather data")
    download_parser.add_argument("--stations", nargs="+", default=station_list, help="List of stations to download (default: all stations in config.toml)")
    download_parser.add_argument("--start_date", nargs=1, default=["1970-01-01"], help="Start date for weather data (default: 1970-01-01 / beginning of data)")
    download_parser.add_argument("--end_date", nargs=1, default=[datetime.date.today().strftime('%Y-%m-%d')], help="End date for weather data (default: today)")

    # Transformer subcommand
    transformer_parser = subparsers.add_parser("transform", help="Run transformers on the downloaded weather data")
    transformer_parser.add_argument("--stations", nargs="+", default=station_list, help="List of stations to download (default: all stations in config.toml)")
    transformer_parser.add_argument("--transformers", nargs="+", default=[], help="Transformers to run (default: all)")

    # List transformers subcommand
    subparsers.add_parser("list_transformers", help="List available transformers")
    subparsers.add_parser("server", help="Start the server")

    args = parser.parse_args()

    if args.command == "download":
        download_weather_data(db, args.stations, args.start_date[0], args.end_date[0], api_key, is_cache_enabled, data_dir, max_workers)
    elif args.command == "transform":
        run_transformers(args.stations, args.transformers)
    elif args.command == "list_transformers":
        list_transformers()
    elif args.command == "server":
        start_server(db)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
