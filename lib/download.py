import calendar
import json
import os
import concurrent.futures
import urllib.request

from datetime import datetime, timedelta
from typing import List
from lib.sql import Database

class Downloader:
    def __init__(self, db: Database, api_key: str, is_cache_enabled: bool, data_dir: str, max_workers: int):
        self.db = db
        self.api_key = api_key
        self.is_cache_enabled = is_cache_enabled
        self.data_dir = data_dir
        self.max_workers = max_workers

    def download_weather_data(self, station_list: List[str], start_date: str, end_date: str) -> None:
        """Download weather data from the given paths."""
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        for station in station_list:
            self.db.create_table(station)
            self.download_weather_data_for_station(station, start_date, end_date)

    def download_weather_data_for_station(self, station: str, start_date: datetime, end_date: datetime) -> None:
        last_day = end_date
        first_day = max(end_date.replace(day=1), start_date)

        max_retries = 5
        retry_delay = 1  # Initial delay in seconds
        failed_fetchers = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            while last_day >= start_date:
                first_day = max(first_day, start_date)
                last_day = min(last_day, end_date)

                fetcher = RequestFetcher(self, station, first_day, last_day)
                future = executor.submit(fetcher.download_weather_data)
                futures[future] = (fetcher, 0)

                first_day = (first_day.replace(day=1) - timedelta(days=1)).replace(day=1)
                _, last_day = calendar.monthrange(first_day.year, first_day.month)
                last_day = first_day.replace(day=last_day)

            while futures:
                for future in concurrent.futures.as_completed(list(futures.keys())):
                    fetcher, retry_count = futures.pop(future)
                    try:
                        result = future.result()
                        print(f'{len(result["observations"])} rows fetched for {fetcher.station_code} from {fetcher.start_date} to {fetcher.end_date}.')
                        self.db.insert_data(fetcher.station_code, result["observations"])
                    except StopIteration:
                        # Remove futures with first_day less than the current fetcher's first_day
                        for f, (fet, _) in futures.items():
                            if fet.end_date < fetcher.start_date:
                                f.cancel()
                        break
                    except RetryException as e:
                        if retry_count < max_retries:
                            retry_count += 1
                            delay = retry_delay * (2 ** (retry_count - 1))  # Exponential backoff
                            print(f"Retrying {fetcher.start_date} (attempt {retry_count}) after {delay} seconds")
                            future = executor.submit(fetcher.download_weather_data)
                            futures[future] = (fetcher, retry_count)
                            time.sleep(delay)
                        else:
                            failed_fetchers.append((fetcher, e))
                    except concurrent.futures.CancelledError:
                        pass
                    except Exception as e:
                        print(f"Error processing {fetcher.start_date}: {e}", e.__class__)
                        failed_fetchers.append((fetcher, e))

        if failed_fetchers:
            print("The following fetches failed:")
            for (fetcher, e) in failed_fetchers:
                print(f"- {fetcher.station_code} from {fetcher.start_date} to {fetcher.end_date}: {e}")

class RequestFetcher:
    BASE_URL = 'https://api.weather.com/v1/location/{station_code}/observations/historical.json?apiKey={api_key}&units=e&startDate={start_date}&endDate={end_date}'

    def __init__(self, downloader: Downloader, station_code: str, start_date: datetime, end_date: datetime):
        self.downloader = downloader
        self.station_code = station_code
        self.start_date = datetime.strftime(start_date, "%Y%m%d")
        self.end_date = datetime.strftime(end_date, "%Y%m%d")

    @property
    def url(self):
        return self.BASE_URL.format(station_code=self.station_code, api_key=self.downloader.api_key, start_date=self.start_date, end_date=self.end_date)

    def download_weather_data(self):
        path = f"{self.downloader.data_dir}/download_cache/{self.station_code}/{self.start_date}-{self.end_date}.json"

        if self.downloader.is_cache_enabled and os.path.exists(path):
            data = json.load(open(path))
            return data

        try:
            data = urllib.request.urlopen(self.url).read()
        except urllib.error.HTTPError as e:
            if e.code == 409:
                raise RetryException("Received 409 error. Retry the request.") from e
            if e.code == 400:
                raise StopIteration("Received 400 error. Stop the request.") from e
            raise e

        if self.downloader.is_cache_enabled:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(data.decode("utf-8"))
        return json.loads(data)

class RetryException(Exception):
    pass

class StopIteration(Exception):
    pass