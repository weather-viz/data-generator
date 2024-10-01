from typing import Callable
from functools import wraps

class Transformer:

    def avg(station: str, observation: str, sample_duration: str) -> str:
        return f"select valid_time_gmt, avg({observation}) from weather_{station} SAMPLE by {sample_duration} ALIGN TO CALENDAR;"

    def median(station: str, observation: str, sample_duration: str) -> str:
        return f"select valid_time_gmt, approx_percentile({observation}, 0.5) from weather_{station} SAMPLE by {sample_duration} ALIGN TO CALENDAR;"

    def max(station: str, observation: str, sample_duration: str) -> str:
        return f"select valid_time_gmt, max({observation}) from weather_{station} SAMPLE by {sample_duration} ALIGN TO CALENDAR;"

    def min(station: str, observation: str, sample_duration: str) -> str:
        return f"select valid_time_gmt, min({observation}) from weather_{station} SAMPLE by {sample_duration} ALIGN TO CALENDAR;"

    def stddev(station: str, observation: str, sample_duration: str) -> str:
        return f"select valid_time_gmt, stddev({observation}) from weather_{station} SAMPLE by {sample_duration} ALIGN TO CALENDAR;"

    _transformers = [
        avg,
        median,
        max,
        min,
        stddev
    ]