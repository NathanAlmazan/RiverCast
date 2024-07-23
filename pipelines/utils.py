from datetime import datetime


def format_timestamp(timestamp: str) -> str:
    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
    timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S+08")
    return timestamp


def format_float(value: str) -> float | None:
    try:
        value = value.replace("(*)", "")
        value = float(value)
        return value
    except ValueError:
        return None
