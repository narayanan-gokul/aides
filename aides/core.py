"""core."""

import base64
import functools
import hmac
import json
import logging
import pickle
import struct
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from rapidfuzz import fuzz
from tqdm import tqdm

logger = logging.getLogger(__name__)


def dtto(dtt_any, *, tz="UTC"):
    """Convert to a standardized datetime object.

    tz merely attaches timezone information. There is no real timezone conversion.

    Args:
        dtt_any (Union[datetime, str, pd.Timestamp]): Input datetime-like object.
        tz (str, optional): The desired time zone to attach. Defaults to "UTC".

    Returns:
        pd.Timestamp: Standardized pandas datetime object with timezone attached.
    """
    tzinfo = ZoneInfo(tz)
    if isinstance(dtt_any, datetime):
        dtt_any = dtt_any.replace(tzinfo=None)
    elif isinstance(dtt_any, str):
        try:
            dtt_any = pd.to_datetime(dtt_any, format="%Y-%m-%d %H:%M:%S")
        except ValueError:
            msg = "Input format must be: 'YYYY-MM-DD HH:MM:SS'"
            raise TypeError(msg) from None
    elif isinstance(dtt_any, pd.Timestamp):
        pass
    else:
        msg = "Input must be a datetime object, a string, or a pd.Timestamp object."
        raise TypeError(msg)
    return pd.Timestamp(dtt_any).tz_localize(tz=tzinfo).replace(microsecond=0)


def now(tz="Asia/Kolkata", tz_attached="UTC"):
    """Get the current time in "UTC" with timezone attached.

    Output clock time remains same. tz just attaches different timezone information.

    Args:
        tz (str, optional): The timezone of which the current time is required.
        tz_attached (str, optional): The desired time zone to attach. Defaults to "UTC".

    Returns:
        pd.Timestamp: The current time in 'Asia/Kolkata' with timezone attached.
    """
    return dtto(datetime.now(tz=ZoneInfo(tz)), tz=tz_attached)


def dtts(dtt_any, *, tz="UTC"):
    """Convert to an ISO 8601 string format.

    Args:
        dtt_any (Union[datetime, str, pd.Timestamp]): Input datetime-like object.
        tz (str, optional): The desired time zone to attach. Defaults to "UTC".

    Returns:
        str: The ISO 8601 formatted datetime string.
    """
    return dtto(dtt_any, tz=tz).isoformat(timespec="seconds")


def epoch(dtt_any, *, tz="Asia/Kolkata"):
    """Convert to a Unix timestamp till dtt_any.

    Epoch will be different for same clock time but different timezone info.
    Because the end time will be different for each timezone.
    dtto does not convert tz, and by default clock time is local & timezone is "UTC".
    So, actual correct current timezone of dtt_any is required.
    As .timestamp() method converts timezone aware object to UTC automatically.

    Args:
        dtt_any (Union[datetime, str, pd.Timestamp]): Input datetime-like object.
        tz (str, optional): The actual time zone of dtt_any. Defaults to "Asia/Kolkata".

    Returns:
        int: The Unix timestamp.
    """
    return int(dtto(dtt_any, tz=tz).timestamp())


def seconds_until(target="day", tz="Asia/Kolkata"):
    """Calculate the remaining seconds until a specific end target.

    Args:
        target (str, optional): End Target, 'day', 'week', 'month'. Defaults to "day".
        tz (str, optional): Timezone for the end target.

    Returns:
        int: Remaining seconds until the end target.
    """
    start = now(tz=tz, tz_attached=tz)
    if target == "day":
        end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif target == "week":
        end_week = start + timedelta(days=6 - start.weekday())
        end = end_week.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif target == "month":
        next_month = (now.month % 12) + 1
        year = now.year + (now.month // 12)
        end_of_month = now.replace(
            month=next_month,
            year=year,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ) - timedelta(microseconds=1)
        end = end_of_month
    else:
        msg = "Invalid target. Choose from 'day', 'week', or 'month'."
        raise ValueError(msg)
    return int((end - start).total_seconds())


def range_dtt(dtt_start, dtt_end, interval, tz="UTC"):
    """Generate time ranges between two datetime-like objects.

    Args:
        dtt_start (Union[datetime, str, pd.Timestamp]): Start datetime-like object.
        dtt_end (Union[datetime, str, pd.Timestamp]): End datetime-like object.
        interval (int): The interval length in days.
        tz (str, optional): The desired time zone to attach. Defaults to "UTC".

    Returns:
        List[Tuple[str, str]]: List of datetime ranges as tuples of ISO 8601 strings.
    """
    start, end = (dtto(dtt_start), dtto(dtt_end))
    ranges = []
    while start < end:
        next_interval = min(start + timedelta(days=interval), end) - timedelta(
            minutes=1,
        )
        ranges.append((dtts(start, tz=tz), dtts(next_interval, tz=tz)))
        start = next_interval + timedelta(minutes=1)
    return ranges


def make_path(*args):
    """Join path components into a single Path object.

    Args:
        *args: Individual path components as strings or Path/PosixPath objects.

    Returns:
        Path: Combined PosixPath object.
    """
    paths = [str(arg) for arg in args]
    paths_normalized = [paths[0]] + [path.lstrip("/") for path in paths[1:]]
    return Path(paths_normalized[0]).joinpath(*paths_normalized[1:]).expanduser()


def make_dir(*args):
    """Create a directory from the given path components.

    Args:
        *args (Union[str, Path, PosixPath]): Individual path components.

    Returns:
        Path: Path object of the created directory.
    """
    path = make_path(*args)
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_files(*args, pattern="*"):
    """List all files in a specified directory and subdir matching a glob pattern.

    Args:
        *args (Union[str, Path, PosixPath]): Individual path components to target.
        pattern (str): The glob pattern to match files.

    Returns:
        List[Path]: Path objects of all files in target directory matching glob pattern.
    """
    path = make_path(*args)
    if path.is_dir():
        return sorted(path.rglob(pattern))
    msg = f"Path {path} either does not exist or is not a directory."
    raise OSError(msg)


def list_subdirs(*args, depth=1):
    """List all subdirectories of a specified directory up to a specified depth.

    Args:
        *args (Union[str, Path, PosixPath]): Individual path components to target.
        depth (int): Depth of subdirectories to list. Defaults to 1.

    Returns:
        List[Path]: Paths of all subdirectories in target up to specified depth.
    """
    path = make_path(*args)
    if path.is_dir():
        pattern = "*/" * depth
        return sorted([subdir for subdir in path.glob(pattern) if subdir.is_dir()])
    msg = f"Path {path} either does not exist or is not a directory."
    raise OSError(msg)


def zip_folder(folder_path):
    """Create a zip file from a local folder including subfolders and files.

    Defaults to output in home directory of user.

    Args:
        folder_path (Path or str): Path to the folder to zip.

    Returns:
        pathlib.Path: The path object of the created zip file.
    """
    folder_path = make_path(folder_path)
    zip_filename = f"{folder_path.name}.zip"
    zip_filepath = make_path(folder_path.parent, zip_filename)

    with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zipf:
        for item in folder_path.rglob("*"):
            arcname = item.relative_to(folder_path)
            zipf.write(item, arcname=arcname)
    return zip_filepath


def read_json(path_or_url):
    """Read a JSON file from a local path or a URL.

    Args:
        path_or_url (str): The local path or URL to the JSON file.

    Returns:
        dict: The parsed JSON data.
    """
    parsed = urlparse(str(path_or_url))
    if parsed.scheme in ["http", "https"]:
        resp = requests.get(path_or_url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    with make_path(path_or_url).open("r") as file_json:
        return json.load(file_json)


def write_json(file_path, data_json, default=None):
    """Write JSON data to a file at a specified path.

    Args:
        file_path (Path or str): The path to the file where data should be written.
        data_json (dict): The JSON data to write to the file.
        default (callable, optional): Function for serializing non-serializable objects.

    Returns:
        pathlib.Path: The path object of the created JSON file.
    """
    file_path = make_path(file_path)
    make_dir(file_path.parent)
    with file_path.open("w") as file_json:
        json.dump(data_json, file_json, indent=2, default=default)
    return file_path


def update_json(file_path, key_path, value, default=None):
    """Update multiple key-value pairs in a JSON file with values from given dictionary.

    Args:
        file_path (str): The path to the JSON file.
        key_path (str): The dot-separated path to the key to update.
        value: The new value to set at the specified key path.
        default (callable, optional): Function for serializing non-serializable objects.

    Returns:
        pathlib.Path: The path object of the updated JSON file.
    """
    data_json = read_json(file_path)

    def update_path(d, keys):
        """Navigate and update the dictionary."""
        for key in keys[:-1]:
            if key not in d or not isinstance(d[key], dict):
                msg = f"Path '{'.'.join(keys[:-1])}' does not exist or is not a dict."
                raise KeyError(msg)
            d = d[key]
        if keys[-1] in d:
            d[keys[-1]] = value
        else:
            msg = f"Key '{keys[-1]}' does not exist in the JSON structure."
            raise KeyError(msg)

    update_path(data_json, key_path.split("."))
    return write_json(file_path, data_json, default=default)


def read_pickle(file_path):
    """Read and return data from a pickle file.

    Args:
        file_path (Path or str): The path to the pickle file to read from.

    Returns:
        Any: The deserialized data from the pickle file.
    """
    file_path = make_path(file_path)
    with file_path.open("rb") as file_pickle:
        return pickle.load(file_pickle)  # noqa: S301 : pickle use is offline


def write_pickle(file_path, data_pickle):
    """Write data to a pickle file at a specified path.

    Args:
        file_path (Path or str): Path to the file to write the pickled object.
        data_pickle (Any): The data to write to the pickle file.

    Returns:
        pathlib.Path: The path object of the created Pickle file.
    """
    file_path = make_path(file_path)
    make_dir(file_path.parent)
    with file_path.open("wb") as file_pickle:
        pickle.dump(data_pickle, file_pickle)
    return file_path


def read_gsheet(sheet_id, sheet_name):
    """Read data from a Google Spreadsheet and return it as a pandas DataFrame.

    Args:
        sheet_id (str): The Spreadsheet's ID.
        sheet_name (str): The name of the sheet within Spreadsheet to read.

    Returns:
        pd.DataFrame: Data read from the specified Google Sheet.
    """
    url = f"https://docs.google.com/spreadsheets/d/{
        sheet_id
    }/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    return pd.read_csv(url)


def split_list(data_list, size):
    """Split a list into smaller lists of a given size.

    Args:
        data_list (list): The list to split.
        size (int): The size of each resulting smaller list.

    Returns:
        list: List of smaller lists.
    """
    return [data_list[i : i + size] for i in range(0, len(data_list), size)]


def nearest_inlist(pivot, data_list, n=0):
    """Find the nearest value(s) to pivot from a list.

    Args:
        pivot (int or float): The pivot value for comparison.
        data_list (list): List of numbers to find the nearest value from.
        n (int, optional): Number of additional nearest values to return. Defaults to 0.

    Returns:
        list or None: Nearest value(s) to the pivot or None if data_list is empty.
    """
    if not data_list:
        return None
    data_list_sorted = sorted(data_list)
    distances = {x: abs(x - pivot) for x in data_list_sorted}

    def distance_key(x):
        return distances[x]

    value_nearest = min(distances, key=distance_key)
    if n == 0:
        return value_nearest
    data_list_sorted.remove(value_nearest)
    items_lower = [i for i in data_list_sorted if i <= pivot]
    items_upper = [i for i in data_list_sorted if i > pivot]
    items_remain = sorted(items_lower, reverse=True)[:n] + sorted(items_upper)[:n]
    return [value_nearest, *sorted(items_remain)]


def sort_dict(data_dict):
    """Sort a dictionary and all nested dictionaries.

    Args:
        data_dict (dict): Dictionary to be sorted.

    Returns:
        dict: Sorted dictionary.
    """
    dict_sorted = {}
    for k, v in sorted(data_dict.items()):
        if isinstance(v, dict):
            dict_sorted[k] = sort_dict(v)
        elif isinstance(v, list | set | tuple):
            try:
                dict_sorted[k] = sorted(v)
            except TypeError:
                dict_sorted[k] = v
        else:
            dict_sorted[k] = v
    return dict_sorted


def safe_call(func, *args):
    """Execute a function safely, catching ValueError and TypeError exceptions.

    Args:
        func (callable): The function to execute.
        *args: Variable arguments to pass to the function.

    Returns:
        Any: The result of the function if successful, otherwise the caught exception.
    """
    try:
        return func(*args)
    except (ValueError, TypeError) as exc:
        logger.exception("Error in concurrent execution: %s")
        return exc


def run_concurrent(func, args_list, chunksize=None, *, use_threads=False):
    """Execute function concurrently using processes or threads on chunks of arguments.

    Args:
        func (callable): The function to execute concurrently.
        args_list (list): List of argument tuples to pass to the function.
        chunksize (int, optional): Tasks to submit per iteration. Default to len args.
        use_threads (bool, optional): Use Thread instead of Process. Defaults to False.

    Returns:
        list: List of results from the function execution in preserved order.
    """
    results = [None] * len(args_list)
    futures_dict = {}  # Dictionary to map futures to their indices
    chunksize = chunksize or len(args_list)
    executorclass = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
    with executorclass() as executor:
        tq = tqdm(
            total=len(args_list),
            desc=f"Processing {func.__name__}",
            unit="task",
        )
        # Submit tasks and map futures to indices
        for index, args in enumerate(args_list):
            future = executor.submit(safe_call, func, *args)
            futures_dict[future] = index
        # Retrieve results in order of completion
        for future in as_completed(futures_dict):
            result = future.result()
            # Place results in correct order
            results[futures_dict[future]] = result
            tq.update(1)
        tq.close()
    return results


def timer(func):
    """Time the execution of a function.

    Args:
        func (callable): The function to be timed.

    Returns:
        callable: The wrapped function with timing.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        """Wrapper function for timing.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Any: The return value of the wrapped function.
        """
        time_start = time.perf_counter()
        value = func(*args, **kwargs)
        time_end = time.perf_counter()
        run_time = time_end - time_start
        print(f"Finished {func.__name__!r} in {round(run_time, 3)} secs")  # noqa: T201
        return value

    return wrapper


def match_partial(pivot, data_list, cutoff=90):
    """Find a string in a list that partially matches the pivot given cutoff score.

    Args:
        pivot (str): The string to match.
        data_list (list): List of strings to search through.
        cutoff (int, optional): Minimum score for a match. Defaults to 90.

    Returns:
        str or None: The matching string or None if no match is found.
    """
    for string in data_list:
        score = fuzz.partial_ratio(pivot, string)
        if score >= cutoff:
            return string
    return None


def totp(key, digits=6, timestep=30, digest="sha1"):
    """Generate a Time-based One-Time Password (TOTP) based on a given key.

    Args:
        key (str): Base32-encoded secret key.
        digits (int, optional): Number of digits in the generated TOTP. Defaults to 6.
        timestep (int, optional): Time step in seconds for TOTP. Defaults to 30.
        digest (str, optional): Hash function to use. Defaults to "sha1".

    Returns:
        str: Generated TOTP as a string of numbers.
    """
    # Normalize key to correct length for base32 decoding
    key = key.upper() + "=" * ((8 - len(key)) % 8)
    # Decode the base32 encoded key
    decoded = base64.b32decode(key)
    # Calculate counter as big-endian
    counter = struct.pack(">Q", int(time.time() / timestep))
    # Create HMAC hash
    hashed = hmac.new(decoded, counter, digest).digest()
    # Use last nibble (4 bits) of hash to get an offset
    offset = hashed[-1] & 0x0F
    # Get 4 bytes of hash, starting at offset
    hash_truncated = hashed[offset : offset + 4]
    # Convert bytes to int and apply mask to get code
    code = struct.unpack(">L", hash_truncated)[0] & 0x7FFFFFFF
    # Convert code to string and return last 'digits' characters
    return str(code)[-digits:].zfill(digits)
