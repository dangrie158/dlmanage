from glob import glob
import re
from typing import List, Optional


def camel_to_snake_case(input: str) -> str:
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", input).lower()


def snake_to_camel_case(input: str) -> str:
    return "".join([part.title() for part in input.split("_")])


def get_gres_value(haystack: str, needle: str) -> Optional[str]:
    if haystack is None:
        return None

    parts = haystack.split(",")
    for kv_pair in parts:
        if "=" not in kv_pair:
            continue

        key, value = kv_pair.split("=")
        if key == needle:
            return value

    return None


def update_gres_value(haystack: str, needle: str, new_value: str) -> str:
    if haystack is None:
        haystack = ""

    if new_value == None:
        new_value = "-1"

    new_haystack_parts: List[str] = []
    parts = haystack.split(",")
    for kv_pair in parts:
        if "=" not in kv_pair:
            continue

        key, current_value = kv_pair.split("=")
        value = new_value if key == needle else current_value
        new_haystack_parts.append(f"{key}={value}")

    # if we could not update an old value, add a new entry
    if needle not in haystack:
        new_haystack_parts.append(f"{needle}={new_value}")

    return ",".join(new_haystack_parts)


def find_home_directory(username: str) -> str | None:
    if len(username) == 0:
        return None

    patterns = (
        f"/home/stud/{username[0]}/{username}",
        f"/home/ma/{username[0]}/{username}",
        f"/home/*/{username}/",
    )
    for candidate_pattern in patterns:
        candidates = glob(candidate_pattern)
        if len(candidates) > 0:
            return candidates[0]
    return None
