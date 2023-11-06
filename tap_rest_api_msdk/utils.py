"""Basic utility functions."""

import json
from typing import Any, Optional


def flatten_json(
    obj: dict,
    except_keys: Optional[list] = None,
    store_raw_json_message: Optional[bool] = False,
) -> dict:
    """Flattens a json object by appending the patch as a key in the returned object.

    Automatically converts arrays and any provided keys into json strings to prevent
    flattening further into those branches.

    Args:
        obj: the json object to be flattened.
        except_keys: list of the keys of the nodes that should be converted to json
            strings.
        store_raw_json_message: Additionally adds the raw JSON message to a field
        named _sdc_raw_json. Note: The field is a JSON type of object.

    Returns:
        A flattened json object.

    """
    out = {}
    if not except_keys:
        except_keys = []

    def t(s: str) -> str:
        """Translate a string to db friendly column names.

        Args:
            s: required - string to make a translation table from.

        Returns:
            Translation table.

        """
        translation_table = s.maketrans("-.", "__")
        return s.translate(translation_table)

    def flatten(o: Any, exception_keys: list, name: str = "") -> None:
        """Recursive flattening of the json object in place.

        Args:
            o: the json object to be flattened.
            exception_keys: list of the keys of the nodes that should
                be converted to json strings.
            name: the prefix for the exception_keys

        """
        if type(o) is dict:
            for k in o:
                # the key is in the list of keys to skip, convert to json string
                if name + k in exception_keys:
                    out[t(name + k)] = json.dumps(o[k])
                else:
                    flatten(o[k], exception_keys, name + k + "_")

        # if the object is an array, convert to a json string
        elif type(o) is list:
            out[t(name[:-1])] = json.dumps(o)

        # otherwise, translate the key to be database friendly
        else:
            out[t(name[:-1])] = o

    flatten(obj, exception_keys=except_keys)
    # Optional store the whole row in the _sdc_raw_json field.
    if store_raw_json_message:
        out["_sdc_raw_json"] = obj  # type: ignore[assignment]
    return out


def unnest_dict(d):
    """Flattens a dict object by create a new object with the key value pairs.

    Recursive flattening any nested dicts to a single level.

    Args:
        obj: the dict object to be flattened.

    Returns:
        A flattened dict object.

    """
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            result.update(unnest_dict(v))
        else:
            result[k] = v
    return result


def get_start_date(self, context: Optional[dict]) -> Any:
    """Return a start date if a DateTime bookmark is available.

    Otherwise it returns the starting date as defined in
    the start_date parameter.

    Args:
        context: - the singer context object.

    Returns:
        An start date else and empty string.

    """
    try:
        return self.get_starting_timestamp(context).strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, AttributeError):
        return self.get_starting_replication_key_value(context)
