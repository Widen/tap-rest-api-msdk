import json


def flatten_json(obj, except_keys=None):
    """Flattens a json object by appending the patch as a key in the returned object.
    Automatically converts arrays and any provided keys into json strings to prevent
    flattening further into those branches.

    Args:
        obj (dict): the json object to be flattened.
        except_keys (Optional[list(str)]): list of the keys of the nodes that should
        be converted to json strings.
    """
    out = {}
    if not except_keys:
        except_keys = []

    def t(s):
        """Returns a translation table for converting strings in database friendly column names"""
        translation_table = s.maketrans('-.', '__')
        return s.translate(translation_table)

    def flatten(o, exception_keys, name=''):
        """Performs the recursive flattening of the json object

        Args:
            o (dict): the json object to be flattened.
            exception_keys (list(str)): list of the keys of the nodes that should
            be converted to json strings.
            name (str): the prefix for the exception_keys
        """
        if type(o) is dict:
            for k in o:
                # the key is in the list of keys to skip, convert to json string
                if name + k in exception_keys:
                    out[t(name + k)] = json.dumps(o[k])
                else:
                    flatten(o[k], exception_keys, name + k + '_')

        # if the object is an array, convert to a json string
        elif type(o) is list:
            out[t(name[:-1])] = json.dumps(o)

        # otherwise, translate the key to be database friendly
        else:
            out[t(name[:-1])] = o

    flatten(obj, exception_keys=except_keys)
    return out
