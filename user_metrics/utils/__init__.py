"""
    Any general purpose utilities useful in the project are defined here.
"""

MW_TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"
from dateutil.parser import parse as date_parse
from collections import namedtuple, OrderedDict
from hashlib import sha1


def format_mediawiki_timestamp(timestamp_repr):
    """
        Convert representation to mediawiki timestamps.  Returns a sring
         timestamp in the MediaWiki Format.

        Parameters
        ~~~~~~~~~~

        timestamp_repr : str|datetime
           Datetime representation to convert.
    """
    if hasattr(timestamp_repr, 'strftime'):
        return timestamp_repr.strftime(MW_TIMESTAMP_FORMAT)
    else:
        return date_parse(timestamp_repr).strftime(
            MW_TIMESTAMP_FORMAT)


def enum(*sequential, **named):
    """
        Implemetents an enumeration::

            >>> Numbers = enum('ZERO', 'ONE', 'TWO')
            >>> Numbers.ZERO
            0
            >>> Numbers.ONE
            1
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


def build_namedtuple(names, types, values):
    """
        Given a set of types, values, and names builds a named tuple.  This
        method expects three lists all of the same length and returns a
        dynamically built namedtuple object.  Currently, only ``list``,
        ``str``, ``int``, and ``float`` cast methods are accepted as members
        of ``types``.

        Parameters
        ~~~~~~~~~~

        names : list
           Strings representing attribute names.

        types : list
           Typecast methods for each value.

        values : list
           Values of attributes.  These may be string.
    """
    param_type = namedtuple('build_namedtuple', ' '.join(names))

    arg_list = list()
    for t, v in zip(types, values):
        if t == str:
            arg_list.append("'" + str(v) + "'")
        elif t == int or t == list or t == float or t == bool:
            arg_list.append(str(v))

    return eval('param_type(' + ','.join(arg_list) + ')')


def unpack_fields(obj):
    """
        Unpacks the values from a named tuple into a dict.  This method
        expects the '_fields' or 'todict' attribute to exist.  namedtuples
        expose the fromer interface while recordtypes expose the latter.
    """
    d = OrderedDict()

    if hasattr(obj, '_fields'):
        for field in obj._fields:
            d[field] = getattr(obj, field)
    elif hasattr(obj, 'todict'):
        d = OrderedDict(obj.todict())

    return d


def nested_import(name):
    """
        Using ``__import__`` retrieve nested object/namespace.  Solution_
        couresty of stack overflow user dwestbook_.

        .. _Solution: http://stackoverflow.com/questions/
        211100/pythons-import-doesnt-work-as-expected
        .. _dwestbrook: http://stackoverflow.com/users/3119/dwestbrook
    """
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def reverse_dict(d):
    """ Simply reverse a dictionary mapping """
    return dict((v,k) for k,v in d.iteritems())


def build_key_tree(nested_dict):
    """ Builds a tree of key values from a nested dict. """
    if hasattr(nested_dict, 'keys'):
        for key in nested_dict.keys():
            yield (key, build_key_tree(nested_dict[key]))
    else:
        yield None


def get_keys_from_tree(tree):
    """
        Depth first traversal - get the key signatures from structure
         produced by ``build_key_tree``.
    """
    key_sigs = list()
    for node in tree:
        stack_trace = [node]
        while stack_trace:
            if stack_trace[-1]:
                ptr = stack_trace[-1][1]
                try:
                    stack_trace.append(ptr.next())
                except StopIteration:
                    # no more children
                    stack_trace.pop()
            else:
                key_sigs.append([elem[0] for elem in stack_trace[:-1]])
                stack_trace.pop()
    return key_sigs



def salt_string(unencoded_string, secret_key):
    """
        Produces a hash code using the SHA-1 hashing algorithm.

        Parameters
        ~~~~~~~~~~

            unencoded_string : string
                The string to be encoded.

            secret_key : string
                The secret key, appended to the string to be coded in order to
                produce a more secure hash.
    """

    hash_str = secret_key.strip() + unencoded_string.strip()
    hash_obj = sha1(hash_str)
    hash_str = hash_obj.hexdigest()

    return hash_str


def terminate_process_with_checks(proc):
    """
        Gracefully terminates a process exposing the correct interface
    """
    if proc and hasattr(proc, 'is_alive') and proc.is_alive() and \
       hasattr(proc, 'terminate'):
        proc.terminate()


# Rudimentary Testing
if __name__ == '__main__':
    t = build_namedtuple(['a', 'b'], [int, str], [1, 's'])
    print t
