from __future__ import annotations

import codecs
import functools
import inspect
import os
import re
import shutil
import sys
import tempfile
import types
import uuid
import warnings
from collections.abc import Hashable, Iterable, Iterator, Mapping, Set
from contextlib import contextmanager, nullcontext, suppress
from datetime import datetime, timedelta
from errno import ENOENT
from functools import lru_cache, wraps
from importlib import import_module
from numbers import Integral, Number
from operator import add
from threading import Lock
from typing import Any, Callable, ClassVar, Literal, TypeVar, cast, overload
from weakref import WeakValueDictionary

import tlz as toolz

from dask import config
from dask.core import get_deps
from dask.typing import no_default

K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")

# used in decorators to preserve the signature of the function it decorates
# see https://mypy.readthedocs.io/en/stable/generics.html#declaring-decorators
FuncType = Callable[..., Any]
F = TypeVar("F", bound=FuncType)

system_encoding = sys.getdefaultencoding()
if system_encoding == "ascii":
    system_encoding = "utf-8"


def apply(func, args, kwargs=None):
    """Apply a function given its positional and keyword arguments.

    Equivalent to ``func(*args, **kwargs)``
    Most Dask users will never need to use the ``apply`` function.
    It is typically only used by people who need to inject
    keyword argument values into a low level Dask task graph.

    Parameters
    ----------
    func : callable
        The function you want to apply.
    args : tuple
        A tuple containing all the positional arguments needed for ``func``
        (eg: ``(arg_1, arg_2, arg_3)``)
    kwargs : dict, optional
        A dictionary mapping the keyword arguments
        (eg: ``{"kwarg_1": value, "kwarg_2": value}``

    Examples
    --------
    >>> from dask.utils import apply
    >>> def add(number, second_number=5):
    ...     return number + second_number
    ...
    >>> apply(add, (10,), {"second_number": 2})  # equivalent to add(*args, **kwargs)
    12

    >>> task = apply(add, (10,), {"second_number": 2})
    >>> dsk = {'task-name': task}  # adds the task to a low level Dask task graph
    """
    if kwargs:
        return func(*args, **kwargs)
    else:
        return func(*args)


def _deprecated(
    *,
    version: str | None = None,
    after_version: str | None = None,
    message: str | None = None,
    use_instead: str | None = None,
    category: type[Warning] = FutureWarning,
):
    """Decorator to mark a function as deprecated

    Parameters
    ----------
    version : str, optional
        Version of Dask in which the function was deprecated. If specified, the version
        will be included in the default warning message. This should no longer be used
        after the introduction of automated versioning system.
    after_version : str, optional
        Version of Dask after which the function was deprecated. If specified, the
        version will be included in the default warning message.
    message : str, optional
        Custom warning message to raise.
    use_instead : str, optional
        Name of function to use in place of the deprecated function.
        If specified, this will be included in the default warning
        message.
    category : type[Warning], optional
        Type of warning to raise. Defaults to ``FutureWarning``.

    Examples
    --------

    >>> from dask.utils import _deprecated
    >>> @_deprecated(after_version="X.Y.Z", use_instead="bar")
    ... def foo():
    ...     return "baz"
    """

    def decorator(func):
        if message is None:
            msg = f"{func.__name__} "
            if after_version is not None:
                msg += f"was deprecated after version {after_version} "
            elif version is not None:
                msg += f"was deprecated in version {version} "
            else:
                msg += "is deprecated "
            msg += 
# ... [truncated] ...
rom importlib.metadata import PackageNotFoundError, version
    from json import dumps
    from platform import uname
    from sys import stdout, version_info

    try:
        from distributed import __version__ as distributed_version
    except ImportError:
        distributed_version = None

    from dask import __version__ as dask_version

    deps = [
        "numpy",
        "pandas",
        "cloudpickle",
        "fsspec",
        "bokeh",
        "pyarrow",
        "zarr",
    ]

    result: dict[str, str | None] = {
        # note: only major, minor, micro are extracted
        "Python": ".".join([str(i) for i in version_info[:3]]),
        "Platform": uname().system,
        "dask": dask_version,
        "distributed": distributed_version,
    }

    for modname in deps:
        try:
            result[modname] = version(modname)
        except PackageNotFoundError:
            result[modname] = None

    stdout.writelines(dumps(result, indent=2))

    return


def maybe_pluralize(count, noun, plural_form=None):
    """Pluralize a count-noun string pattern when necessary"""
    if count == 1:
        return f"{count} {noun}"
    else:
        return f"{count} {plural_form or noun + 's'}"


def is_namedtuple_instance(obj: Any) -> bool:
    """Returns True if obj is an instance of a namedtuple.

    Note: This function checks for the existence of the methods and
    attributes that make up the namedtuple API, so it will return True
    IFF obj's type implements that API.
    """
    return (
        isinstance(obj, tuple)
        and hasattr(obj, "_make")
        and hasattr(obj, "_asdict")
        and hasattr(obj, "_replace")
        and hasattr(obj, "_fields")
        and hasattr(obj, "_field_defaults")
    )


def get_default_shuffle_method() -> str:
    if d := config.get("dataframe.shuffle.method", None):
        return d
    try:
        from distributed import default_client

        default_client()
    except (ImportError, ValueError):
        return "disk"

    try:
        from distributed.shuffle import check_minimal_arrow_version

        check_minimal_arrow_version()
    except ModuleNotFoundError:
        return "tasks"
    return "p2p"


def get_meta_library(like):
    if hasattr(like, "_meta"):
        like = like._meta

    return import_module(typename(like).partition(".")[0])


class shorten_traceback:
    """Context manager that removes irrelevant stack elements from traceback.

    * omits frames from modules that match `admin.traceback.shorten`
    * always keeps the first and last frame.
    """

    __slots__ = ()

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        if exc_val and exc_tb:
            exc_val.__traceback__ = self.shorten(exc_tb)

    @staticmethod
    def shorten(exc_tb: types.TracebackType) -> types.TracebackType:
        paths = config.get("admin.traceback.shorten")
        if not paths:
            return exc_tb

        exp = re.compile(".*(" + "|".join(paths) + ")")
        curr: types.TracebackType | None = exc_tb
        prev: types.TracebackType | None = None

        while curr:
            if prev is None:
                prev = curr  # first frame
            elif not curr.tb_next:
                # always keep last frame
                prev.tb_next = curr
                prev = prev.tb_next
            elif not exp.match(curr.tb_frame.f_code.co_filename):
                # keep if module is not listed in config
                prev.tb_next = curr
                prev = curr
            curr = curr.tb_next

        # Uncomment to remove the first frame, which is something you don't want to keep
        # if exc_tb.tb_next and exp.match(exc_tb.tb_frame.f_code.co_filename):
        #     return exc_tb.tb_next

        return exc_tb

