from __future__ import annotations

import os
from typing import Any, Callable, Literal, TypeVar

import pytest

from dask.utils import (
    _deprecated,
    _deprecated_with_version,
    _get_version,
    apply,
    get_default_shuffle_method,
    get_meta_library,
    is_namedtuple_instance,
    maybe_pluralize,
    shorten_traceback,
)


def test_get_meta_library():
    assert get_meta_library("pandas.DataFrame") == "pandas"
    assert get_meta_library("pandas") == "pandas"
    assert isinstance(get_meta_library("pandas"), str)


def test_is_namedtuple_instance():
    assert not is_namedtuple_instance(1)
    assert not is_namedtuple_instance(None)
    assert not is_namedtuple_instance("foo")
    assert not is_namedtuple_instance([1, 2, 3])
    assert not is_namedtuple_instance({"a": 1, "b": 2})
    assert not is_namedtuple_instance({"a": 1, "b": 2, "_fields": ["a", "b"]})
    assert is_namedtuple_instance((1, 2, 3))
    assert is_namedtuple_instance((1, 2, 3, "_fields": ["a", "b"]))
    assert is_namedtuple_instance((1, 2, 3, "_fields": ["a", "b"], "_field_defaults": [1, 2]))


def test_shorten_traceback():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback() as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=["foo"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all2():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all3():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all4():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all5():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all6():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all7():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all8():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all9():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all10():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all11():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all12():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all13():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all14():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all15():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all16():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all17():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all18():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all19():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all20():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all21():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all22():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all23():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all24():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all25():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all26():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all27():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all28():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all29():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all30():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all31():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all32():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all33():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all34():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all35():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all36():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all37():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all38():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all39():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
            raise ValueError("bar")

        with pytest.raises(ValueError) as exc:
            bar()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "bar"


def test_shorten_traceback_no_shorten_all40():
    @shorten_traceback()
    def foo():
        raise ValueError("foo")

    with pytest.raises(ValueError) as exc:
        foo()

    tb = exc.value.__traceback__
    assert tb.tb_next is not None
    assert tb.tb_frame.f_code.co_name == "foo"

    with shorten_traceback(paths=[".*"]) as ctx:
        @shorten_traceback()
        def bar():
           