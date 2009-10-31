from nose.tools import assert_raises

from orm import util


def test_slice2limit_no_args():
    limit = util.slice2limit(slice(None))
    assert limit is None, limit


def test_slice2limit_upper():
    limit = util.slice2limit(slice(10))
    assert limit == 'limit 10', limit


def test_slice2limit_lower():
    limit = util.slice2limit(slice(10, None))
    assert limit == 'limit 10, -1', limit


def test_slice2limit_lower_and_upper():
    limit = util.slice2limit(slice(10, 11))
    assert limit == 'limit 10, 1', limit


def test_slice2limit_upper_less_than_lower_raises_valueerror():
    assert_raises(ValueError, util.slice2limit, slice(11, 9))


def test_slice2limit_step_raises_typeerror():
    assert_raises(TypeError, util.slice2limit, slice(10, 11, 2))


def test_slice2limit_negative_values_raise_notimplementederror():
    assert_raises(NotImplementedError, util.slice2limit, slice(-1))
    assert_raises(NotImplementedError, util.slice2limit, slice(-1, None))
