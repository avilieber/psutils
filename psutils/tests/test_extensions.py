import pytest  # noqa: F401


def test_accessor_register():
    # extension hasnt been imported yet,
    # so DataFrame should not be decorated
    from pyspark.sql import DataFrame

    assert not hasattr(DataFrame, "withColumns")

    # we should now be able to import the module and get all accessors
    import psutils.extensions  # noqa: 401

    assert hasattr(DataFrame, "withColumns")
