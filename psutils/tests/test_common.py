import pytest  # noqa: F401


def test_dbfs_path():
    from psutils import DbfsPath

    dbfs_path = DbfsPath("/dbfs/")
    alt_dbfs_path = DbfsPath.from_dbfs("dbfs:/")

    assert str(dbfs_path) == str(alt_dbfs_path)
