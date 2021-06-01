"Data loading functions"

from functools import reduce

from .common import DbfsPath, spark


def safe_union_two_by_name(df1, df2):
    """Combine common fields from two dataframes rowwise.

    Note we do not use the ``pyspark.sql.DataFrame.unionByName``
    function here because we explicitly reorder columns to the
    order of ``take``.

    Parameters
    ----------
    df1 : pyspark.sql.DataFrame
        first dataframe to combine
    df2 : pyspark.sql.DataFrame
        second dataframe to combine

    Returns
    -------
    out : pyspark.sql.DataFrame

    """
    take = [c for c in df1.columns if c in df2.columns]
    df1 = df1.select(take)
    df2 = df2.select(take)
    out = df1.union(df2)
    return out


def safe_union_by_name(*dfs):
    """Combine common fields from dataframes rowwise.

    Generalizes ``safe_union_two_by_name`` to an arbitrary
    number of dataframes.

    Parameters
    ----------
    *dfs : list of dataframes to combine

    Returns
    -------
    out : pyspark.sql.DataFrame

    """
    out = reduce(safe_union_two_by_name, dfs)
    return out


def safe_load(*paths, **read_opts):
    """Read and union files from multiple paths.

    Parameters
    ----------
    *paths : list of filepaths (str) to read into spark

    **read_opts : passed onto ``spark.read.load``

    Returns
    -------
    out : pyspark.sql.DataFrame

    """
    out = safe_union_by_name(*[spark.read.load(path, **read_opts) for path in paths])
    return out


SVF_PATH = "dbfs:/mnt/files/silver/svf/parquet_new/"


def load_svf():
    """Read and union files from multiple paths.

    Returns
    -------
    svf : pyspark.sql.DataFrame

    """
    standard_path = DbfsPath.from_dbfs(SVF_PATH)
    hdt_path = standard_path / "h{1,4}_dt=*/"
    dt_path = standard_path / "dt=*/"
    svf_hdt = spark.read.parquet(hdt_path.to_dbfs())
    svf_hdt = svf_hdt
    svf_dt = spark.read.parquet(dt_path.to_dbfs())
    svf = svf_hdt.unionByName(svf_dt, allowMissingColumns=True)
    return svf


RVF_PATH = "dbfs:/mnt/files/silver/rvf/parquet_new/"


def load_rvf():
    """Read and union files from multiple paths.

    Returns
    -------
    rvf : pyspark.sql.DataFrame

    """
    rvf = spark.read.parquet(RVF_PATH)
    return rvf


VENUES_PATH = "dbfs:/mnt/files/silver/venues/parquet_new/"


def load_venues():
    """Read and union files from multiple paths.

    Returns
    -------
    rvf : pyspark.sql.DataFrame

    """
    rvf = spark.read.parquet(VENUES_PATH)
    return rvf


ZIPS_PATH = "dbfs:/mnt/files/silver/zips"


def load_zips():
    """Read and union files from multiple paths.

    Returns
    -------
    rvf : pyspark.sql.DataFrame

    """
    zips = spark.read.parquet(ZIPS_PATH)
    return zips
