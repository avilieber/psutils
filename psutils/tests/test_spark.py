import pytest


@pytest.fixture
def geo_config_opts():
    from sedona.utils import KryoSerializer, SedonaKryoRegistrator

    return {
        "spark.serializer": KryoSerializer.getName,
        "spark.kryo.registrator": SedonaKryoRegistrator.getName,
        "spark.jars.packages": "org.apache.sedona:"
        "sedona-python-adapter-3.0_2.12:1.0.0-incubating,"
        "org.datasyslab:"
        "geotools-wrapper:geotools-24.0",
    }


def test_spark(geo_config_opts):

    from psutils import spark

    common_config = dict(spark.sparkContext.getConf().getAll())
    assert all(conf not in common_config for conf in geo_config_opts)

    from psutils.geo import spark

    geo_config = dict(spark.sparkContext.getConf().getAll())
    common_config.update(geo_config_opts)
    assert all(geo_config[k] == v for k, v in common_config.items())
