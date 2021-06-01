"""Utility functions for GeoSpark aka Apache Sedona"""

from sedona.utils import KryoSerializer, SedonaKryoRegistrator

from .common import spark

config_opts = {
    "spark.serializer": KryoSerializer.getName,
    "spark.kryo.registrator": SedonaKryoRegistrator.getName,
    "spark.jars.packages": "org.apache.sedona:"
    "sedona-python-adapter-3.0_2.12:1.0.0-incubating,"
    "org.datasyslab:"
    "geotools-wrapper:geotools-24.0",
}

builder = spark.builder
for prop, val in config_opts.items():
    builder = builder.config(prop, val)
spark = builder.getOrCreate()
