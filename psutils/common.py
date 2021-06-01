"""Home of SparkSession"""

import re
from pathlib import PosixPath

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Databricks Shell").getOrCreate()


class DbfsPath(PosixPath):
    @classmethod
    def from_dbfs(cls, path):
        std_path = re.compile(r"^dbfs\:").sub("/dbfs", path)
        return cls(std_path)

    def to_dbfs(self):
        return re.compile(r"^\/dbfs").sub("dbfs:", str(self))
