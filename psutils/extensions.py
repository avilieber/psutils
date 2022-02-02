import functools
import warnings
from abc import ABC, abstractmethod

from pandas.core.accessor import CachedAccessor
from pyspark.sql import DataFrame


class AccessorRegister:
    def __init__(self, cls):
        self._cls = cls

    def __call__(self, name):
        # from pyspark.sql import DataFrame
        cls = self._cls

        def decorator(accessor):
            if hasattr(cls, name):
                warnings.warn(
                    f"registration of accessor {repr(accessor)} under name "
                    f"{repr(name)} for type {repr(cls)} is overriding a preexisting "
                    f"attribute with the same name.",
                    UserWarning,
                    stacklevel=2,
                )
            setattr(cls, name, CachedAccessor(name, accessor))
            return accessor

        return decorator


register_dataframe_accessor = AccessorRegister(DataFrame)


class _DataFrameMethod(ABC):
    def __init__(self, frame):
        if not isinstance(frame, DataFrame):
            raise ValueError("PySpark DataFrames only.")
        self._frame = frame

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


@register_dataframe_accessor("withColumns")
class WithColumns(_DataFrameMethod):
    def __call__(self, **columns):

        return functools.reduce(
            lambda df, tup: df.withColumn(*tup), columns.items(), self._frame
        )
    

@register_dataframe_accessor('cached')
class Cached(_DataFrameMethod):
    def __call__(self):
        return self._frame
        
    def _cache(self):
        self._frame.persist()
        
    def _uncache(self):
        self._frame.unpersist()
        
    def __enter__(self):
        self._cache()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._uncache()
