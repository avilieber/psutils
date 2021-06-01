import functools
import warnings
from abc import ABC, abstractmethod

from pandas.core.accessor import CachedAccessor
from pandas.core.frame import DataFrame


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


def register_dataframe_accessor(name):
    # https://github.com/pandas-dev/pandas/blob/v1.2.4/pandas/core/accessor.py#L190
    from pyspark.sql import DataFrame

    def decorator(accessor):
        if hasattr(DataFrame, name):
            warnings.warn(
                f"registration of accessor {repr(accessor)} under name "
                f"{repr(name)} for type {repr(DataFrame)} is overriding a preexisting "
                f"attribute with the same name.",
                UserWarning,
                stacklevel=2,
            )
        setattr(DataFrame, name, CachedAccessor(name, accessor))
        return accessor

    return decorator


class _DataFrameMethod(ABC):
    def __init__(self, frame):
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
