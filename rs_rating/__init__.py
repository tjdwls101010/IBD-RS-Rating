from importlib.metadata import PackageNotFoundError, version

from .client import RS

try:
    __version__ = version("ibd-rs-rating")
except PackageNotFoundError:  # running from a source tree with no installed dist
    __version__ = "0.0.0+unknown"

__all__ = ["RS", "__version__"]
