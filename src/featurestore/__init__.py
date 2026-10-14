"""featurestore: point-in-time-correct features for training and low-latency serving."""

from featurestore.schema import FeatureType, FeatureView
from featurestore.store import FeatureStore

__all__ = ["FeatureStore", "FeatureType", "FeatureView"]
__version__ = "0.1.0"
