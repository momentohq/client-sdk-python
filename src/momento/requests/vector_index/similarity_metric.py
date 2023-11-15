from enum import Enum


class SimilarityMetric(Enum):
    """The similarity metric to use when comparing vectors in the index."""

    EUCLIDEAN_SIMILARITY = "EUCLIDEAN_SIMILARITY"
    """The Euclidean distance squared between two vectors, ie the sum of squared differences between each element.
    Smaller is better. Ranges from 0 to infinity."""
    INNER_PRODUCT = "INNER_PRODUCT"
    """The inner product between two vectors, ie the sum of the element-wise products.
    Bigger is better. Ranges from 0 to infinity."""
    COSINE_SIMILARITY = "COSINE_SIMILARITY"
    """The cosine similarity between two vectors, ie the cosine of the angle between them.
    Bigger is better. Ranges from -1 to 1."""
