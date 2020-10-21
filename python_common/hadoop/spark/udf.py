import numpy 
from pyspark.sql import functions
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.ml.linalg import Vectors, VectorUDT,DenseVector


@functions.udf(ArrayType(IntegerType()))
def vector_indices(value):
    if isinstance(value,numpy.ndarray):
        return [i for i, v in enumerate(list(value)) if v != 0]
    if isinstance(value,DenseVector):
        value = value.tolist()
        return [i for i, v in enumerate(list(value)) if v != 0]
    return None


@functions.udf(VectorUDT())
def to_sparse(dense_vector):
    size = len(dense_vector)
    pairs = [(i, v) for i, v in enumerate(dense_vector.values.tolist()) if v != 0]
    return Vectors.sparse(size, pairs)
