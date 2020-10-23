import numpy 
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType ,MapType, StringType, DoubleType
from pyspark.ml.linalg import Vectors, VectorUDT,DenseVector


@udf(ArrayType(IntegerType()))
def vector_indices(value):
    if isinstance(value,numpy.ndarray):
        return [i for i, v in enumerate(list(value)) if v != 0]
    if isinstance(value,DenseVector):
        value = value.tolist()
        return [i for i, v in enumerate(list(value)) if v != 0]
    return None


@udf(VectorUDT())
def to_sparse(dense_vector):
    size = len(dense_vector)
    pairs = [(i, v) for i, v in enumerate(dense_vector.values.tolist()) if v != 0]
    return Vectors.sparse(size, pairs)



@udf(MapType(StringType(), StringType()))
def to_ext_dict(ext_key, ext_val):
    return {k: v for k, v in zip(ext_key.split(','), ext_val.split(','))}


@udf(MapType(StringType(), StringType()))
def to_ctr_dict(ext_key, ext_val):
    return {k: v for k, v in zip(eval(ext_key), eval(ext_val)) if k[-3:] in ['clk', 'imp', 'ctr']}



@udf()
def to_string(v):
    return str(v)


@udf()
def weekday(dt):
    return f'{dt:%A}'.lower()

@udf(returnType=DoubleType())
def int_default_zero(values):
    if values:
        return values
    return 0.0


@udf()
def is_weekend(dt):
    return 'yes' if dt.weekday() >= 5 else 'no'

