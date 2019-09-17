import sys;
from pyspark.sql.types import StringType;
from pyspark.serializers import CloudPickleSerializer;
f = open(sys.argv[1], 'wb');

from pyspark.files import SparkFiles
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DataType
from pyspark.sql.types import DoubleType, IntegerType, FloatType, LongType, StringType
from pyspark.sql.types import _parse_datatype_json_string

return_type = _parse_datatype_json_string(sys.argv[2])
print("function return type: " + str(return_type))
archive_path = sys.argv[3]

class ModelCache(object):
    _models = {}

    def __init__(self):
        pass

    @staticmethod
    def get_or_load(archive_path):
        if archive_path in ModelCache._models:
            return ModelCache._models[archive_path]
        from mlflow.pyfunc import load_pyfunc
        ModelCache._models[archive_path] = load_pyfunc(archive_path)
        return ModelCache._models[archive_path]

def predict(*args):
    import pandas
    from mlflow.pyfunc.spark_model_cache import SparkModelCache
    from mlflow.pyfunc import load_pyfunc  # pylint: disable=cyclic-import
    # elem_type = IntegerType
    elem_type = return_type

    if isinstance(elem_type, ArrayType):
        elem_type = elem_type.elementType

    supported_types = [IntegerType, LongType, FloatType, DoubleType, StringType]

    if not any([isinstance(elem_type, x) for x in supported_types]):
        raise MlflowException(
            message="Invalid result_type '{}'. Result type can only be one of or an array of one "
                    "of the following types types: {}".format(str(elem_type), str(supported_types)),
            error_code=INVALID_PARAMETER_VALUE)
    # model = SparkModelCache.get_or_load(archive_path)
    # model = load_pyfunc(archive_path)
    model = ModelCache.get_or_load(archive_path)
    schema = {str(i): arg for i, arg in enumerate(args)}
    # Explicitly pass order of columns to avoid lexicographic ordering (i.e., 10 < 2)
    columns = [str(i) for i, _ in enumerate(args)]
    pdf = pandas.DataFrame(schema, columns=columns)
    # model.predict(pdf)
    result = model.predict(pdf)
    if not isinstance(result, pandas.DataFrame):
        result = pandas.DataFrame(data=result)

    elif type(elem_type) == IntegerType:
        result = result.select_dtypes([np.byte, np.ubyte, np.short, np.ushort,
                                       np.int32]).astype(np.int32)

    elif type(elem_type) == LongType:
        result = result.select_dtypes([np.byte, np.ubyte, np.short, np.ushort, np.int, np.long])

    elif type(elem_type) == FloatType:
        result = result.select_dtypes(include=(np.number,)).astype(np.float32)

    elif type(elem_type) == DoubleType:
        result = result.select_dtypes(include=(np.number,)).astype(np.float64)

    if len(result.columns) == 0:
        raise MlflowException(
            message="The the model did not produce any values compatible with the requested "
                    "type '{}'. Consider requesting udf with StringType or "
                    "Arraytype(StringType).".format(str(elem_type)),
            error_code=INVALID_PARAMETER_VALUE)

    if type(elem_type) == StringType:
        result = result.applymap(str)

    if type(return_type) == ArrayType:
        return pandas.Series([row[1].values for row in result.iterrows()])
    else:
        return result[result.columns[0]]

f.write(CloudPickleSerializer().dumps((predict, return_type)));
