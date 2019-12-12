import json
import datetime

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()

######################## Utils ########################


def to_iso_string(x):
    try:
        return parser.parse(x).isoformat()
    except:
        return None


to_iso_string_udf = F.udf(to_iso_string)


def to_date_robust(x):
    '''converts a string column to date column with best effort'''
    return F.to_date(to_iso_string_udf(x))


def escape_df_name(name):
    return name if '.' not in name else f'`{name}`'


class DateEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.date):
            return o.isoformat()
        return super().default(o)

######################## Main ########################


def profile_datatype(dataset, name, dataType=None):
    if dataType is None:
        dataType = dataset.schema[name].dataType
    if isinstance(dataType, T.IntegerType) or isinstance(dataType, T.LongType):
        select = dataset.select(
            F.count(name),
            F.min(name),
            F.max(name),
            F.mean(name),
            F.stddev(name),
        ).collect()[0]
        ret = {
            "type": "INTEGER (LONG)",
            "count": select[0],
            "max_value": select[1],
            "min_value": select[2],
            "mean": select[3],
            "stddev": select[4],
        }
    elif isinstance(dataType, T.DoubleType) or isinstance(dataType, T.FloatType):
        select = dataset.select(
            F.count(name),
            F.min(name),
            F.max(name),
            F.mean(name),
            F.stddev(name),
        ).collect()[0]
        ret = {
            "type": "REAL",
            "count": select[0],
            "max_value": select[1],
            "min_value": select[2],
            "mean": select[3],
            "stddev": select[4],
        }
    elif isinstance(dataType, T.BooleanType):
        select = dataset.select(
            F.count(name),
            F.count(F.when(dataset[name], 1)),
        ).collect()[0]
        ret = {
            "type": "BOOLEAN",
            "count": select[0],
            "max_value": True,
            "min_value": False,
            "mean": select[1] / select[0],
            "stddev": (select[1] / select[0] - (select[1] / select[0]) ** 2)**(.5),
        }
    elif isinstance(dataType, T.DateType) or isinstance(dataType, T.TimestampType):
        select = dataset.select(
            F.count(name),
            F.min(name),
            F.max(name),
        ).collect()[0]
        ret = {
            "type": "DATE/TIME",
            "count": select[0],
            "max_value": select[1],
            "min_value": select[2],
        }
    elif isinstance(dataType, T.StringType):
        data_str_length = dataset.select(dataset[name], F.length(dataset[name]).alias('_len'))
        ret = {
            "type": "TEXT",
            "count": dataset.select(dataset[name]).count(),
            "shortest_values": [x for [x] in (data_str_length.orderBy(F.asc('_len')).select(name).take(5))],
            "longest_values": [x for [x] in (data_str_length.orderBy(F.desc('_len')).select(name).take(5))],
            "average_length": data_str_length.select(F.mean(data_str_length['_len'])).collect()[0][0],
        }
    else:
        raise NotImplementedError
    return ret


# 1. list all filenames and titles
datasets = (spark.read.format('csv')
            .options(inferschema='true', sep='\t')
            .load('/user/hm74/NYCOpenData/datasets.tsv')
            .toDF('filename', 'title'))

# 2. for each dataset
for filename, title in datasets.toLocalIterator():
    # filename, title = next(datasets.toLocalIterator())
    print(u'>> entering {}.tsv.gz: {}'.format(filename, title))
    try:
        with open('task1.{}.json'.format(filename)) as f:
            json.load(f)
            print(u'>> skipping {}.tsv.gz: {}'.format(filename, title))
            continue
    except:
        pass

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(header='true', inferschema='true', sep='\t')
               .load('/user/hm74/NYCOpenData/{}.tsv.gz'.format(filename)))

    # 2.2 count dataset rows
    dataset_count = dataset.count()

    # 2.3 create dataset profile
    output = {'dataset_name': filename, 'columns': [], 'key_column_candidates': []}

    # 2.4 batch compute simple column profiles
    # 2.4.1 batch select at once
    batch_select = dataset.select([
        item for name in dataset.columns for item in (
            F.count(F.when(~F.isnull(escape_df_name(name)), True)),
            F.count(F.when(F.isnull(escape_df_name(name)), True)),
            F.countDistinct(escape_df_name(name)),
        )
    ]).collect()[0]
    # 2.4.2 group result by chunk of size 3
    batch_select_chunked = (batch_select[i:i+3] for i in range(0, len(batch_select), 3))

    # 2.5 create column profiles
    for column, select in zip(dataset.schema, batch_select_chunked):
        # column, select = next(zip(dataset.schema, batch_select_chunked))
        name = escape_df_name(column.name)
        dataType = column.dataType

        # 2.5.1 use batch select
        column_output = {
            'column_name': name,
            'number_non_empty_cells': select[0],
            'number_empty_cells': select[1],
            'number_distinct_values': select[2],
            'frequent_values': None,
            'data_types': [],
            'semantic_types': [],
        }
        assert column_output['number_non_empty_cells'] + column_output['number_empty_cells'] == dataset_count
        assert column_output['number_distinct_values'] <= dataset_count

        # 2.5.2 fill frequent_values
        column_output['frequent_values'] = [x for [x] in (dataset.groupBy(name).agg(F.count('*').alias('_count'))
                                                          .orderBy(F.desc('_count'))
                                                          .select(name)
                                                          .take(5))]

        # 2.5.3 default datatype => fill data_types
        column_output['data_types'].append(profile_datatype(dataset, name, dataType))

        # 2.5.4 datatype indefinite => try others
        if isinstance(dataType, T.StringType):
            cast_dataset = dataset.select(
                dataset[name].cast(T.LongType()).alias('_integer'),
                dataset[name].cast(T.DoubleType()).alias('_double'),
                to_date_robust(dataset[name]).alias('_date'),
            )
            cast_select = cast_dataset.select([
                F.count(F.when(~F.isnull('_integer'), 1)),
                F.count(F.when(F.isnull('_integer') & ~F.isnull('_double'), 1)),
                F.count(F.when(F.isnull('_double') & ~F.isnull('_date'), 1)),
            ]).collect()[0]

            if cast_select[0]:
                column_output['data_types'].append(profile_datatype(cast_dataset, '_integer'))
            if cast_select[1]:
                column_output['data_types'].append(profile_datatype(cast_dataset, '_double'))
            if cast_select[2] > 0.6 * dataset.count():
                column_output['data_types'].append(profile_datatype(cast_dataset, '_date'))

        # 2.5.5 all distinct => key candidate
        if column_output['number_distinct_values'] == dataset.count():
            output['key_column_candidates'].append(name)

        # 2.5.6 add column to output
        output['columns'].append(column_output)

    # 2.6 dump dataset profile as json
    with open('task1.{}.json'.format(filename), 'w') as f:
        json.dump(output, f, indent=2, cls=DateEncoder)
