from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(2)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

t_env.connect(FileSystem().path('input')) \
    .with_format(OldCsv()
                 .line_delimiter(' ')
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .register_table_source("inputSource")

t_env.connect(FileSystem().path('output')) \
    .with_format(OldCsv().field_delimiter(',').field('word', DataTypes.STRING()).field('count', DataTypes.BIGINT()))\
    .with_schema(Schema().field('word', DataTypes.STRING()).field('count', DataTypes.BIGINT()))\
    .register_table_sink('sink')

t_env.scan('inputSource').group_by('word').select('word, count(1)').insert_into('sink')

t_env.execute('my first job')
