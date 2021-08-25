'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-08-24
@Last Modified by: Amar Pawar
@Last Modified time: 2021-08-24
@Title : Trending Hashtag count with pyflink
/**********************************************************************************
'''

from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import lit

settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(settings)

t_env.get_config().get_configuration().set_string("parallelism.default", "1")
t_env.connect(FileSystem().path('/home/ubuntu/Documents/hadoop_practice/flink_demo/input')) \
    .with_format(OldCsv()
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .create_temporary_table('source')

t_env.connect(FileSystem().path('/home/ubuntu/Documents/hadoop_practice/flink_demo/output')) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .create_temporary_table('sink')

tab = t_env.from_path('source')
tab.group_by(tab.word) \
   .select(tab.word, lit(1).count) \
   .execute_insert('sink').wait()