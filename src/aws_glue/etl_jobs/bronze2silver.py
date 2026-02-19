```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="trade_data_eth_imat3a03",
    table_name="eth_datos_grandes_eth_project"
)

dynamic_frame = DynamicFrame.fromDF(
    dynamic_frame.toDF().drop("partition_0"),
    glueContext,
    "dynamic_frame"
)

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://datos-grandes-eth-project/silver/"},
    format="parquet"
)

job.commit()
```
