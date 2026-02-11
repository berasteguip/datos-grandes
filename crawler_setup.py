import boto3

glue = boto3.client("glue", region_name="eu-south-2")
sts = boto3.client("sts")

account_id = sts.get_caller_identity()["Account"]
role_arn = f"arn:aws:iam::{account_id}:role/service-role/AWSGlueServiceRole-ethimat3a03"


response = glue.create_crawler(
    Name="crawler_eth_imat3a03",
    Role=role_arn,
    DatabaseName="trade_data_eth_imat3a03",
    Targets={
        "S3Targets": [
            {
                "Path": "s3://datos-grandes-eth-project/"
            }
        ]
    },
    Description="Crawler para datos ETH",
    TablePrefix="eth_",
    SchemaChangePolicy={
        "UpdateBehavior": "UPDATE_IN_DATABASE",
        "DeleteBehavior": "LOG"
    }
)

glue.start_crawler(Name="crawler_eth_imat3a03")