import os

import boto3
from botocore.exceptions import ClientError


REGION = "eu-south-2"
BUCKET = "datos-grandes-eth-project"
ROLE_NAME = "AWSGlueServiceRole-ethimat3a03"
JOB_NAME = "glue_streaming_vwap_binance"
SCRIPT_LOCAL_PATH = "src/aws_glue/etl_jobs/glue_streaming_vwap_binance.py"
SCRIPT_S3_KEY = "glue/jobs/glue_streaming_vwap_binance.py"
SCRIPT_S3_URI = f"s3://{BUCKET}/{SCRIPT_S3_KEY}"
CHECKPOINT_LOCATION = f"s3://{BUCKET}/checkpoints/glue-streaming-vwap-binance-temp/"

BOOTSTRAP_SERVERS = "51.49.235.244:9092"
TOPIC_INPUT = "imat3a_ETH"
TOPIC_OUTPUT = "imat3a_ETH_VWAP"
KAFKA_USERNAME = "kafka_client"
KAFKA_PASSWORD = "88b8a35dca1a04da57dc5f3e"


def main() -> None:
    session = boto3.Session(region_name=REGION)
    s3 = session.client("s3")
    glue = session.client("glue")
    sts = session.client("sts")

    account_id = sts.get_caller_identity()["Account"]
    role_arn = f"arn:aws:iam::{account_id}:role/service-role/{ROLE_NAME}"

    s3.upload_file(SCRIPT_LOCAL_PATH, BUCKET, SCRIPT_S3_KEY)

    default_arguments = {
        "--job-language": "python",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-metrics": "true",
        "--BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
        "--TOPIC_INPUT": TOPIC_INPUT,
        "--TOPIC_OUTPUT": TOPIC_OUTPUT,
        "--CHECKPOINT_LOCATION": CHECKPOINT_LOCATION,
        "--KAFKA_USERNAME": KAFKA_USERNAME,
        "--KAFKA_PASSWORD": KAFKA_PASSWORD,
    }

    job_definition = {
        "Role": role_arn,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "Command": {
            "Name": "gluestreaming",
            "ScriptLocation": SCRIPT_S3_URI,
            "PythonVersion": "3",
        },
        "DefaultArguments": default_arguments,
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 60,
        "MaxRetries": 0,
        "Description": "Streaming Glue job que lee velas de Binance desde Kafka y publica el VWAP en imat3a_ETH_VWAP.",
    }

    created = False
    try:
        glue.create_job(Name=JOB_NAME, **job_definition)
        created = True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code != "AlreadyExistsException":
            raise
        glue.update_job(JobName=JOB_NAME, JobUpdate=job_definition)

    run = glue.start_job_run(JobName=JOB_NAME)

    print(f"region={REGION}")
    print(f"job_name={JOB_NAME}")
    print(f"job_status={'created' if created else 'updated'}")
    print(f"script_s3_uri={SCRIPT_S3_URI}")
    print(f"checkpoint_location={CHECKPOINT_LOCATION}")
    print(f"job_run_id={run['JobRunId']}")


if __name__ == "__main__":
    required_env = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ]
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Faltan variables AWS temporales: {', '.join(missing)}")
    main()
