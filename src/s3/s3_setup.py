import boto3
from pathlib import Path

s3 = boto3.client("s3", region_name="eu-south-2")

bucket = "datos-grandes-eth-project"  # nombre global único

s3.create_bucket(
    Bucket=bucket,
    CreateBucketConfiguration={
        "LocationConstraint": "eu-south-2"
    }
)

print("Bucket creado")

base_path = Path("data")

for csv_file in base_path.glob("eth_data_*.csv"):
    year = csv_file.stem.split("_")[-1]   # 2022, 2023, ...
    
    s3_key = f"bronze/year={year}/{csv_file.name}"
    
    
    s3.upload_file(
        Filename=str(csv_file),
        Bucket=bucket,
        Key=s3_key
    )
    
    print(f"Subido {s3_key}")
