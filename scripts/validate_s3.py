from config.config import S3_BUCKET
from utils.s3_utils import folder_exists

folders = [
    "raw/",
    "processed/",
    "gold/",
    "scripts/",
    "logs/"
]

print(f"Validating S3 bucket: {S3_BUCKET}\n")

for folder in folders:
    if folder_exists(S3_BUCKET, folder):
        print(f"✅ {folder} exists")
    else:
        print(f"❌ {folder} not found")