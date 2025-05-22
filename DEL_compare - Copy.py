import pandas as pd
import boto3
from io import StringIO

# Replace these values with your own
aws_access_key_id = 'XXXXXXXadfartr'
aws_secret_access_key = 'XXXXXaffeaed'
bucket_name = 'cscafe-sit6-qc-sasffaef'
key1 = 'path.csv'
key2 = 'path.csv'

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

# Download files into DataFrames
def s3_csv_to_df(s3_client, bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

df1 = s3_csv_to_df(s3_client, bucket_name, key1)
df2 = s3_csv_to_df(s3_client, bucket_name, key2)

# Add a source column and set index for comparison
df1['_source'] = 'df1'
df2['_source'] = 'df2'

# Use all columns except '_source' as join keys
compare_cols = [col for col in df1.columns if col != '_source']

# Concatenate and drop duplicates that appear in both
combined = pd.concat([df1, df2], ignore_index=True)

# Mark duplicates on all columns except '_source'
duplicates = combined.duplicated(subset=compare_cols, keep=False)
diff_df = combined[~duplicates].copy()

# Mark where the row comes from
def get_presence(row):
    if row['_source'] == 'df1':
        found = df2[compare_cols].eq(row[compare_cols].values).all(axis=1).any()
        return "Only in df1" if not found else "Different in both"
    else:
        found = df1[compare_cols].eq(row[compare_cols].values).all(axis=1).any()
        return "Only in df2" if not found else "Different in both"

diff_df['Difference'] = diff_df.apply(get_presence, axis=1)
diff_df = diff_df.drop(columns=['_source'])

# Output: diff_df contains the differing rows with a "Difference" column
print(diff_df)
diff_df.to_excel('C:\\Automation_SIT5\\Pfizer_QC_Automation\\Target\\Del_Recon_output.xlsx', index=False)