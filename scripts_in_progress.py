import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('doh-inspection-storage')
f = bucket.objects.filter()
latest = [obj.key for obj in sorted(f, key=lambda x: x.last_modified, reverse=True)][0]
print(latest)
