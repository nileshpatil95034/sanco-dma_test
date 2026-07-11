import json
import boto3

# Create S3 client
s3 = boto3.client('s3')


def lambda_handler(event, context):
    # Replace with your S3 bucket name
    bucket_name = os.environ["BUCKET_NAME"]
    file_name = "shield.json"

    try:
        # Read file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_name)

        # Read file content
        file_content = response['Body'].read().decode('utf-8')

        # Convert JSON string to Python dictionary
        data = json.loads(file_content)

        # Print the JSON data
        print("File Data:")
        print(json.dumps(data, indent=4))

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "File read successfully",
                "data": data
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")

        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }