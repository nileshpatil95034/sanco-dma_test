from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct


class PostIngestionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket.from_bucket_name(
            self,
            "InsuranceBucket",
            "insuranceprojectnew"
        )

        lambda_function = _lambda.Function(
            self,
            "PostIngestionLambda",
            function_name="post_ingestion_lambda",
            runtime=_lambda.Runtime.PYTHON_3_13,      # See note below
            handler="aws_lambda.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            environment={
                "BUCKET_NAME": "insuranceprojectnew"
            }
        )

        s3deploy.BucketDeployment(
            self,
            "DeployShieldJson",
            destination_bucket=bucket,
            destination_key_prefix="raw",
            sources=[
                s3deploy.Source.asset("data")
            ]
        )