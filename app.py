#!/usr/bin/env python3

import aws_cdk as cdk
from infrastructure.post_ingestion_stack import PostIngestionStack

app = cdk.App()

PostIngestionStack(app, "PostIngestionStack")

app.synth()