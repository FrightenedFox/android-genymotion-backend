#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.backend_stack import BackendStack

app = cdk.App()

stage = app.node.try_get_context("stage")

if not stage:
    raise ValueError("Please specify a stage using '-c stage=staging' or '-c stage=production'")

# Get account and region from environment variables or default to placeholders
account = os.environ.get("CDK_DEFAULT_ACCOUNT", os.environ.get("AWS_ACCOUNT_ID", "YOUR_ACCOUNT_ID"))
region = os.environ.get("CDK_DEFAULT_REGION", os.environ.get("AWS_DEFAULT_REGION", "YOUR_REGION"))

env = cdk.Environment(account=account, region=region)

BackendStack(app, f"AndroidGenymotionBackend-{stage.capitalize()}", stage_name=stage, env=env)

app.synth()
