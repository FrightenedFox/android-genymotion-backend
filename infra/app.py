#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.backend_stack import BackendStack

app = cdk.App()

stage = app.node.try_get_context("stage")

if not stage:
    raise ValueError("Please specify a stage using '-c stage=staging' or '-c stage=production'")

env = cdk.Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"])

BackendStack(app, f"AndroidGenymotionBackend-{stage.capitalize()}", stage_name=stage, env=env)

app.synth()
