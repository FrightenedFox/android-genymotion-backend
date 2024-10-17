from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_iam as iam,
    aws_logs as logs,
)
import os

class BackendStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, stage_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define the Lambda function
        backend_lambda = _lambda.Function(
            self, "BackendLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="api.lambda_handler",
            code=_lambda.Code.from_asset("../src/android_genymotion_backend"),
            environment={
                "STAGE": stage_name,
                "AWS_ACCOUNT_ID": os.environ["CDK_DEFAULT_ACCOUNT"],
                "AWS_REGION": os.environ["CDK_DEFAULT_REGION"],
            },
            timeout=cdk.Duration.seconds(30),
            memory_size=512,
        )

        # Add necessary IAM permissions
        backend_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:*",
                    "ec2:*",
                ],
                resources=["*"],
            )
        )

        # Create an API Gateway REST API
        api = apigw.LambdaRestApi(
            self, "BackendAPI",
            handler=backend_lambda,
            proxy=True,
            deploy_options=apigw.StageOptions(
                stage_name=stage_name,
                logging_level=apigw.MethodLoggingLevel.INFO,
                data_trace_enabled=True,
                access_log_destination=apigw.LogGroupLogDestination(
                    logs.LogGroup(self, "ApiGatewayAccessLogs")
                ),
            ),
        )
