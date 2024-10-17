from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_iam as iam,
    aws_logs as logs,
    Duration,
    BundlingOptions,
    CfnOutput,
)
import os


class BackendStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, stage_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get account and region from the CDK context
        aws_account_id = self.account
        aws_region = self.region

        # Define the Lambda function with bundling
        backend_lambda = _lambda.Function(
            self,
            "BackendLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="api.handler",
            code=_lambda.Code.from_asset(
                "../src/android_genymotion_backend",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=["bash", "-c", "pip install -r requirements.txt -t /asset-output && cp -r . /asset-output"],
                    platform="linux/arm64",
                ),
            ),
            environment={
                "STAGE": stage_name,
                "AWS_ACCOUNT_ID": aws_account_id,
            },
            timeout=Duration.seconds(30),
            memory_size=512,
            architecture=_lambda.Architecture.ARM_64,
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

        # Create an API Gateway REST API with API key required
        api = apigw.RestApi(
            self,
            "BackendAPI",
            rest_api_name=f"AndroidGenymotionBackend-{stage_name.capitalize()}",
            deploy_options=apigw.StageOptions(
                stage_name=stage_name,
                logging_level=apigw.MethodLoggingLevel.INFO,
                data_trace_enabled=True,
                access_log_destination=apigw.LogGroupLogDestination(logs.LogGroup(self, "ApiGatewayAccessLogs")),
            ),
            default_method_options=apigw.MethodOptions(api_key_required=True),
        )

        # Integrate Lambda with API Gateway
        integration = apigw.LambdaIntegration(backend_lambda)

        # Add a proxy resource to forward all requests to the Lambda function
        api.root.add_proxy(
            default_integration=integration,
            any_method=True,
        )

        # Create a usage plan and API key
        plan = api.add_usage_plan(
            "UsagePlan",
            name="EasyPlan",
            throttle=apigw.ThrottleSettings(
                rate_limit=1000,
                burst_limit=200,
            ),
        )

        key = api.add_api_key("ApiKey")

        plan.add_api_key(key)
        plan.add_api_stage(
            stage=api.deployment_stage,
            throttle=[
                apigw.ThrottlingPerMethod(
                    method=method,
                    throttle=apigw.ThrottleSettings(
                        rate_limit=1000,
                        burst_limit=200,
                    ),
                )
                for method in api.methods
            ],
        )

        CfnOutput(
            self,
            "APIKeyValue",
            value=key.key_id,
            description="The API Key for accessing the API",
        )