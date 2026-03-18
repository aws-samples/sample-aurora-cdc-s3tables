import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as path from 'path';

export class LambdaTransformStack extends cdk.Stack {
  public readonly functionArn: string;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const role = new iam.Role(this, 'Role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')],
    });

    const fn = new lambda.Function(this, 'Function', {
      functionName: 'firehose-debezium-transform',
      runtime: lambda.Runtime.PYTHON_3_14,
      handler: 'firehose-debezium-transform.lambda_handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../../lambda')),
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      role,
    });

    this.functionArn = fn.functionArn;
    new cdk.CfnOutput(this, 'FunctionArn', { value: fn.functionArn });
  }
}
