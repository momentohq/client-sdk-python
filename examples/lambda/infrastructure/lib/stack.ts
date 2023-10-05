import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';

export class MomentoLambdaStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        if (!process.env.MOMENTO_API_KEY) {
            throw new Error('The environment variable MOMENTO_API_KEY must be set.');
        }

        // Create Lambda function from Docker Image
        const dockerLambda = new lambda.DockerImageFunction(this, 'MomentoDockerLambda', {
            functionName: 'MomentoDockerLambda',
            code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, '../../docker')), // Point to the root since Dockerfile should be there
            environment: {
                MOMENTO_API_KEY: process.env.MOMENTO_API_KEY || ''
            },
            memorySize: 128,
            timeout: cdk.Duration.seconds(30)
        });
    }
}
