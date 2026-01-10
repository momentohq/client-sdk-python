<head>
  <meta name="Momento Python Client Library Documentation" content="Python client software development kit for Momento Cache">
</head>
<img src="https://docs.momentohq.com/img/logo.svg" alt="logo" width="400"/>

[![project status](https://momentohq.github.io/standards-and-practices/badges/project-status-official.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)
[![project stability](https://momentohq.github.io/standards-and-practices/badges/project-stability-stable.svg)](https://github.com/momentohq/standards-and-practices/blob/main/docs/momento-on-github.md)

<br>

## Example Lambda

This directory contains an example lambda, built using AWS CDK, that performs a basic set and get operation on Momento cache.

The primary use is to provide a base for testing Momento in an AWS lambda environment. The lambda creates a Momento client, and then calls a set and get on a hard-coded key,value.

## Prerequisites

- Node version 14 or higher is required (for deploying the Cloudformation stack containing the Lambda)
- A Momento API key is required, you can generate one using the [Momento Console](https://console.gomomento.com/api-keys)
- A Momento service endpoint is required. You can find a [list of them here](https://docs.momentohq.com/platform/regions)

## Deploying the Momento Python Lambda with Docker and AWS CDK

The source code for the CDK application lives in the `infrastructure` directory.
To build and deploy it you will first need to install the dependencies:

```bash
cd infrastructure
npm install
```

To deploy the CDK app you will need to have [configured your AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html#cli-chap-authentication-precedence).

Then run:

```
export MOMENTO_API_KEY=<your-api-key>
export MOMENTO_ENDPOINT=<your-endpoint>
npm run cdk deploy
```

The lambda does not set up a way to access itself externally, so to run it, you will have to go to `MomentoDockerLambda` in AWS Lambda and run a test.

The lambda is set up to make set and get calls for the key 'key' in the cache 'cache'. You can play around with the code by changing the `docker/lambda/index.py` file. Remember to update `docker/lambda/aws_requirements.txt` file if you add additional Python dependencies.

## Deploying the Momento Python Lambda as a Zip File on AWS Lambda with the AWS Management Console

Alternatively, we can deploy the Momento Python Lambda as a Zip File on AWS Lambda. We can do this using the `zip` directory in this example.

Follow these steps to create the zip and deploy it to AWS Lambda using the AWS Management Console:

1. Run `make dist` in the `zip` directory to package the lambda for upload as `dist.zip`.

> :bulb: **Tip**: Check out the Makefile for important build steps to package for AWS Lambda.

2. Create a new Lambda function by selecting "Author from scratch".
3. Set the function name to `momento-lambda-demo`.
4. Choose the runtime as `Python 3.8` (you can adjust this as desired).
5. Select the architecture as `x86_64`.
6. Click on "Create function" to create the Lambda function.
7. In the "Code" tab, choose "Upload from the zip" as the code source.
8. Under "Runtime settings", set the Handler to index.handler.
9. Switch to the "Configuration" tab.
10. Set the environment variable `MOMENTO_API_KEY` to your API key.
11. Set the environment variable `MOMENTO_ENDPOINT` to your chosen endpoint.
12. Finally, go to the "Test" tab to test your Lambda function.
