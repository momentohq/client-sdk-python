FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.8

WORKDIR /var/task

# Copy the lambda and the requirements file
COPY lambda/index.py .
COPY lambda/aws_requirements.txt .

# Install Python dependencies
RUN pip install -r aws_requirements.txt -t .

# Set the CMD to your lambda (could also be done as a parameter override outside of the Dockerfile)
CMD ["index.handler"]

