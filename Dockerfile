FROM python:3.9

WORKDIR /code
ENV PYTHONPATH=/code
# ENV ETHERSCAN_API_KEY=

# Install Google Cloud SDK
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . /code

# CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "80"]
