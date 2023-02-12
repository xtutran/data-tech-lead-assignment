FROM python:3.10-slim AS python3
FROM openjdk:8-slim

COPY --from=python3 / /

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

ADD ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY . ./

CMD ["python", "main.py", "--mysql-host", "172.17.0.1"]
