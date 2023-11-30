FROM python:3.11
WORKDIR .
ENV APP_DATE = "$APP_DATE"
ENV APP_ENV = "$APP_ENV"
COPY src /src
COPY requirements.txt .
RUN pip install -r requirements.txt

