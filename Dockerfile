FROM python:3.7-alpine3.7
RUN mkdir /app
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev
RUN python -m pip install --upgrade pip
RUN pip install -r /app/requirements.txt
CMD python pgImport.py
