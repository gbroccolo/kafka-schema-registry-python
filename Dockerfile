FROM python:3.8-slim

COPY ./src/requirements.txt /tmp/requirements.txt
COPY ./src/*.py /app/
COPY ./avro/messages.avro /avro/

RUN pip3 install -U -r /tmp/requirements.txt && rm -f /tmp/requirements.txt
