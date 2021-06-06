ARG BASE_CONTAINER=jupyter/minimal-notebook
ARG BASE_TAG=python-3.8.8
FROM $BASE_CONTAINER:$BASE_TAG

COPY requirements.txt /opt/app/

RUN pip install -r /opt/app/requirements.txt

WORKDIR /opt/app/