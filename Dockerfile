# Dockerfile for replay demo

FROM quay.io/signalfuse/maestro-base:15.04-0.2.6
MAINTAINER Wentao Du <wentao@signalfuse.com>

# Install signalfx python library
RUN pip install signalfx

# Copy tsdata and metadata into docker
ADD replay-data.tar.gz /opt/

# Copy python script into docker
ADD src/docker_run.py /opt/
ADD src/publish_data.py src/util.py src/__init__.py /opt/src/

CMD ["python", "/opt/docker_run.py"]
