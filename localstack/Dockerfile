FROM localstack/localstack:latest
USER root

RUN sed -i \
    -e 's|default_version="7.1"|default_version="8.1"|g' \
    -e 's|return \["7.1"\]|return ["8.1"]|g' \
    /opt/code/localstack/localstack-core/localstack/packages/ffmpeg.py