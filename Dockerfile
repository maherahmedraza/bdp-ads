FROM ubuntu:latest
LABEL authors="maher"

ENTRYPOINT ["top", "-b"]