FROM apache/airflow:2.7.0

ARG SPARK_VERSION="3.4.1"
ARG HADOOP_VERSION="3"
ARG HADOOP_VERSION_EXPLICIT="3.3.4"

USER root

# Install OpenJDK-19
RUN apt-get update \
    && apt-get install jing -y \
    && apt-get install wget -y \
    && apt-get install nano -y \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


###############################
## SPARK files and variables
###############################
ENV SPARK_HOME /usr/local/spark

RUN echo "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
        wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin
###############################
## Finish SPARK files and variables
###############################


# Setup Hadopp install and env vars
#RUN echo "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION_EXPLICIT}/hadoop-${HADOOP_VERSION_EXPLICIT}.tar.gz"
#RUN cd "/tmp" && \
#        wget --no-verbose "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION_EXPLICIT}/hadoop-${HADOOP_VERSION_EXPLICIT}.tar.gz" && \
#        tar -xvzf "hadoop-${HADOOP_VERSION_EXPLICIT}.tar.gz" && \
#        mkdir -p "/usr/local/hadoop" && \
#        cp -a "hadoop-${HADOOP_VERSION_EXPLICIT}/." "/usr/local/hadoop/" && \
#        rm "hadoop-${HADOOP_VERSION_EXPLICIT}.tar.gz"
#
## Create HADOOP_HOME env var
#ENV HADOOP_HOME /usr/local/hadoop
## set HADOOP_CONF_DIR
#ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop

# Create symbolic link for Hadoop configuration
RUN mkdir -p $SPARK_HOME/conf
ENV HADOOP_CONF_DIR $SPARK_HOME/conf/yarn-conf
#RUN ln -s $HADOOP_CONF_DIR $SPARK_HOME/conf/yarn-conf

USER airflow

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

# Install Python packages
COPY ./requirements.txt /
RUN pip install -r /requirements.txt