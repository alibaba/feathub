FROM flink:1.15.2


# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
    wget https://www.python.org/ftp/python/3.7.13/Python-3.7.13.tgz && \
    tar -xvf Python-3.7.13.tgz && \
    cd Python-3.7.13 && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-3.7.13.tgz && rm -rf Python-3.7.13 && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install feathub
COPY ./wheels /tmp/wheels
RUN pip3 install /tmp/wheels/* && \
    rm -rf /tmp/wheels && \
    cp $(python -c "from feathub.processors.flink.job_submitter import get_application_job_entry_point; print(get_application_job_entry_point())") /opt/flink

# Prepare flink
WORKDIR ${FLINK_HOME}
COPY ./libs/* ${FLINK_HOME}/lib
RUN mkdir plugins/oss-fs-hadoop && \
    mkdir plugins/s3-fs-hadoop && \
    ln -s ../../opt/$(ls opt | grep oss-fs-hadoop) plugins/oss-fs-hadoop/$(ls opt | grep oss-fs-hadoop) && \
    ln -s ../../opt/$(ls opt | grep s3-fs-hadoop) plugins/s3-fs-hadoop/$(ls opt | grep s3-fs-hadoop)

# Further customization can be added.
# You can refer to https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/#further-customization.
