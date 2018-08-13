FROM mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.6

RUN apt-get update && \
    apt-get install -y \
            jq \
            python python-dev python-pip python-virtualenv && \
            rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/dist
ADD  python-server2.py .
RUN chmod 777 ./python-server2.py

CMD ["python","-u","./python-server2.py"]