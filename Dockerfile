FROM apache/spark:3.5.3-python3
USER root
RUN python3 -m pip install --no-cache-dir notebook==7.3.2
RUN python3 -c "from urllib.request import urlretrieve; root='https://repo.maven.apache.org/maven2/'; libs=['org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar','com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar']; [urlretrieve(root+p, '/opt/spark/jars/'+p.rsplit('/',1)[1]) for p in libs]"
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
WORKDIR /project
