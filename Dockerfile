FROM bitnami/spark:3.5.1


# Optional: custom JDBC
RUN curl https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -o /opt/bitnami/spark/jars/postgresql-42.7.4.jar

# Since the cluster will deserialize your app and run it, the cluster need similar depenecies.
# ie. if your app uses numpy
#RUN pip install numpy

RUN python3 -m venv .venv
RUN source .venv/bin/activate
RUN pip install pyspark
RUN pip install requests