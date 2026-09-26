FROM apache/spark:4.1.3-scala2.13-java17-python3-r-ubuntu

USER root

# Gunakan python3 -m pip agar menginstall ke environment python3 yang aktif
RUN python3 -m pip install --no-cache-dir pyarrow pandas
RUN python3 -m pip install --no-cache-dir spacy
RUN python3 -m pip install --no-cache-dir pyahocorasick

# Environment variable agar Spark Master & Worker menggunakan binary Python yang sama
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
