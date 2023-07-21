FROM apache/airflow:2.6.3-python3.9
USER root
RUN apt-get update
RUN apt-get install openssl build-essential xorg libssl-dev -y
USER airflow
COPY requirements.txt ./
RUN pip3 install -r ./requirements.txt
# RUN pip install 'apache-airflow[amazon,slack,google,ssh,sftp,papermill]'
# RUN pip install xlsxwriter xlrd openpyxl python-slugify pdfkit pymongo
RUN pip install peewee peewee_async python-dotenv aiopg