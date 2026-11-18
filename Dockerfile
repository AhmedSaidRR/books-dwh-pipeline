FROM apache/airflow:2.10.5-python3.11

# Airflow's constraints file pins pandas/requests/urllib3 to versions that are
# known to work with this Airflow release, so the pipeline cannot break the
# scheduler image by accident.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    -r /tmp/requirements.txt

# Sanity check so a broken image fails at build time, not at DAG parse time.
RUN python -c "import bs4, pandas, psycopg, pyarrow; print('pipeline deps OK')"
