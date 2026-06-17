# Stage 1: install dependencies
FROM apache/airflow:2.9.1-python3.11 AS builder

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

<<<<<<< Updated upstream
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
=======
# Stage 2: final lean image
FROM apache/airflow:2.9.1-python3.11
>>>>>>> Stashed changes

USER airflow
COPY --from=builder /home/airflow/.local /home/airflow/.local
ENV PATH=/home/airflow/.local/bin:$PATH   