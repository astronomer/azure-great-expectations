FROM astronomerinc/ap-airflow:1.10.12-1-buster-onbuild
ENV AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME=3600