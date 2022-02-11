FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

USER root
WORKDIR /app

COPY requirements.txt /app/

RUN pip install -r requirements.txt

COPY etl.py /app/

EXPOSE 8080

ENTRYPOINT [ "python3" ]

CMD [ "etl.py" ]