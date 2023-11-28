FROM python:3.10-buster

ENV PYTHONUNBUFFERED True

WORKDIR /dbt
COPY . .

RUN pip install -r requirements.txt
RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]