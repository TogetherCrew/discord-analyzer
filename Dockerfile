# It's recommended that we use `bullseye` for Python (alpine isn't suitable as it conflcts with numpy)
FROM python:3.10-bullseye AS base
WORKDIR /project
COPY . .
RUN pip3 install -r requirements.txt

FROM base AS test
RUN chmod +x docker-entrypoint.sh
CMD ["./docker-entrypoint.sh"]

FROM base AS prod
CMD ["python3", "start_rabbit_mq.py"]