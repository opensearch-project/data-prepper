FROM python:3.7-slim-buster

COPY . /app/

RUN apt-get update
RUN apt-get install -y curl
RUN pip3 install -r /app/requirements.txt
WORKDIR /app
RUN chmod +x /app/script.sh
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.2.1/wait /app/wait
RUN chmod +x /app/wait

CMD /app/wait && /app/script.sh