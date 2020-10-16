FROM python:3.7-buster

COPY requirements.txt /

RUN pip3 install -r /requirements.txt

COPY databaseService.py /
WORKDIR /

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.2.1/wait /wait
RUN chmod +x /wait

CMD /wait && python3 databaseService.py