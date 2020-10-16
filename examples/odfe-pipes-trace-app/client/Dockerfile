FROM python:3.7-slim-buster

COPY requirements.txt /

RUN pip3 install -r requirements.txt

COPY client.py /
WORKDIR /

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.7.3/wait /wait
RUN chmod +x /wait

CMD /wait && python3 client.py