FROM python:alpine

RUN apk add --no-cache build-base g++ gcc python3-dev linux-headers

RUN python3 -m pip install --upgrade pip

COPY ./requirements.txt .
RUN python3 -m pip install -r requirements.txt

COPY ./app.py .

ENTRYPOINT python3 ./app.py
