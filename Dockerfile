# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster
ADD puma_scrape.py /

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "puma_scrape.py"]
