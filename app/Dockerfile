FROM python:3.10-alpine

WORKDIR /app

COPY data ./data

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY main.py ./main.py

CMD [ "python", "./main.py"]
