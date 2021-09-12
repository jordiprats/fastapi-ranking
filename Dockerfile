FROM python:3.8-alpine

WORKDIR /code

# uvicorn - not an actual dependency
RUN pip install uvicorn

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ranking.py .

EXPOSE 8000

CMD [ "/usr/local/bin/uvicorn", "ranking:app", "--host", "0.0.0.0", "--port", "8000" ]