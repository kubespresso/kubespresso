FROM python:3.6.8-alpine3.9

COPY kubespresso.py /
COPY requirements.txt /
RUN pip install -U pip && pip install -r requirements.txt && rm requirements.txt
ENTRYPOINT ["python", "/kubespresso.py"]
