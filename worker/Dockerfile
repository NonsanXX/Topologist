FROM python:3.12-slim
RUN apt-get update && apt-get install -y openssh-client && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY worker/ .
ENV PYTHONUNBUFFERED=1
CMD ["python","-u","worker.py"]
