# One small image serves both the API and the dashboard; they differ only in
# the command they run. Project code is mounted at runtime, so editing a file
# needs a container restart, not a rebuild.
FROM python:3.12-slim

WORKDIR /opt/app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8000 8501