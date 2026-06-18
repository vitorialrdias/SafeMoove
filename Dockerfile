FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --trusted-host pypi.org \
                --trusted-host files.pythonhosted.org \
                --no-cache-dir -r requirements.txt

CMD ["python"]