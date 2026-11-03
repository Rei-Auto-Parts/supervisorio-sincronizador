FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir python-tds pg8000
COPY sincronizador.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "sincronizador.py"]
