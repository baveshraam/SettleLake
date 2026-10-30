FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .
ENV SETTLELAKE_HOME=/data
VOLUME /data
EXPOSE 8501
CMD ["settlelake", "dashboard", "--port", "8501"]
