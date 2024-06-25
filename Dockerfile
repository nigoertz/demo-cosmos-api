FROM python:3.12.0
WORKDIR /src
COPY /src/main.py main.py
RUN pip3 install fastapi pymongo pydantic
COPY . .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]