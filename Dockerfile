FROM oceandata.azurecr.io/daskhub-python-notebook:latest

USER root

RUN pip install poetry
WORKDIR /src
COPY . .

RUN pip install nox papermill python-dotenv

CMD ["nox"]

