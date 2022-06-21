FROM oceandata.azurecr.io/daskhub-python-notebook:92410250edb1ece19ab234766af0bacd9003b2de

USER root

RUN pip install poetry
WORKDIR /src
COPY . .

RUN pip install nox papermill python-dotenv

CMD ["nox"]

