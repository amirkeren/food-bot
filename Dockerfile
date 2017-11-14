FROM python:latest
LABEL maintainer "Amir Keren <amir.k@taboola.com>"

RUN git clone https://github.com/amirkeren/food-bot /food-bot/
WORKDIR /food-bot
RUN pip install -r requirements.txt

ENV BOT_ACCESS_TOKEN <ACCESS_TOKEN_HERE>
ENV OAUTH_ACCESS_TOKEN <ACCESS_TOKEN_HERE>
ENV VERIFICATION_TOKEN <VERIFICATION_TOKEN_HERE>

EXPOSE 5000
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]
