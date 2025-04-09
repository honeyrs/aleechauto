FROM mysterysd/wzmlx:latestv3

#FROM anasty17/mltb:latest
#FROM mysterysd/wzmlx:latest


WORKDIR /usr/src/app
RUN chmod 777 /usr/src/app

RUN apt-get update && apt-get install -y python3-dev
RUN pip3 install --upgrade setuptools

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash", "start.sh"]
