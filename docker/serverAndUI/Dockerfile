#
# conductor:serverAndUI - Netflix conductor server and UI
#
FROM openjdk:8-jdk

MAINTAINER Netflix OSS <conductor@netflix.com>

# Make app folders
RUN mkdir -p /app/config /app/logs /app/libs

# Startup script(s)
COPY ./bin /app

# Configs
COPY ./config /app/config

# Get all the dependencies
RUN apt-get update -y && apt-get -y install git curl && chmod +x /app/startup.sh
RUN curl -sL https://deb.nodesource.com/setup_9.x |  bash  &&  apt-get install -y nodejs build-essential

# Get and install conductor
RUN git clone https://github.com/fsa-streamotion/conductor.git \
  && cd conductor \
  && ./gradlew build -w -x test \
  && mv ./server/build/libs/conductor-server-*-all.jar /app/libs/ \
  && mv ./ui /app \
  && cd /app/ui \
  && npm install \
  && npm run build --server \
  && cd / \
  && rm -rf conductor


EXPOSE 5000 8080

CMD ["/app/startup.sh"]
ENTRYPOINT ["/bin/bash"]