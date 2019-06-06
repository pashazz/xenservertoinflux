FROM gradle:jdk11 as builder
COPY --chown gradle:gradle . /src
WORKDIR /src
RUN gradle build

FROM openjdk:11
COPY --from=builder /src/build/distributions/xenservertoinflux.tar /Applications
WORKDIR /app
RUN tar xvf xenservertoinflux.tar
WORKDIR /app/xenservertoinflux
CMD bin/xenservertoinflux -c config.ini -db http://influxdb:8086
