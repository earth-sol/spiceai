FROM mcr.microsoft.com/mssql/server:2022-latest
USER root
RUN apt-get update && apt-get upgrade -y
USER mssql