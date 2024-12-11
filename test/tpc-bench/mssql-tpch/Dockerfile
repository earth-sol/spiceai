FROM mcr.microsoft.com/mssql/server:2022-latest
ENV ACCEPT_EULA=Y
ENV MSSQL_SA_PASSWORD=S3cretP@ssw0rd
ENV DB_NAME=tpch

# restore from backup
USER root

RUN mkdir -p /data & \
    mkdir -p /srv

COPY ./data /data
COPY ./setup.sh /srv/setup.sh
COPY ./entrypoint.sh /srv/entrypoint.sh

RUN chmod +x /srv/setup.sh & \
    chmod +x /srv/entrypoint.sh & \
    chown mssql:mssql -R /data & \
    chown mssql:mssql -R /srv

USER mssql

CMD ["/bin/bash", "/srv/entrypoint.sh"]