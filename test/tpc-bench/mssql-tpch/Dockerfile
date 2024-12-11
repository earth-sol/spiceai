FROM mcr.microsoft.com/mssql/server:2022-latest
ENV ACCEPT_EULA=Y
ENV DB_NAME=tpch

# restore from backup
USER root
RUN mkdir -p /srv
RUN mkdir -p /data

COPY ./data/tpch.bak /data/tpch.bak
COPY ./setup.sh /srv/setup.sh
COPY ./entrypoint.sh /srv/entrypoint.sh

RUN chmod +x /srv/setup.sh & \
    chmod +x /srv/entrypoint.sh & \
    chmod mssql:mssql -R /data & \
    chown mssql:mssql -R /srv

USER mssql

CMD ["/bin/bash", "/srv/entrypoint.sh"]