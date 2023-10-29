FROM apache/superset:2.1.1

COPY --chown=superset run.sh /app/

COPY --chown=superset superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py
