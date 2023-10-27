superset fab create-admin --username ${SUPERSET_ADMIN_USERNAME} \
                --firstname Superset \
                --lastname Admin \
                --email admin@superset.com \
                --password ${SUPERSET_ADMIN_PASSWORD}
superset db upgrade
superset init
/usr/bin/run-server.sh
