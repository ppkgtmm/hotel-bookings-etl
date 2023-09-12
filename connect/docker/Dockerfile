FROM confluentinc/cp-kafka-connect:7.4.0

COPY run.sh .

USER root

RUN chmod +x run.sh

USER appuser

ENTRYPOINT [ "/bin/sh", "-c" ]
