FROM apache/kafka:latest

COPY init-topic.sh /init-topic.sh

USER root

RUN chmod +x /init-topic.sh

USER appuser

ENTRYPOINT ["/init-topic.sh"]