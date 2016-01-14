FROM rdsn-dev

COPY deploy_svc /home/rdsn/

COPY config.ini /home/rdsn/

ENV LD_LIBRARY_PATH /home/rdsn/lib

CMD ["/home/rdsn/deploy_svc", "/home/rdsn/config.ini" ]

