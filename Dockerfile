FROM sofraserv/financedb_base:test

WORKDIR /var/www/tdQuoteFlaskDocker
RUN    mkdir /var/www/tdQuoteFlaskDocker/logs

EXPOSE 18080

COPY   requirements.txt requirements.txt
RUN    pip3 install -r requirements.txt

COPY   . /var/www/tdQuoteFlaskDocker

ADD start.sh /var/www/tdQuoteFlaskDocker/start.sh
RUN chmod +x /var/www/tdQuoteFlaskDocker/start.sh
CMD ["/var/www/tdQuoteFlaskDocker/start.sh"]