FROM python:3.9.13-buster

WORKDIR /var/www/tdQuoteFlaskDocker

RUN    apt-get update

RUN    echo y | apt-get install unixodbc unixodbc-dev
RUN    echo y | apt-get install locales
RUN    echo y | apt-get install vsftpd
RUN    echo y | apt-get install libpam-pwdfile
RUN    sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen
RUN    locale-gen en_US.UTF-8  
ENV    LANG en_US.UTF-8  
ENV    LANGUAGE en_US:en  
ENV    LC_ALL en_US.UTF-8
EXPOSE 18080

COPY   requirements.txt requirements.txt
RUN    pip3 install -r requirements.txt

COPY   . /var/www/tdQuoteFlaskDocker

ADD start.sh /var/www/tdQuoteFlaskDocker/start.sh
RUN chmod +x /var/www/tdQuoteFlaskDocker/start.sh
CMD ["/var/www/tdQuoteFlaskDocker/start.sh"]