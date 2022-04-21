FROM mysql:5.7.30
COPY my.cnf /etc/mysql/
COPY mysql.conf.d/ /etc/mysql/mysql.conf.d/
RUN touch -a /var/log/mysql/error.log && chown mysql:adm /var/log/mysql/error.log
RUN touch -a /var/log/mysql/general.log && chown mysql:adm /var/log/mysql/general.log
RUN touch -a /var/log/mysql/slow.log && chown mysql:adm /var/log/mysql/slow.log
