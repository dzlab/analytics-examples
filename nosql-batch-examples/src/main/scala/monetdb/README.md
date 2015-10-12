= Install MonetDB
```bach
cat <<'EOF' >> /etc/apt/sources.list.d/monetdb.list
deb http://dev.monetdb.org/downloads/deb/ vivid monetdb
deb-src http://dev.monetdb.org/downloads/deb/ vivid monetdb
EOF

wget --output-document=- https://www.monetdb.org/downloads/MonetDB-GPG-KEY | sudo apt-key add -
sudo apt-get update
sudo apt-get install monetdb5-sql monetdb-client
```

= Run MonetDB
```bach
monetdbd start /dbfarm
monetdb create database_name   # create a database in a maintenance mode
monetdb release database_name  # leave maintaince mode
monetdb destroy database_name  # destroy database
monetdb status
```

= CLI client
```batch
mclient -d database_name       # default user/password (monetdb/monetdb)
sql> select * from tables where system = false;
```
