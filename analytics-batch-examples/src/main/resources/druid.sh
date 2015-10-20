
# start overload service
java -Xms2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/overlord:lib/* io.druid.cli.Main server overlord
