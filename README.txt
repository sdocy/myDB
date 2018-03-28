A very simple data store implementation in C++ that imports data from from plain text files and stores the
data in plain text files, using caching to alleviate some of the disk I/O headaches.

Provides a simple query tool written in perl for querying the data store files.

# perl myDBquery.pl [-s option[,option]] [-o option[,option]] [-f option=value]
#
#   -s		specify which record fields of the query result to display
#   -o		specify which record fields to sort the query results by
#   -f		specify a record field and value to filter the query results
#
# option	a valid data field tag...STB, TITLE, PROVIDER, DATE, REV, VIEW_TIME
# value		a valid value for the specified data field


c++ importer code is in https://github.com/sdocy/myDB/tree/master/myDB

perl query code is at https://github.com/sdocy/myDB/blob/master/myDBquery.pl

