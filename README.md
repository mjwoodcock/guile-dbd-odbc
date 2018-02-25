Guile DBI ODBC Driver
=====================

Pre-requisites
--------------

Guile-dbi from [here](https://github.com/opencog/guile-dbi)  
Guile development libraries  
unixODBC development librariies  
CMake  

Installation
------------

mkdir build  
cd build  
cmake -G "Unix Makefiles" ..  
make  
sudo make install  

Usage
-----
To connect:  
  (db-open "odbc" "DSN=dsnname")

Any valid DSN can be used, such as:  
  (db-open "odbc" "DSN=dsnname;pwd=password...")

Differences With Other Guile DBD Drivers
----------------------------------------

The ODBC DBD Driver returns '() for SQL NULL values instead of #f.
