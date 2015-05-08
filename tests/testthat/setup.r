library(vertica.dplyr)
library(Lahman)

# Connect with JDBC Driver
  vertica_jdbc <- src_vertica(dsn = NULL, jdbcpath="/opt/vertica/java/lib/vertica_jdbc.jar","foobar","localhost",5433,"dbadmin")

# Connect with ODBC Driver
  vertica_odbc <- src_vertica(dsn="VerticaDSN")

if(db_has_table(vertica_odbc$con,"flights")) db_drop_table(vertica_odbc$con,"flights")
if(!db_has_table(vertica_odbc$con,"Salaries")) copy_to(vertica_odbc,Salaries,"Salaries")
if(!db_has_table(vertica_odbc$con,"Salaries")) stop("Couldn't load salaries data from Lahman dataset!")
