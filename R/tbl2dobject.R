####################################################################
#A dplyr connector for the Vertica database.
#Copyright (C) [2015] Hewlett-Packard Development Company, L.P.

#This program is free software; you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation; either version 2 of the License, or (at
#your option) any later version.

#This program is distributed in the hope that it will be useful, but
#WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#General Public License for more details.  You should have received a
#copy of the GNU General Public License along with this program; if
#not, write to the Free Software Foundation, Inc., 59 Temple Place,
#Suite 330, Boston, MA 02111-1307 USA
#####################################################################

## The following are wrapper functions to transform dplyr table to dobjects in Distributed R 
## Requires packages distributedR and HPdata
## Note: These 'tbl2dobject' functions will be moved to HPData package and become 'db2dobject'
## when this functionality is officially supported

#' Converts a dplyr tbl object to a darray in Distributed R.
#'
#' This requires distributedR and HPData and will attempt to load them. If Distributed R is not already running, it will start it.
#' Internally, this function simply saves the tbl reference as a view in Vertica and then uses the Vertica Loader to move data into
#' Distributed R, and then deletes the view from database. Note that if you do not use the Vertica Native Data Loader, your table
#' must have a 'rowid' column for this to work (see ?HPdata::db2darray for more information).
#'
#' @param table The local R-variable name of the tbl_vertica object to be converted to a darray.
#' @param dsn The name of the DSN as specified in the ODBC.INI file. If an ODBC connection 
#' is already active, then the DSN of that connection will be used. If not (JDBC is being used), this field will be 
#' required.
#' @param features Names of the columns to convert into the darray, provided as a list.
#' @param npartitions this optional argument specifies the desired number of splits (partitions) in the
#' dobject.
#' @param verticaConnector TRUE to use the Vertica Connector for Distributed R. If FALSE, your table must 
#' include a 'rowid' column. See the manual page for HPdata::db2darray for more information.
#' @param loadPolicy "local" or "uniform". Please see help doc for db2darray in package HPdata.
#' Please read the details for more information.
#' @return A new darray object
#' @examples
#' \dontrun{
#' vertica <- src_vertica("VerticaDSN")
#' table1 <- tbl(vertica,"some_table")
#' table1_modified <- filter(table1,foo > 3)
#' my_darray <- tbl2darray(table1_modified,features=list("some_column"))
#' }
#' 
#' @export
tbl2darray <- function(table, dsn, features, npartitions, verticaConnector=TRUE, loadPolicy="local") {
  .attemptLoad("HPdata")
  .invokeLoader(dsn=.checkAndStart(table,dsn),table=table,features=features,npartitions=npartitions,verticaConnector=verticaConnector,loadPolicy=loadPolicy,type="darray")
}
      
#' Converts a dplyr tbl object to a pair of darrays in Distributed R which correspond to the responses and 
#' predictors of a predictive model. It is assumed that samples (including responses and predictors) are 
#' stored in a single table.
#'
#' This requires distributedR and HPData and will attempt to load them. If Distributed R is not already running, it will start it.
#' Internally, this function simply saves the tbl reference as a view in Vertica and then uses the Vertica Loader to move data into
#' Distributed R, and then deletes the view from database. Note that if you do not use the Vertica Native Data Loader, your table
#' must have a 'rowid' column for this to work (see ?HPdata::db2darrays for more information).
#'
#' @param table The local R-variable name of the tbl_vertica object to be converted to darrays.
#' @param dsn The name of the DSN as specified in the ODBC.INI file. If an ODBC connection 
#' is already active, then the DSN of that connection will be used. If not (JDBC is being used), this field will be 
#' required.
#' @param resp the list of the column names corresponding to responses.
#' @param pred this is an optional argument to specify list of the column names corresponding to predictors. If this argument is not specfied or is empty, the function will load all columns of the table or view excluding the column specified in resp argument.
#' @param npartitions this optional argument specifies the desired number of splits (partitions) in the
#' dobject.
#' @param verticaConnector TRUE to use the Vertica Connector for Distributed R. If FALSE, your table must 
#' include a 'rowid' column. See the manual page for HPdata::db2darrays for more information.
#' @param loadPolicy "local" or "uniform". Please see help doc for db2darrays in package HPdata.
#' Please read the details for more information.
#' @return  Y : the darray of responses; X : the darray of predictors 
#' @examples
#' \dontrun{
#' my_darrays <- tbl2darrays(my_table,resp=list("some_column"))
#' }
#' 
#' @export
tbl2darrays <- function(table, dsn, resp, pred, npartitions, verticaConnector=TRUE, loadPolicy="local") {
  .attemptLoad("HPdata")
  .invokeLoader(dsn=.checkAndStart(table,dsn),table=table,resp=resp,pred=pred,npartitions=npartitions,verticaConnector=verticaConnector,loadPolicy=loadPolicy,type="darrays")
}

#' Converts a dplyr tbl object to a dframe in Distributed R.
#'
#' This requires distributedR and HPData and will attempt to load them. If Distributed R is not already running, it will start it.
#' Internally, this function simply saves the tbl reference as a view in Vertica, uses the Vertica Loader to move data into
#' Distributed R, and then deletes the view from database. Note that if you do not use the Vertica Native Data Loader, your table
#' must have a 'rowid' column for this to work (see ?HPdata::db2dframe for more information).
#'
#' @param table The local R-variable name of the tbl_vertica object to be converted to a dframe.
#' @param dsn The name of the DSN as specified in the ODBC.INI file. If an ODBC connection 
#' is already active, then the DSN of that connection will be used. If not (JDBC is being used), this field will be 
#' required.
#' @param features Names of the columns to convert into the dframe, provided as a list.
#' @param npartitions this optional argument specifies the desired number of splits (partitions) in the
#' dobject.
#' @param verticaConnector TRUE to use the Vertica Connector for Distributed R. If FALSE, your table must 
#' include a 'rowid' column. See the manual page for HPdata::db2dframe for more information.
#' @param loadPolicy "local" or "uniform". Please see help doc for db2dframe in package HPdata.
#' Please read the details for more information.
#' @return A new dframe object
#' @examples
#' \dontrun{
#' vertica <- src_vertica("VerticaDSN")
#' table1 <- tbl(vertica,"some_table")
#' table1_modified <- filter(table1,foo > 3)
#' my_dframe <- tbl2dframe(table1_modified,features=list("some_column"))
#' }
#' 
#' @export
tbl2dframe <- function(table, dsn, features, npartitions, verticaConnector=TRUE, loadPolicy="local") {
  .attemptLoad("HPdata")
  .invokeLoader(dsn=.checkAndStart(table,dsn),table=table,features=features,npartitions=npartitions,verticaConnector=verticaConnector,loadPolicy=loadPolicy,type="dframe")
}

.invokeLoader <- function(dsn,table,features,npartitions,verticaConnector,loadPolicy,type,resp,pred){
  viewname <- dplyr:::random_table_name()
  suppressMessages(db_save_view(table,viewname,temporary=FALSE))
  sql <- build_sql("DROP VIEW ", ident(table),
    con = table$src$con)
  on.exit(invisible(db_drop_view(table$src$con,viewname)))

  if(type=="dframe") {
    loader <- "suppressMessages(db2dframe(viewname,
                     dsn,features,npartitions,
                     verticaConnector=verticaConnector,
                     loadPolicy=loadPolicy))"
  } 
  
  if(type=="darray") {
    loader <- "suppressMessages(db2darray(viewname,
                     dsn,features,npartitions,
                     verticaConnector=verticaConnector,
                     loadPolicy=loadPolicy))"
  }

  if(type=="darrays") {
    loader <- "suppressMessages(db2darrays(viewname,
                     dsn,resp=resp,pred=pred,npartitions,
                     verticaConnector=verticaConnector,
                     loadPolicy=loadPolicy))"
  }

  tryCatch(eval(parse(text=loader)),error = 
        function(e) {
          if(verticaConnector){
            warning("Could not load using the native data loader. Setting verticaConnector=FALSE.")
            assign("verticaConnector",FALSE,envir=parent.env(environment()))
            eval(parse(text=loader),envir=parent.env(environment()))
          }
          else stop(e)
       })
}

.checkAndStart <- function(table, dsn){
  # Start Distributed R if not running
  tryCatch(distributedR_status(),
           error = function(e) {
             message("Distributed R is not running. Starting it...")
             tryCatch(distributedR_start(),
                      error = function(e) {
                        stop("Could not start Distributed R. Cannot convert to dobject")
                      })
           })
 
  assert_that(class(table)[1] == "tbl_vertica")
  if(table$src$con@type != "ODBC") assert_that(is.character(dsn))
  else dsn <- table$src$info["Data_Source_Name"]
  dsn
}
