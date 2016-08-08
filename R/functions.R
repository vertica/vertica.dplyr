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

## This file defines the scalar, window, and aggregate functions that are invokable in Vertica, as well as utility functions for UDFs

#' This function shows to the user the names of the funtions, their type, as well as the return and input data types.
#'
#' @param src The src_vertica instance from which to query for UDxes.
#' @param type The UDF type to list (one of "aggregate","scalar", or "transform").
#' @return A data frame containing four columns: function name, function type, output data type(s), and input data type(s).
#' @examples
#' \dontrun{
#' vertica <- src_vertica("VerticaDSN")
#' UDF <- list_udf(vertica)
#' }
#' @export
list_udf <- function(src,type=NULL) {
  if(is.null(type)) where <- "1=1"
  else if(type == "transform") {
    where <- "procedure_type='Transform'" 
  } else if (type == "aggregate") {
    where <- "procedure_type='Aggregate'"
  }
   else if (type == "scalar") {
    where <- "procedure_type='Scalar'"
  } else {
    where <- "1=1"
  }

  list_udf_query <- paste0("SELECT * FROM (SELECT schema_name,function_name AS UDF_NAME,DECODE(procedure_type,\'User Defined Function\',\'Scalar\',\'User Defined Transform\',\'Transform\',\'User Defined Aggregate \',\'Aggregate\',\'other\') AS PROCEDURE_TYPE,function_return_type AS RETURN_TYPE,function_argument_type AS ARGUMENT_TYPES FROM user_functions) as foo WHERE ", where)

 if(src$con@type=="ODBC") {
    res <- sqlQuery(src$con@conn,list_udf_query)
  }
  else {
    res <- dbGetQuery(src$con@conn,list_udf_query)
  }

  function.names <- mapply(function(x,y) {
    if(as.character(x) != "public") {
      y <- paste0(as.character(x),".",as.character(y))
    }
    as.character(y)
  },res[[1]],res[[2]])

  if(length(function.names) == 0) function.names <- data.frame(function.names=integer(0))

  out <- cbind(function.names,res[,c(3,4,5)])
  out
}

validate_range <- function(range) {
  if(is.character(range)) {
    range[1] = tryCatch({val=eval(parse(text=range[1]))
                 assert_that(!is.na(val))
                 val
               },
                 error = function(e){
                 range[1] = -Inf
               })

    range[2] = tryCatch({val=eval(parse(text=range[2]))
                 assert_that(!is.na(val))
                 val
               },
                 error = function(e){
                 range[2] = Inf
               }) 
    }

    if(!is.null(range)) range <- as.numeric(range)

  range
}

udf_helper <- function(params) {
  paramStr <- ""

  suppressWarnings( if(!is.list(params)) {
    if(!is.character(params)) stop("UDF parameters have to be passed as a key-value named list")
    if(substr(params,1,5) != "LIST(") stop("UDF parameters have to be passed as a key-value named list")
  } )

  suppressWarnings(  if(is.character(params) && nchar(params) > 6 ) {
    paramStr <- substr(params,6,nchar(params)-1)
    paramStr <- gsub('"','',paramStr)
    paramStr <- strsplit(paramStr,", ")

    paramStr <- lapply(paramStr[[1]],function(x) {
                  temp <- strsplit(x," AS ")
                    if(length(temp[[1]]) < 2) stop("UDF parameters must be named!")
                      paste0(temp[[1]][[2]],"=",temp[[1]][[1]])
                })

    paramStr <- paste0(paramStr,collapse=", ")
})

  suppressWarnings(if(is.list(params) && length(params) > 0) {
    if(length(names(params)) != length(params)) stop("UDF parameters must be named!")
    paramsList <- mapply(function(x,y) { paste0(x,"=",escape(y)) }, names(params),params)
    paramStr <- paste0(paramsList,collapse=", ")
  })
   
  paramStr
}

# Generic Vertica window function sql constructor with range and order by parameters.
vertica_win_func <- function(f) {
  force(f)
  function(..., partition=dplyr:::partition_group(),order=dplyr:::partition_group(), range=NULL) {
    range <- validate_range(range) 
    over(build_sql(sql(f), list(...)), partition, order, frame = range)
  }
}

# Generic Vertica UDF sql constructor for scalar and transform functions.
vertica_udf <- function(f,transform=FALSE) {
  force(f)
  if(!transform) {
    function(...,params=list()) {
      args <- list(...)
      params <- udf_helper(params)
      if(nchar(params) > 0) {
        udf <- paste0("USING PARAMETERS ", params)
        if(length(args) > 0) {
          args[[length(args)]] <- sql(paste(args[[length(args)]],udf))
        } else {
          args[[1]] <- sql(udf)
        }
      }
     build_sql(sql(f),args)
    }
  }
  else {
    function(..., params=list(), partition=dplyr:::partition_group(),order=dplyr:::partition_group(), range=NULL) {
      args <- list(...)
      params <- udf_helper(params)
      if(nchar(params) > 0) {
          udf <- paste0("USING PARAMETERS ", params)
        if(length(args) > 0) {
          args[[length(args)]] <- sql(paste(args[[length(args)]],udf))
        } else {
          args[[1]] <- sql(udf)
        }
      }
      range <- validate_range(range)
      over(build_sql(sql(f), args), partition, order, frame = range)
    }
  }
}

# Scalar Functions
vertica_scalar_func <- sql_translator(.parent=base_scalar)

# Window Functions
vertica_window_func <- sql_translator(
  lag = vertica_win_func("lag"),
  changed = vertica_win_func("conditional_change_event"),
  isTrue = vertica_win_func("conditional_true_event"),
  ntile = vertica_win_func("ntile"),
  lead = vertica_win_func("lead"),
  median = vertica_win_func("median"),
  row_number = vertica_win_func("row_number"),
  head = vertica_win_func("first_value"),
  tail = vertica_win_func("last_value"),
  exp_mvg_avg = vertica_win_func("exponential_moving_average"),
  percentile_cont = vertica_win_func("percentile_cont"),
  percentile_disc = vertica_win_func("percentile_disc"),
  min_rank = vertica_win_func("rank"),
  rank = vertica_win_func("rank"),
  dense_rank = vertica_win_func("dense_rank"),
  percent_rank = vertica_win_func("percent_rank"),
  cume_dist = vertica_win_func("cume_dist"),
  row_num = vertica_win_func("row_number"),
  sd = vertica_win_func("stddev"),
  sd_pop = vertica_win_func("stddev_pop"),
  sd_samp = vertica_win_func("stddev_samp"),
  var_pop = vertica_win_func("var_pop"),
  var_samp = vertica_win_func("var_samp"),
  mean = vertica_win_func("avg"),
  sum = vertica_win_func("sum"),
  min = vertica_win_func("min"),
  max = vertica_win_func("max"),
  n = function(
          order=dplyr:::partition_group(), 
          range=c(-Inf,Inf)) {
          dplyr:::over(build_sql(sql("COUNT(*)")), 
          order, order, frame = range)
  }
)

# Aggregate Functions
vertica_agg_func <- sql_translator(
  .parent = base_agg,
   n = function() sql("count(*)")
  ,sd = sql_prefix("stddev",1)
  ,sd_pop = sql_prefix("stddev_pop",1)
  ,sd_samp = sql_prefix("stddev_samp",1)
  ,var_pop = sql_prefix("var_pop",1)
  ,var_samp = sql_prefix("var_samp",1)
  ,lm_slope = sql_prefix("regr_slope",2)
  ,lm_intercept = sql_prefix("regr_intercept",2)
  ,cov = sql_prefix("covar_samp",2)
  ,bitwAnd = sql_prefix("bit_and",1)
  ,bitwOr = sql_prefix("bit_or",1)
  ,bitwXor = sql_prefix("bit_xor",1)
)

# Powers translations of scalar and window functions
#' @export
sql_translate_env.VerticaConnection <- function(x) {
  sql_variant(scalar = vertica_scalar_func,
  window = vertica_window_func,
  aggregate = vertica_agg_func
  )
}

# Queries the database for UDFs, and appropriately "registers" them in vertica.dplyr
import_udf <- function(src) {
  aggregates <- list_udf(src,"aggregate")
  scalars <- list_udf(src,"scalar")
  transforms <- list_udf(src,"transform")
  
  agg_funs <- lapply(as.list(as.character(aggregates[["function.names"]])),
                     vertica_udf,
                     transform=FALSE)

  names(agg_funs) <- as.character(aggregates[["function.names"]])  

  scalar_funs <- lapply(as.list(as.character(scalars[["function.names"]])),
                        vertica_udf,      
                        transform=FALSE)

  names(scalar_funs) <- as.character(scalars[["function.names"]])  

  transform_funs <- lapply(as.list(as.character(transforms[["function.names"]])),
                           vertica_udf,
                           transform=TRUE)

  names(transform_funs) <- as.character(transforms[["function.names"]])  
  
  vertica_agg_func <- list2env(agg_funs, dplyr:::copy_env(vertica_agg_func))
  vertica_scalar_func <- list2env(scalar_funs, dplyr:::copy_env(vertica_scalar_func))
  vertica_window_func <- list2env(transform_funs, dplyr:::copy_env(vertica_window_func))

  assign("sql_translate_env.src_vertica", function(x) {
    sql_variant(
      scalar = vertica_scalar_func,
      window = vertica_window_func,
      aggregate = vertica_agg_func
      )
  }, envir = parent.frame(n=2))

}

# Test whether a string matches the '"schema_name"."table_name"' pattern:
is.schema_table <- function(tablename) {
  if(!is.string(tablename)) return(FALSE)
  grepl('^".*"\\.".*"$', tablename)
}

# Extract schema and table names from a possibly "packed" table name.
# The following are possible scenarios:
#  - tablename is provided as a packed string with schema's name before the dot
#    and table's name after the dot, and both are within double quotemarks (important!):
#    tablename = '"schema_name"."table_name"'
#  - tablename is a string that doesn't match the above pattern:
#    then tablename is considered to be the name of a table and the name of schema
#    is taken from the value of the "dplyr.vertica_default_schema" option
#    which is set by default to be "public".
# Returns a list with schema and table string elements.
#' @export
get_schema_table <- function(tablename) {
  assert_that(is.string(tablename))

  if(is.schema_table(tablename)) {
    schema <- sub('^"(.*?)"\\."(.*)"$', "\\1", tablename, perl=T)
    table <- sub('^"(.*?)"\\."(.*)"$', "\\2", tablename, perl=T)
  } else {
    schema <- getOption("dplyr.vertica_default_schema")
    table <- tablename
  }
  list(schema=schema, table=table)
}

# Returns a proper sql piece for the "schema_name"."table_name" construct.
# In a way, it is a slightly more complex identifier than dplyr would assume,
# so cannot assign 'ident' class to it as it would break many things.
# If table name isn't fully specified (schema name is absent), the default schema name 
# stored in the "dplyr.vertica_default_schema" option is used.
#' @export
ident_schema_table <- function(tablename) {
  n <- get_schema_table(tablename)
  build_sql(ident(n$schema), ".", ident(n$table))
}

