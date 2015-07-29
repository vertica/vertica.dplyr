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

## This file defines the scalar, window, and aggregate functions that are invokable in Vertica, as well as utility functions for R-UDxes

#' This function shows to the user the names of the funtions, their type, as well as the return and input data types.
#'
#' @param src The src_vertica instance from which to query for UDxes.
#' @return A data frame containing four columns: function name, function type, output data type(s), and input data type(s).
#' @examples
#' \dontrun{
#' vertica <- src_vertica("VerticaDSN")
#' UDxes <- list_udx(vertica)
#' }
#' @export
list_udx <- function(src,type=NULL) {
  
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

  list_udx_query <- paste0("SELECT * FROM (SELECT schema_name,function_name,DECODE(procedure_type,\'User Defined Function\',\'Scalar\',\'User Defined Transform\',\'Transform\',\'User Defined Aggregate \',\'Aggregate\',\'other\') AS procedure_type,function_return_type,function_argument_type FROM user_functions) as foo WHERE ", where)

 if(src$con@type=="ODBC") {
    res <- sqlQuery(src$con@conn,list_udx_query)
  }
  else {
    res <- dbGetQuery(src$con@conn,list_udx_query)
  }

    function.names <- mapply(function(x,y) {
      if(as.character(x) != "public") {
        y <- paste0(as.character(x),".",as.character(y))
      }
      as.character(y)
    },res[[1]],res[[2]])

    out <- cbind(function.names,res[,c(3,4,5)])
    names(out) <- c("UDF Name","Type","Return Type","Argument Type(s)")
    out
}

# Generic Vertica window function sql constructor with range and order by parameters.
vertica_win_func <- function(f) {
  force(f)
  function(..., partition=dplyr:::partition_group(),order=dplyr:::partition_group(), range=NULL) {
    if(is.character(range)) {
      range[1] = tryCatch({val=eval(parse(text=range[1]))
                           assert_that(!is.na(val))
                           val},
                 error = function(e){
                 range[1] = -Inf
               })
      range[2] = tryCatch({val=eval(parse(text=range[2]))
                           assert_that(!is.na(val))
                           val},
                 error = function(e){
                 range[2] = Inf
               }) 
    }
    if(!is.null(range)) range <- as.numeric(range)
    over(build_sql(sql(f), list(...)), partition, order, frame = range)
  }
}

vertica_udf <- function(f,transform=FALSE) {
  force(f)
  if(!transform) {
    function(...,params=list()) {
      udf <- build_sql("USING PARAMETERS ", dplyr:::sql_vector(params))
    
      args <- list(...)
      print(class(args[[length(args)]]))
      args[[length(args)]] <- sql(paste(args[[length(args)]],udf))

      build_sql(sql(f),args)
    }
  }

  else {

  function(..., partition=dplyr:::partition_group(),order=dplyr:::partition_group(), range=NULL) {
    if(is.character(range)) {
      range[1] = tryCatch({val=eval(parse(text=range[1]))
                           assert_that(!is.na(val))
                           val},
                 error = function(e){
                 range[1] = -Inf
               })
      range[2] = tryCatch({val=eval(parse(text=range[2]))
                           assert_that(!is.na(val))
                           val},
                 error = function(e){
                 range[2] = Inf
               }) 
    }
    if(!is.null(range)) range <- as.numeric(range)
    over(build_sql(sql(f), list(...)), partition, order, frame = range)
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
src_translate_env.src_vertica <- function(x) {
  sql_variant(scalar = vertica_scalar_func,
  window = vertica_window_func,
  aggregate = vertica_agg_func
  )
}

import_udxes <- function(src) {
  aggregates <- list_udx(src,"aggregate")
  scalars <- list_udx(src,"scalar")
  transforms <- list_udx(src,"transform")
  
  agg_funs <- mapply(vertica_udf,
                  as.list(as.character(aggregates[["UDF Name"]])),
                  FALSE)

  names(agg_funs) <- as.character(aggregates[["UDF Name"]])  

  scalar_funs <- mapply(vertica_udf,
                  as.list(as.character(scalars[["UDF Name"]])),
                  FALSE)

  names(scalar_funs) <- as.character(scalars[["UDF Name"]])  

  transform_funs <- mapply(vertica_udf,
                  as.list(as.character(transforms[["UDF Name"]])),
                  TRUE)

  names(transform_funs) <- as.character(transforms[["UDF Name"]])  
  
  vertica_agg_func <- list2env(agg_funs, dplyr:::copy_env(vertica_agg_func))
  vertica_scalar_func <- list2env(scalar_funs, dplyr:::copy_env(vertica_scalar_func))
  vertica_window_func <- list2env(transform_funs, dplyr:::copy_env(vertica_window_func))

  assign("src_translate_env.src_vertica", function(x) {
    sql_variant(
      scalar = vertica_scalar_func,
      window = vertica_window_func,
      aggregate = vertica_agg_func
      )
}, envir = globalenv())

}
