

generate_sql_query <- function(fun, tbl, col_types, col_names, lengths, partition_cols, order_cols, libraries = NULL, ...)
{
	
	params = list(...)
	params$vertica.dplyr.libraries = libraries
	params$vertica.dplyr.func = fun

	params = serialize_object(params)

	outcols = list(lengths = lengths, col_types = col_types, names = col_names)
	outcols = serialize_object(outcols)

	parameters_clause = paste0("using parameters params = '",
		params, "', outcols='",outcols,"'")

	over_clause = ""
	
	source_query <- tbl$query$sql
	source_query <- paste0("( ", source_query ,") AS ZZZ1")

	query = paste0("select vertica_function_executor(* ", 
	        parameters_clause,
		") over(",
		over_clause,
		") from ",
		source_query)


	vertica <- tbl$src$con
	temp <- select(tbl)
	output_table <- temp
	output_table$select <- NULL
	output_table <- update(output_table, select = col_names)
	output_table$query <- query.VerticaConnection(vertica, query,NULL)
	output_table$from <- paste0("( ", query ,") AS ZZZ1")
	class(output_table$from) <- c("sql", "character")

	output_table$select = sapply(col_names,as.symbol)
	output_table$mutate = TRUE
	output_table$order_by = FALSE
	output_table$summarise = FALSE
	output_table$where = FALSE
	#print(sqlQuery(vertica@conn,query))
	
	return (output_table)

}


execute_custom_R_udf <- function( fun, tbl, ..., 
		     partition_cols = character(0), 
		     order_cols = character(0), 
		     libraries = character(0))
{
	temp.env <- new.env(parent = globalenv())
	environment(fun) <- temp.env

	additional_args = list(...)
	if(any(names(additional_args) == ""))
		stop("All additional arguments must be named")

	test_fun <- function(x)
	{
		environment(user_fun) <- environment()
		output <- user_fun(x)
		output <- as.data.frame(output)
		factor_cols = sapply(output, is.factor)
		output[,factor_cols] = as.character(output[,factor_cols])
		col_types = sapply(output, typeof)
		col_length = sapply(output, function(z) 
		{
			if(is.character(z))
				return(max(1000,sapply(z,nchar))+1)
			return(1)
		})
		col_names = names(output)
		if(length(col_names) < length(output))
			col_names = paste0("col",1:length(output))
		return(data.frame(col_types = col_types, col_names = col_names,
			col_length = col_length))
	}


	out_types = generate_sql_query(test_fun, tbl, 
		c(6,6,2),c("types","names","length"), c(100,100, 1), 
		partition_cols = NULL, order_cols = NULL, ..., user_fun = fun)

	out_types = collect(out_types)

	out_types[[1]] = sapply(out_types[[1]], vertica_R_mapping)
	out_types[[1]] = sapply(out_types[[1]], function(z) 
		       which(z == possible_output_types))
	
	output = generate_sql_query(fun, tbl, 
		out_types[[1]], out_types[[2]], out_types[[3]], 
		partition_cols = NULL, order_cols = NULL, ...)

	return(output)
	
}


