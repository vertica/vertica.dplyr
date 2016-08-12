#this will output a table of type vertica.dplyr

generate_sql_query <- function(fun, tbl_in, col_types, col_names, col_lengths,  params = list(), libraries = NULL)
{
	params$vertica.dplyr.libraries = libraries
	params$vertica.dplyr.func = fun
	params = serialize_object(params)

	outcols = list(lengths = col_lengths, 
		col_types = col_types, 
		names = col_names)
	outcols = serialize_object(outcols)


	tbl_out <- add_op_single("custom_R_udf",tbl_in, 
		dots = list(params = params, outcols = outcols))

	return(tbl_out)
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
		#col_names = names(output)
		#if(length(col_names) < length(output))
			col_names = paste0("col",1:length(output))
		return(data.frame(col_types = col_types, col_names = col_names,
			col_length = col_length))
	}

	params = list(...)
	params$col_types = NULL
	params$col_names = NULL
	params$col_lengths = NULL
	params_copy = params
	params_copy$user_fun = fun
	out_types = generate_sql_query(test_fun, tbl, 
		col_types = c(6,6,2),
		col_names = c("types","names","length"), 
		col_lengths = c(100,100, 1),
		params = params_copy)


	out_types = collect(out_types)
	if(nrow(out_types) == 0)
		stop("No output columns found")
	col_types <- aggregate(types ~ names, out_types, function(x)
	{
		types <- unique(x)
		if(length(types) > 1)
		stop("output types do not match for same column")
		else
		types
	})
	col_types <- as.character(col_types$types)
	col_types = sapply(col_types, vertica_R_mapping)
	col_types = sapply(col_types, function(z) 
		       which(z == possible_output_types))
	col_lengths = aggregate(length ~ names, out_types, max)$length
	col_names <- sort(unique(out_types$names))
	
	output = generate_sql_query(fun, tbl, 
		col_types, col_names, col_lengths, params = params)

	return(output)
	
}

sql_build.op_custom_R_udf <- function(op, con, ...)
{
	object <- list()
    	subquery <- sql_build(op$x, con)


    	group_vars <- sql(ident(op_grps(op$x)))
    	order_vars <- sql(op_sort(op$x))

    	if(length(group_vars) == 0)
    		group_vars = NULL
    	if(length(order_vars) == 0)
        	order_vars = NULL

    	object <- list()
    	object$from <- subquery
    	object$partition = group_vars
    	object$order = order_vars

	object$params = op$dots$params
	object$outcols = op$dots$outcols
	class(object) <- "custom_R_udf_query"
	return(object)
}

sql_render.custom_R_udf_query <- function (query, con = NULL, ..., root = FALSE)
{
	from <- sql_render(query$from, root = root)
    	sub_query_fields <- db_query_fields(con, from)

     	from <- sql_subquery(con, from)

	partition = sql("")
	order = sql("")


	if(!is.null(query$partition))
	{
	partition = query$partition
	#partition = dplyr::translate_sql_(query$partition,
	#	con = con, vars = sub_query_fields, 
    	#	window = FALSE)
	partition = sql_vector(partition, parens = FALSE, collapse = ",")
	partition = build_sql("PARTITION BY ", partition)
	}
	if(!is.null(query$order))
	{
	order = query$order
	#order = dplyr::translate_sql_(query$order,
	#	con = con, vars = sub_query_fields, 
    	#	window = FALSE)
	order = sql_vector(order, parens = FALSE, collapse = ",")
	order = build_sql("ORDER BY ", order)

	}
	
	over <- build_sql("OVER(",partition," ", order, ")")
	query_sql <- build_sql("SELECT vertica_function_executor(* ",
			  "using parameters params=", query$params, 
			  ", outcols = ", query$outcols,
			  ") ",over," FROM ", from)

	return(query_sql)
}