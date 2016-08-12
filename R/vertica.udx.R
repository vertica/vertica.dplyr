#x is a data.frame
#y is a list of which the first and only element is a function to run


possible_output_types = c("boolean","int","float","real","char","varchar","longvarchar","date","datetime","smalldatetime","time","timestamp","timestamptz","timetz","numeric","varbinary","longvarbinary","binary")

vertica_R_mapping <- function(type)
{
	if(type == "double")
		return ("float")
	if(type == "integer")
		return ("int")
	if(type == "character")
		return ("varchar")
}


function_charsize = 30*1000*1000


### the main udx execution fuction that does the work
### receives 2 parameters params and outcols
### params are parameters for the function
### outcols are the output column types of the function

vertica_function_executor <- function(x,y)
{
	parameters = deserialize_object(y$params)

	#extract the function
	vertica.dplyr.func = parameters[["vertica.dplyr.func"]]
	parameters["vertica.dplyr.func"] <- NULL

	#extract and load the necessary libraries
	libraries = parameters[["vertica.dplyr.libraries"]]
	parameters["vertica.dplyr.libraries"] <- NULL
	if(length(libraries) > 0)
		sapply(libraries, function(x) library(x,character.only = TRUE))

	list2env(parameters, environment(vertica.dplyr.func))
	
     	output = vertica.dplyr.func(x)
	return(output)
}


vertica_function_executor.OutputCallback <- function(x,y)
{

	outcols = deserialize_object(y$outcols)
	ncols = length(outcols$col_types)
	outcols$col_types = sapply(outcols$col_types, function(z)
	{
		possible_output_types[z]
	})

	ret = data.frame(datatype = outcols$col_types, 
	       length = outcols$length, 
	       scale = rep(1,ncols),
	       name = outcols$names)
	ret
}

vertica_function_executor.ParamCallback <- function(x)
{
	 ret = data.frame(datatype = c("long varchar", "long varchar"), 
	       length = c(function_charsize, function_charsize), 
	       scale = c(1,1), 
	       name = c("params","outcols"))

}

vertica_function_executor.factory <- function()
{
	list(name=vertica_function_executor,
		udxtype=c("transform"),
		intype=c("any"), 
		outtype=c("any"),
		outtypecallback = vertica_function_executor.OutputCallback,
		parametertypecallback = vertica_function_executor.ParamCallback)
}

serialize_object<- function(x)
{
	if(class(x) == "function" || typeof(x) == "closure")
		    environment(x) <- globalenv()
	raw_object <- serialize(x, connection = NULL, ascii = TRUE, xdr = FALSE)
	raw_object <- rawToChar(raw_object)
}

deserialize_object <- function(x)
{
	unserialize(charToRaw(x))
}