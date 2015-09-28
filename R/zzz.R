.onLoad <- function(libname, pkgname) {
  op <- options()
  op.dplyr <- list(
    dplyr.vertica_default_schema = "public"
  )
  toset <- !(names(op.dplyr) %in% names(op))
  if(any(toset)) options(op.dplyr[toset])

  invisible()
}

