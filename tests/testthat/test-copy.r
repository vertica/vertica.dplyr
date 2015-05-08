source("setup.r")

context("copy_to")

test_that("odbc copying rows of nyc flights table to vertica works",{
  library(nycflights13)
  flights_table <- flights[1:100,]
  a <- copy_to(vertica_odbc,flights_table,"flights")

  expect_equal(as.data.frame(flights_table),as.data.frame(a))
})

test_that("jdbc copying rows of nyc flights table to vertica works",{
  library(nycflights13)
  flights_table <- flights[1:100,]
  if(db_has_table(vertica_jdbc$con,"flights")) db_drop_table(vertica_jdbc$con,"flights")
  a <- copy_to(vertica_jdbc,flights_table,"flights")

  expect_equal(as.data.frame(flights_table),as.data.frame(a))
})

