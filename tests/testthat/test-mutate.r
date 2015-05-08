source("setup.r")

context("mutate")

test_that("odbc mutate functionality works", {
  vertica_tbl <- tbl(vertica_odbc,"Salaries")
  
  modified_local_tbl <- as.data.frame(select(mutate(Salaries, foo = yearID + 3),foo))
  modified_vertica_tbl <- as.data.frame(select(mutate(vertica_tbl, foo = yearID + 3),foo))

  expect_equal(modified_local_tbl,modified_vertica_tbl)
})

test_that("jdbc mutate functionality works", {
  vertica_tbl <- tbl(vertica_jdbc,"Salaries")
  
  modified_local_tbl <- as.data.frame(select(mutate(Salaries, foo = yearID + 3),foo))
  modified_vertica_tbl <- as.data.frame(select(mutate(vertica_tbl, foo = yearID + 3),foo))

  expect_equal(modified_local_tbl,modified_vertica_tbl)
})
   
