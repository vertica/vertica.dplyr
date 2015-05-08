source("setup.r")

context("arrange")

test_that("odbc arrange functionality works", {
  vertica_salary_tbl <- tbl(vertica_odbc,"Salaries")
  arranged_vertica <- as.data.frame(arrange(select(vertica_salary_tbl,salary),desc(salary)))
  arranged_local <- as.data.frame(arrange(select(Salaries,salary),desc(salary)))

  expect_equal(arranged_local,arranged_vertica)
})

test_that("jdbc arrange functionality works", {
  vertica_salary_tbl <- tbl(vertica_jdbc,"Salaries")
  arranged_vertica <- as.data.frame(arrange(select(vertica_salary_tbl,salary),desc(salary)))
  arranged_local <- as.data.frame(arrange(select(Salaries,salary),desc(salary)))

  expect_equal(arranged_local,arranged_vertica)
})
