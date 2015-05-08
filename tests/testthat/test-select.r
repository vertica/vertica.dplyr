source("setup.r")

context("select")

test_that("odbc select functionality works", {
  vertica_salary_tbl <- tbl(vertica_odbc,"Salaries")
  sal_local <- as.data.frame(select(Salaries,salary,teamID))
  sal_vertica <- as.data.frame(select(vertica_salary_tbl,salary,teamID))

  expect_equal(names(sal_local),names(sal_vertica))
  expect_equal(nrow(sal_local),nrow(sal_vertica))
  expect_error(select(Salaries,nonsensename))
  expect_error(select(vertica_salary_tbl,nonsensename))
})

test_that("jdbc select functionality works", {
  vertica_salary_tbl <- tbl(vertica_jdbc,"Salaries")
  sal_local <- as.data.frame(select(Salaries,salary,teamID))
  sal_vertica <- as.data.frame(select(vertica_salary_tbl,salary,teamID))

  expect_equal(names(sal_local),names(sal_vertica))
  expect_equal(nrow(sal_local),nrow(sal_vertica))
  expect_error(select(Salaries,nonsensename))
  expect_error(select(vertica_salary_tbl,nonsensename))
})
