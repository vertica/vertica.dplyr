source("setup.r")

context("filter")

test_that("odbc filter functionality works", {
  vertica_salary_tbl <- tbl(vertica_odbc,"Salaries")
  filtered_vertica <- as.data.frame(filter(vertica_salary_tbl,salary==1472819))
  filtered_local <- as.data.frame(filter(Salaries,salary==1472819))

  expect_equal(filtered_local$yearID,filtered_vertica$yearID)
  expect_equal(filtered_local$playerID,filtered_vertica$playerID)
  expect_equal(filtered_local$salary,filtered_vertica$salary)

  expect_equal(nrow(filter(Salaries,salary > 400000)),nrow(filter(vertica_salary_tbl,salary > 400000)))
})

test_that("jdbc filter functionality works", {
  vertica_salary_tbl <- tbl(vertica_jdbc,"Salaries")
  filtered_vertica <- as.data.frame(filter(vertica_salary_tbl,salary==1472819))
  filtered_local <- as.data.frame(filter(Salaries,salary==1472819))

  expect_equal(filtered_local$yearID,filtered_vertica$yearID)
  expect_equal(filtered_local$playerID,filtered_vertica$playerID)
  expect_equal(filtered_local$salary,filtered_vertica$salary)

  expect_equal(nrow(filter(Salaries,salary > 400000)),nrow(filter(vertica_salary_tbl,salary > 400000)))
})
