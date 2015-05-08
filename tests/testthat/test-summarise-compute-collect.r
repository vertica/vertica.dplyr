source("setup.r")

context("summarise-collect-compute")

  vertica_salaries_tbl <- tbl(vertica_odbc,"Salaries")

  by_team <- group_by(Salaries, teamID)
  team_salaries_local <- summarise(by_team,
  count = n(),
  avg_salary=mean(salary))

  by_team_vertica <- group_by(vertica_salaries_tbl, teamID)

  team_salaries_vertica <- summarise(by_team_vertica,
  count = n(),
  avg_salary=mean(salary))

  salaries_local <- as.data.frame(select(arrange(team_salaries_local,desc(avg_salary)),count,avg_salary))
  salaries_vertica <- as.data.frame(select(arrange(team_salaries_vertica,desc(avg_salary)),count,avg_salary))

test_that("odbc summarise functionality works", {
   expect_equal(salaries_local,salaries_vertica)

})

test_that("odbc collect and compute work", {
  if(db_has_table(vertica_odbc$con,"compute_test_table")) db_drop_table(vertica_odbc$con,"compute_test_table")
  
  tmp <- compute(select(arrange(team_salaries_vertica,desc(avg_salary)),count,avg_salary), "compute_test_table")
  collected_tbl <- as.data.frame(collect(tmp))
  
  expect_equal(salaries_local,collected_tbl)
})

  vertica_salaries_tbl <- tbl(vertica_jdbc,"Salaries")
  by_team_vertica <- group_by(vertica_salaries_tbl, teamID)

  team_salaries_vertica <- summarise(by_team_vertica,
  count = n(),
  avg_salary=mean(salary))

test_that("jdbc summarise functionality works", {
   expect_equal(salaries_local,salaries_vertica)

})

test_that("jdbc collect and compute work", {
  if(db_has_table(vertica_jdbc$con,"compute_test_table")) db_drop_table(vertica_jdbc$con,"compute_test_table")

  tmp <- compute(select(arrange(team_salaries_vertica,desc(avg_salary)),count,avg_salary), "compute_test_table")
  collected_tbl <- as.data.frame(collect(tmp))

  expect_equal(salaries_local,collected_tbl)
})

