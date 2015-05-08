source("setup.r")

context("joins")

d1 <- data_frame(
  x = c(1,2,3),
  y = c(4,5,6),
  a = c("X","Y","Z")
  )

d2 <- data_frame(
  x2 = c(2,2,3),
  y2 = c(4,1,2),
  b = c("B","B","C")
  )

if(!db_has_table(vertica_odbc$con,"d1")) copy_to(vertica_odbc,d1,"d1",fast.load=FALSE)
if(!db_has_table(vertica_odbc$con,"d2")) copy_to(vertica_odbc,d2,"d2",fast.load=FALSE)

dd1 <- tbl(vertica_odbc,"d1")
dd2 <- tbl(vertica_odbc,"d2")

test_that("odbc left joins work", {
  vertica_joined <- as.data.frame(left_join(dd1, dd2, by = c("y" = "y2")))
  ref_join <- as.data.frame(tbl(vertica_odbc,"ref_join"))

  expect_equal(vertica_joined,ref_join)
})
  
dd1 <- tbl(vertica_jdbc,"d1")
dd2 <- tbl(vertica_jdbc,"d2")

test_that("jdbc left joins work", {
  vertica_joined <- as.data.frame(left_join(dd1, dd2, by = c("y" = "y2")))
  ref_join <- as.data.frame(tbl(vertica_jdbc,"ref_join"))

  expect_equal(vertica_joined,ref_join)
})


