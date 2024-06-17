

# libraries ---------------------------------------------------------------

library(duckdb)
library(dplyr)
library(fs)
library(tidyverse)
library(vroom)
library(btools)
library(skimr)


# microdata ---------------------------------------------------------------

# E:\data\acs\pums\5year\2022
dpath <- r"(E:\data\acs\pums\5year\2022)"
acs1yrdb <- fs::path( r"(E:\data\acs\pums\1year\acs1yrdb.duckdb)")
acs5yrdb <- fs::path( r"(E:\data\acs\pums\5year\acs5yrdb.duckdb)")


temp_dir <- tempdir()  # Define a temporary directory

erase_temp <- function(temp_dir){
  closeAllConnections()
  files_to_delete <- list.files(temp_dir, full.names = TRUE, recursive = TRUE)
  unlink(files_to_delete, recursive = TRUE)
}

erase_temp(temp_dir)
list.files(temp_dir, full.names = TRUE, recursive = TRUE)

# showConnections(all = TRUE)
# file.exists(files_to_delete[1])


# function ----------------------------------------------------------------

f1year <- function(year, rectype){
  a <- proc.time()

  stopifnot(year %in% c(2012:2019, 2021:2022))
  stopifnot(rectype %in% c("person", "household"))

  temp_dir <- tempdir()
  erase_temp(temp_dir) # try to get rid of all files in the temp directory

  ypath <- path(r"(E:\data\acs\pums\1year\)", year)

  if(rectype=="household"){
    zfile <- "csv_hus.zip"
    tabname <- paste0("hus1_", year)
  } else if(rectype=="person") {
    zfile <- "csv_pus.zip"
    tabname <- paste0("pus1_", year)
  }

  zpath <- path(ypath, zfile)
  csvfiles <- unzip(zpath, list = TRUE) |>
    filter(str_detect(Name, ".csv")) |>
    arrange(Name)

  # get column types
  unzip(zipfile=zpath, files = csvfiles$Name, exdir = temp_dir, overwrite = TRUE) # extract all csv files
  zpaths <- path(temp_dir, csvfiles$Name)

  # get column types from full first file in zip archive
  some_rows <- vroom(zpaths[1], guess_max=10e3, n_max=10e3)
  column_types <- sapply(some_rows, class)
  column_classes <- column_types # this will be a named vector, too
  column_classes[column_types=="logical"] <- "numeric"
  iweights <- str_detect(names(column_classes), coll("pwgtp", ignore_case = TRUE))
  column_classes[iweights] <- "numeric"
  # print(table(column_types))
  # print(table(column_classes))

  con <- dbConnect(duckdb(dbdir = acs1yrdb))
  df <- vroom(zpaths, col_types = column_classes) |>
    btools::lcnames()
  dbWriteTable(con, tabname, df, overwrite=TRUE)
  # I use the vroom-dbWriteTable combination rather than duckdb_read_csv() because
  # the latter does not seem robust enough
  dbDisconnect(con)

  b <- proc.time()
  print(b - a)
}


g1year <- function(year){
  f1year(year, rectype="household")
  f1year(year, rectype="person")
}

g1year(2022)
g1year(2021)
# no 2020 - ACS was bad that year
g1year(2019)
g1year(2018)
g1year(2017)
g1year(2016)
g1year(2015)
g1year(2014)
g1year(2013)
g1year(2012)



# take a look -------------------------------------------------------------


con <- dbConnect(duckdb(dbdir = acs1yrdb))
dbGetInfo(con)
dbListTables(con)

dbListFields(con, name="hus1_2021")
query <- "SELECT COUNT(*) as total_rows FROM hus1_2021"
dbGetQuery(con, query)


dbListFields(con, name="pus1_2021") |> sort()
query <- "SELECT COUNT(*) as total_rows FROM pus1_2021"
dbGetQuery(con, query)


tbl(con, "hus5_2022") |>
  group_by(DIVISION) |>
  summarise(n=n()) |>
  collect()

stcounts <- tbl(con, "hus5_2022") |>
  group_by(ST) |>
  summarise(n=n()) |>
  arrange(desc(n)) |>
  collect()

nyh <- tbl(con, "hus5_2022") |>
  filter(ST==36) |>
  select(ST, RT, SERIALNO, WGTP, NP, FINCP, ADJINC) |>
  collect() |>
  lcnames()

nyp <- tbl(con, "pus1_2022") |>
  filter(mig %in% 2:3) |> # movers 2 diff house outside US/PR ly, 3 diff house in US/PR ly
  filter(st==36 | migsp=='036') |>
  select(st, migsp, rt, serialno, mig, pwgtp, pincp) |>
  collect()

nyp |> pull(pwgtp) |> sum()

nyp |>
  filter(mig==3, migsp!='036') |> # domestic inmovers from out of state
  summarise(n=n(), wtdn=sum(pwgtp), .by=migsp) |>
  mutate(domshare=wtdn / sum(wtdn)) |>
  arrange(desc(wtdn))
# 34 NJ
# 6 CA
# 42 PA
# 12 FL
# 25 MA

# 9  CT
# 37 NC
# 48 TX
# 51 VA
# 17

year <- 2022
year <- 2017
year <- 2012
tbl(con, paste0("pus1_", year)) |>
  filter(mig %in% 2:3) |> # movers 2 diff house outside US/PR ly, 3 diff house in US/PR ly
  filter(st==36 | migsp=='036') |>
  filter(mig==3, migsp!='036') |> # domestic inmovers from out of state
  select(st, migsp, rt, serialno, mig, pwgtp, pincp) |>
  summarise(n=n(), wtdn=sum(pwgtp, na.rm = TRUE), .by=migsp) |>
  mutate(totn=sum(wtdn), domshare=wtdn / sum(wtdn), year=!!year) |>
  arrange(desc(wtdn)) |>
  collect()


dbDisconnect(con)









# f(2021, rectype="person")
# f(2021, rectype="household")
#
# f(2019)

#.. household records ----
zpath <- path(dpath, "csv_hus.zip")

csvfiles <- unzip(zpath, list = TRUE) |>
  filter(str_detect(Name, ".csv"))
csvfiles

# get column types
unzip(zpath, files = csvfiles$Name, exdir = temp_dir, overwrite = TRUE)
list.files(temp_dir)

(zpaths <- path(temp_dir, csvfiles$Name))

some_rows <- vroom(zpaths[1], n_max=100e3)
column_types <- sapply(some_rows, class)
table(column_types)
# (nchar <- names(column_types[column_types == "character"]))
# (nnum <- names(column_types[column_types == "numeric"]))

con <- dbConnect(duckdb(dbdir = acs5yrdb))
a <- proc.time()
for (i in 1:length(zpaths)) {
  print(zpaths[i])
  duckdb_read_csv(con, "hus5_2022", zpaths[i], colClasses = column_types)
}
b <- proc.time()
b - a

dbDisconnect(con)


#.. person records ----
zpath <- path(dpath, "csv_pus.zip")

csvfiles <- unzip(zpath, list = TRUE) |>
  filter(str_detect(Name, ".csv"))
csvfiles

# get column types
unzip(zpath, files = csvfiles$Name, exdir = temp_dir, overwrite = TRUE)
list.files(temp_dir)

(zpaths <- path(temp_dir, csvfiles$Name))

some_rows <- vroom(zpaths[1], n_max=Inf)
column_types <- sapply(some_rows, class)
table(column_types)
# (nchar <- names(column_types[column_types == "character"]))
# (nnum <- names(column_types[column_types == "numeric"]))
# (nlogic <- names(column_types[column_types == "logical"]))
column_classes <- str_replace(column_types, "logical", "numeric")
column_classes["GCM"] <- "character"
table(column_classes)

con <- dbConnect(duckdb(dbdir = acs5yrdb))
a <- proc.time()
for (i in 1:length(zpaths)) {
  print(zpaths[i])
  duckdb_read_csv(con, "pus5_2022", zpaths[i], colClasses = column_classes)
}
b <- proc.time()
b - a

dbDisconnect(con)



# take a look -------------------------------------------------------------

con <- dbConnect(duckdb(dbdir = acs5yrdb))
dbGetInfo(con)
dbListTables(con)

dbListFields(con, name="hus5_2022")
query <- "SELECT COUNT(*) as total_rows FROM hus5_2022"
dbGetQuery(con, query)


dbListFields(con, name="pus5_2022") |> sort()
query <- "SELECT COUNT(*) as total_rows FROM pus5_2022"
dbGetQuery(con, query)


tbl(con, "hus5_2022") |>
  group_by(DIVISION) |>
  summarise(n=n()) |>
  collect()

stcounts <- tbl(con, "hus5_2022") |>
  group_by(ST) |>
  summarise(n=n()) |>
  arrange(desc(n)) |>
  collect()

nyh <- tbl(con, "hus5_2022") |>
  filter(ST==36) |>
  select(ST, RT, SERIALNO, WGTP, NP, FINCP, ADJINC) |>
  collect() |>
  lcnames()

nyp <- tbl(con, "pus5_2022") |>
  filter(ST==36) |>
  select(ST, RT, SERIALNO, PWGTP, PINCP) |>
  collect() |>
  lcnames()

nyp |> pull(pwgtp) |> sum()


dbDisconnect(con)

summary(ny)
skim(ny)
# fincp na is GQ

# duckdb_register(con, "flights", nycflights13::flights)


# older stuff -------------------------------------------------------------

# OLDER person records ----------------------------------------------------------

zpath <- path(dpath, "csv_pus.zip")

csvfiles <- unzip(zpath, list = TRUE) |>
  filter(str_detect(Name, ".csv"))
csvfiles

temp_dir <- tempdir()  # Define a temporary directory
unzip(zpath, files = csvfiles$Name, exdir = temp_dir, overwrite = TRUE)

zpaths <- path(temp_dir, csvfiles$Name)
df2 <- vroom(zpaths, id="fname")
glimpse(df2)
count(df2, fname)

con <- dbConnect(duckdb(dbdir = duckdb))

a <- proc.time()
for (i in 1:4) {
  print(zpaths[i])
  duckdb_read_csv(con, "data", zpaths[i], colClasses = col_classes)
}
b <- proc.time()
b - a


a <- proc.time()
dbWriteTable(con, "pus5_2022", df2)
b <- proc.time()
b - a


column_types <- sapply(df2, class)
table(column_types)
sts <- count(df2, ST)
(nchar <- names(column_types[column_types == "character"]))
(nnum <- names(column_types[column_types == "numeric"]))
all_names <- names(df2)

col_classes <- setNames(rep("NULL", length(all_names)), all_names) # Initialize colClasses with "NULL" assuming you might want to skip unspecified columns
col_classes[nchar] <- "character"
col_classes[nnum] <- "numeric"
col_classes

a <- proc.time()
duckdb_read_csv(con, "data", zpaths[1], colClasses = col_classes)
b <- proc.time()
b - a

a <- proc.time()
for (i in 1:4) {
  print(zpaths[i])
  duckdb_read_csv(con, "data", zpaths[i], colClasses = col_classes)
}
b <- proc.time()
b - a

# query <- "SELECT COUNT(*) as total_rows FROM data"
# dbGetQuery(con, query)

query <- "SELECT COUNT(*) as total_rows FROM pus5_2022"
dbGetQuery(con, query)

drop_table_query <- "DROP TABLE IF EXISTS data"
dbExecute(con, drop_table_query)



dbDisconnect(con)




duckdb_read_csv(con, "data", zpath)


dbReadTable(con, "data")

dbDisconnect(con)

dbWriteTable(con, "iris_table", iris)

# parse column types

duckdb_read_csv(con, "data", p1)

a <- proc.time()
df <- read_csv(path(temp_dir, csvfiles$Name[1]))
b <- proc.time()
b - a # 16 secs

a <- proc.time()
df2 <- vroom(path(temp_dir, csvfiles$Name[1]))
b <- proc.time()
b - a # 2 secs

glimpse(df)


temp_file <- tempfile()
unzip(zip_path, files = file_name, exdir = tempdir(), overwrite = TRUE)
extracted_path <- file.path(tempdir(), file_name)


mtcars_by_cyl <- vroom_example(vroom_examples("mtcars-"))
mtcars_by_cyl
tmp <- vroom(mtcars_by_cyl[4])
glimpse(tmp)

tmp <- vroom(zpath, id="fname")
glimpse(tmp)
count(tmp, fname)

tmp2 <- vroom(c(p1, p2), id="fname")
glimpse(tmp2)
count(tmp2, fname)

con <- dbConnect(duckdb())


duckdb_read_csv(con, "data", zpath)
dbReadTable(con, "data")

dbDisconnect(con)


read_csv_to_duckdb <- function(file_name, table_name, conn) {
  # Temporarily extract the file
  temp_file <- tempfile()
  unzip(zip_path, files = file_name, exdir = tempdir(), overwrite = TRUE)
  extracted_path <- file.path(tempdir(), file_name)

  # Read CSV file into DuckDB
  duckdb_read_csv(conn, table_name, extracted_path)

  # Optionally, delete the temporary file
  unlink(extracted_path)
}



# test --------------------------------------------------------------------






con <- dbConnect(duckdb())
duckdb_register(con, "flights", nycflights13::flights)

tbl(con, "flights") |>
  group_by(dest) |>
  summarise(delay = mean(dep_time, na.rm = TRUE)) |>
  collect()

dbGetInfo(con)
dbListTables(con)
dbListFields(con, name="flights")
dbColumnInfo(res, ...)

dbColumnInfo(res, ...)

duckdb_read_csv()

dbDisconnect(con)

# read into duckdb without reading into memory (I think)
con <- dbConnect(duckdb())

data <- data.frame(a = 1:3, b = letters[1:3])
path <- tempfile(fileext = ".csv")

write.csv(data, path, row.names = FALSE)

duckdb_read_csv(con, "data", path)
dbReadTable(con, "data")

dbDisconnect(con)




