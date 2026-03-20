with_duckdb_connection <- function(fn) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(
    {
      if (isTRUE(tryCatch(DBI::dbIsValid(con), error = function(e) FALSE))) {
        DBI::dbDisconnect(con, shutdown = FALSE)
      }
    },
    add = TRUE
  )

  fn(con)
  invisible(NULL)
}
