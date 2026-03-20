#' Ingest NRD parquet files lazily
#'
#' `nrd_ingest()` creates or reuses a DuckDB-backed lazy table without pulling
#' data into memory.
#'
#' @param paths A character vector of `.parquet` paths, an Arrow dataset or
#'   query, or an existing DuckDB-backed lazy table.
#'
#' @returns A DuckDB-backed `tbl_lazy`.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   data <- nrd_ingest(c("/path/to/nrd_2019.parquet", "/path/to/nrd_2020.parquet"))
#' }
nrd_ingest <- function(paths) {
  cleanup_needed <- TRUE

  if (inherits(paths, "tbl_lazy")) {
    .nrd_assert_duckdb_lazy(paths, arg = "paths")
    return(paths)
  }

  if (inherits(paths, c("Dataset", "ArrowTabular", "arrow_dplyr_query"))) {
    dataset <- paths
  } else {
    if (!is.character(paths) || length(paths) == 0) {
      rlang::abort("`paths` must be a non-empty character vector of `.parquet` files.")
    }

    dataset <- arrow::open_dataset(paths, format = "parquet", unify_schemas = TRUE)
  }

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  temp_dir <- .nrd_configure_connection(con)
  owner <- .nrd_make_owner(con, temp_dir)

  on.exit(
    {
      if (isTRUE(cleanup_needed) && !isTRUE(owner$closed)) {
        try(nrd_close(owner), silent = TRUE)
      }
    },
    add = TRUE
  )

  data <- arrow::to_duckdb(dataset, con = con)
  data <- .nrd_standardize_names(data)
  data <- .nrd_attach_owner(data, owner)

  cleanup_needed <- FALSE
  data
}
