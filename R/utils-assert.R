# Assert that required columns exist before continuing.
.nrd_assert_cols <- function(data, required_cols, arg = "data") {
  missing_cols <- setdiff(required_cols, colnames(data))

  if (length(missing_cols) > 0) {
    rlang::abort(
      paste0(
        "`", arg, "` is missing required columns: ",
        paste(missing_cols, collapse = ", "),
        "."
      )
    )
  }

  invisible(data)
}

# Check whether an object participates in lazy evaluation.
.nrd_is_lazy_table <- function(data) {
  inherits(data, "tbl_lazy") || inherits(data, "arrow_dplyr_query")
}

# Assert that an object is a DuckDB-backed lazy table.
.nrd_assert_duckdb_lazy <- function(data, arg = "data") {
  if (!inherits(data, "tbl_lazy") || !.nrd_is_duckdb_lazy(data)) {
    rlang::abort(
      paste0(
        "`", arg, "` must be a DuckDB-backed lazy table. ",
        "Use `nrd_ingest()` to initialize the pipeline."
      )
    )
  }

  invisible(data)
}
