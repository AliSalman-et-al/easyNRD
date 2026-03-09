#' Materialize and Export an NRD Pipeline
#'
#' `nrd_export()` forces execution of a lazy NRD pipeline and writes the result
#' to a compressed parquet file.
#'
#' @param .data A lazy or in-memory table produced by the easyNRD pipeline.
#' @param path Output parquet file path.
#'
#' @return The output `path`, invisibly.
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   nrd_ingest("/path/to/nrd.parquet") |>
#'     nrd_build_episodes() |>
#'     nrd_export("/path/to/episodes.parquet")
#' }
#' }
nrd_export <- function(.data, path) {
  if (!is.character(path) || length(path) != 1 || is.na(path) || nchar(path) == 0) {
    rlang::abort("`path` must be a single, non-empty file path string.")
  }

  dir_path <- dirname(path)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }

  out <- if (inherits(.data, "tbl_lazy") || inherits(.data, "arrow_dplyr_query")) {
    dplyr::collect(.data)
  } else {
    .data
  }

  arrow::write_parquet(out, sink = path, compression = "zstd")
  invisible(path)
}
