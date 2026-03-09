#' Deprecated: Use `nrd_ingest()` + `nrd_build_episodes()`
#'
#' `nrd_read()` is retained for backward compatibility and forwards to the new
#' pipeline API.
#'
#' @param datasets Character vector of parquet file paths.
#' @param transfer_disp_codes Integer `DISPUNIFORM` transfer codes.
#' @param transfer_sameday_codes Integer `SAMEDAYEVENT` continuity codes.
#'
#' @return A lazy episode-level table.
#' @export
nrd_read <- function(
  datasets,
  transfer_disp_codes = 2L,
  transfer_sameday_codes = c(1L, 4L)
) {
  .Deprecated(new = "nrd_ingest + nrd_build_episodes")
  nrd_ingest(datasets = datasets) |>
    nrd_build_episodes(
      transfer_disp_codes = transfer_disp_codes,
      transfer_sameday_codes = transfer_sameday_codes
    )
}
