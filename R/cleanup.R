#' Remove easyNRD Temporary Session Directories
#'
#' `nrd_cleanup()` removes temporary `easyNRD_*` session directories from a
#' base temp location. Use this utility between long-running epidemiology jobs
#' to recover disk space, especially on shared institutional servers.
#'
#' @param temp_directory Base directory that contains `easyNRD_*` subfolders.
#'   Defaults to [base::tempdir()].
#' @param force Logical flag. If `FALSE` (default), remove only orphaned
#'   directories from inactive process IDs. If `TRUE`, remove all
#'   `easyNRD_*` directories in `temp_directory`.
#'
#' @returns Invisibly returns the number of directories removed.
#' @family pipeline functions
#' @export
#'
#' @examples
#' \donttest{
#' if (FALSE) {
#'   nrd_cleanup()
#'   nrd_cleanup(temp_directory = "/scratch/nrd_tmp", force = TRUE)
#' }
#' }
nrd_cleanup <- function(temp_directory = tempdir(), force = FALSE) {
  if (!is.character(temp_directory) ||
    length(temp_directory) != 1 ||
    is.na(temp_directory) ||
    nchar(temp_directory) == 0) {
    cli::cli_abort(c(
      "`temp_directory` must be a single, non-empty path string.",
      i = "Use the same base directory supplied to `nrd_ingest()` when possible."
    ))
  }

  if (!is.logical(force) || length(force) != 1 || is.na(force)) {
    cli::cli_abort("`force` must be TRUE or FALSE.")
  }

  if (!dir.exists(temp_directory)) {
    cli::cli_inform("No cleanup needed, `{temp_directory}` does not exist.")
    cli::cli_alert_success("Removed 0 easyNRD temporary directories.")
    return(invisible(0L))
  }

  mode <- if (isTRUE(force)) "all easyNRD session folders" else "orphaned easyNRD session folders"
  cli::cli_inform(c(
    "Scanning `{temp_directory}` for temporary easyNRD directories.",
    i = "Cleanup mode: {mode}."
  ))

  cleanup <- .nrd_cleanup_session_temp_dirs(
    temp_directory = temp_directory,
    force = force
  )

  cli::cli_alert_success(
    "Removed {cleanup$removed} easyNRD temporary director{?y/ies}."
  )

  invisible(cleanup$removed)
}
