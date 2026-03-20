# List easyNRD session directories under a cache root.
.nrd_list_session_temp_dirs <- function(path) {
  if (!dir.exists(path)) {
    return(character(0))
  }

  dirs <- list.dirs(path, recursive = FALSE, full.names = TRUE)
  dirs[grepl("^easyNRD_[0-9]+(?:_.+)?$", basename(dirs))]
}

# Extract the process ID encoded in an easyNRD session directory name.
.nrd_extract_session_pid <- function(dir_path) {
  pid_chr <- sub("^easyNRD_([0-9]+).*$", "\\1", basename(dir_path))
  suppressWarnings(as.integer(pid_chr))
}

# Check whether a process ID is currently alive.
.nrd_pid_is_alive <- function(pid) {
  if (!is.numeric(pid) || length(pid) != 1 || is.na(pid) || pid <= 0) {
    return(FALSE)
  }

  if (as.integer(pid) == as.integer(Sys.getpid())) {
    return(TRUE)
  }

  isTRUE(tryCatch(
    tools::pskill(as.integer(pid), 0),
    error = function(e) FALSE,
    warning = function(w) FALSE
  ))
}

# Remove a directory and report whether it is gone.
.nrd_remove_dir_safe <- function(path) {
  tryCatch(
    {
      unlink(path, recursive = TRUE, force = TRUE)
      !dir.exists(path)
    },
    error = function(e) FALSE,
    warning = function(w) FALSE
  )
}

# Remove orphaned or all easyNRD session directories from a cache root.
.nrd_cleanup_session_temp_dirs <- function(path, force = FALSE) {
  dirs <- .nrd_list_session_temp_dirs(path)

  if (length(dirs) == 0) {
    return(0L)
  }

  if (isTRUE(force)) {
    targets <- dirs
  } else {
    pids <- vapply(dirs, .nrd_extract_session_pid, FUN.VALUE = integer(1))
    alive <- vapply(pids, .nrd_pid_is_alive, FUN.VALUE = logical(1))
    targets <- dirs[!alive]
  }

  as.integer(sum(vapply(targets, .nrd_remove_dir_safe, FUN.VALUE = logical(1))))
}

#' Resolve the easyNRD cache directory
#'
#' `nrd_cache_dir()` resolves the package cache directory in a fixed order and
#' creates it when needed.
#'
#' @returns A single path string.
#' @export
#'
#' @examples
#' nrd_cache_dir()
nrd_cache_dir <- function() {
  env_path <- Sys.getenv("EASYNRD_CACHE_DIR", unset = "")
  if (nzchar(env_path)) {
    path <- env_path
  } else {
    opt_path <- getOption("easynrd.cache_dir")
    if (is.character(opt_path) && length(opt_path) == 1 && !is.na(opt_path) && nzchar(opt_path)) {
      path <- opt_path
    } else {
      path <- tools::R_user_dir("easyNRD", "cache")
    }
  }

  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  path
}

#' Close easyNRD-managed resources
#'
#' `nrd_close()` releases the DuckDB connection and session temp directory owned
#' by a table created with [nrd_ingest()].
#'
#' @param data A DuckDB-backed lazy table returned by [nrd_ingest()].
#'
#' @returns Invisibly returns `TRUE` if resources were released and `FALSE`
#'   otherwise.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   data <- nrd_ingest("/path/to/file.parquet")
#'   nrd_close(data)
#' }
nrd_close <- function(data) {
  owner <- .nrd_get_owner(data)
  if (is.null(owner) || isTRUE(owner$closed)) {
    return(invisible(FALSE))
  }

  con <- owner$con
  temp_dir <- owner$temp_dir

  if (inherits(con, "duckdb_connection") && isTRUE(tryCatch(DBI::dbIsValid(con), error = function(e) FALSE))) {
    DBI::dbDisconnect(con, shutdown = TRUE)
  }

  if (is.character(temp_dir) && length(temp_dir) == 1 && dir.exists(temp_dir)) {
    unlink(temp_dir, recursive = TRUE, force = TRUE)
  }

  owner$closed <- TRUE
  owner$con <- NULL
  owner$temp_dir <- NULL

  invisible(TRUE)
}

#' Remove easyNRD temporary session directories
#'
#' `nrd_cleanup()` removes `easyNRD_*` cache subdirectories from a base path.
#'
#' @param path Directory that contains `easyNRD_*` subdirectories.
#' @param force If `TRUE`, remove all matching directories. If `FALSE`, remove
#'   only directories belonging to inactive process IDs.
#'
#' @returns Invisibly returns the number of directories removed.
#' @export
#'
#' @examples
#' if (FALSE) {
#'   nrd_cleanup()
#' }
nrd_cleanup <- function(path = nrd_cache_dir(), force = FALSE) {
  if (!is.character(path) || length(path) != 1 || is.na(path) || !nzchar(path)) {
    rlang::abort("`path` must be a single, non-empty path string.")
  }

  if (!is.logical(force) || length(force) != 1 || is.na(force)) {
    rlang::abort("`force` must be TRUE or FALSE.")
  }

  if (!dir.exists(path)) {
    return(invisible(0L))
  }

  invisible(.nrd_cleanup_session_temp_dirs(path = path, force = force))
}
