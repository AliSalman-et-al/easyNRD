#' Close easyNRD-Owned Resources
#'
#' `nrd_close()` deterministically releases resources owned by `easyNRD`,
#' including DuckDB connections created by [nrd_ingest()] and their session
#' temporary spill directories.
#'
#' @param data An `easyNRD` lazy table returned by [nrd_ingest()] or an
#'   internal owner environment.
#'
#' @returns Invisibly returns `TRUE` when resources were closed by this call,
#'   otherwise `FALSE` when resources were already closed or no owner metadata
#'   is present.
#' @family pipeline functions
#' @export
nrd_close <- function(data) {
  owner <- .nrd_get_owner(data)
  if (is.null(owner)) {
    return(invisible(FALSE))
  }

  invisible(.nrd_close_owner(owner, shutdown = TRUE))
}

#' Inspect easyNRD Resource Ownership
#'
#' `nrd_connection_info()` reports whether an object is managed by easyNRD and
#' returns connection lifecycle metadata for diagnostics and tests.
#'
#' @param data An object accepted by [nrd_close()].
#'
#' @returns A named list with `managed`, `closed`, `connection_valid`, and
#'   `temp_directory` fields.
#' @family pipeline functions
#' @export
nrd_connection_info <- function(data) {
  owner <- .nrd_get_owner(data)
  if (is.null(owner)) {
    return(list(
      managed = FALSE,
      closed = NA,
      connection_valid = NA,
      temp_directory = NULL
    ))
  }

  con <- owner$con
  con_valid <- FALSE
  if (!is.null(con)) {
    con_valid <- isTRUE(tryCatch(
      DBI::dbIsValid(con),
      error = function(e) FALSE,
      warning = function(w) FALSE
    ))
  }

  list(
    managed = TRUE,
    closed = isTRUE(owner$closed),
    connection_valid = con_valid,
    temp_directory = owner$session_temp_dir
  )
}

.nrd_make_owner <- function(con, session_temp_dir = NULL) {
  owner <- new.env(parent = emptyenv())
  owner$.nrd_owner <- TRUE
  owner$closed <- FALSE
  owner$con <- con
  owner$session_temp_dir <- session_temp_dir

  attr(con, "nrd_owner_env") <- owner

  reg.finalizer(
    owner,
    function(e) {
      .nrd_close_owner(e, shutdown = FALSE)
      invisible(NULL)
    },
    onexit = FALSE
  )

  owner
}

.nrd_get_owner <- function(x) {
  if (is.environment(x) && isTRUE(x$.nrd_owner)) {
    return(x)
  }

  if (inherits(x, "duckdb_connection")) {
    owner <- attr(x, "nrd_owner_env", exact = TRUE)
    if (is.environment(owner) && isTRUE(owner$.nrd_owner)) {
      return(owner)
    }
    return(NULL)
  }

  if (inherits(x, "tbl_lazy")) {
    owner <- attr(x, "nrd_env", exact = TRUE)
    if (is.environment(owner) && isTRUE(owner$.nrd_owner)) {
      return(owner)
    }

    con <- tryCatch(dbplyr::remote_con(x), error = function(e) NULL)
    return(.nrd_get_owner(con))
  }

  NULL
}

.nrd_attach_owner <- function(tbl, owner) {
  if (!is.null(owner) && is.environment(owner) && isTRUE(owner$.nrd_owner)) {
    attr(tbl, "nrd_env") <- owner
  }
  tbl
}

.nrd_close_owner <- function(owner, shutdown = TRUE) {
  if (!is.environment(owner) || !isTRUE(owner$.nrd_owner)) {
    return(FALSE)
  }

  if (isTRUE(owner$closed)) {
    return(FALSE)
  }

  con <- owner$con
  session_temp_dir <- owner$session_temp_dir
  disconnected <- TRUE

  if (!is.null(con)) {
    con_is_valid <- isTRUE(tryCatch(
      DBI::dbIsValid(con),
      error = function(e) FALSE,
      warning = function(w) FALSE
    ))

    if (con_is_valid) {
      disconnected <- isTRUE(tryCatch(
        {
          DBI::dbDisconnect(con, shutdown = shutdown)
          TRUE
        },
        error = function(e) FALSE,
        warning = function(w) FALSE
      ))
    }
  }

  if (!isTRUE(disconnected)) {
    return(FALSE)
  }

  owner$closed <- TRUE
  owner$con <- NULL
  owner$session_temp_dir <- NULL

  if (!is.null(con)) {
    try(attr(con, "nrd_owner_env") <- NULL, silent = TRUE)
  }

  if (is.character(session_temp_dir) && length(session_temp_dir) == 1 &&
    nzchar(session_temp_dir) && dir.exists(session_temp_dir)) {
    try(unlink(session_temp_dir, recursive = TRUE, force = TRUE), silent = TRUE)
  }

  TRUE
}
