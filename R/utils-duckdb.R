# Return the remote DBI connection for a lazy table.
.nrd_remote_con <- function(data) {
  tryCatch(dbplyr::remote_con(data), error = function(e) NULL)
}

# Check whether a lazy table is backed by DuckDB.
.nrd_is_duckdb_lazy <- function(data) {
  if (!inherits(data, "tbl_lazy")) {
    return(FALSE)
  }

  inherits(.nrd_remote_con(data), "duckdb_connection")
}

# Standardize HCUP column-name variants at ingest time.
.nrd_standardize_names <- function(data) {
  rename_map <- c(
    NRD_VisitLink = "NRD_VISITLINK",
    NRD_DaysToEvent = "NRD_DAYSTOEVENT"
  )
  present <- names(rename_map)[names(rename_map) %in% colnames(data)]

  if (length(present) == 0) {
    return(data)
  }

  dplyr::rename(data, !!!stats::setNames(present, rename_map[present]))
}

# Create a unique temp directory for one DuckDB ingest session.
.nrd_make_session_temp_dir <- function() {
  token <- paste(sample(c(letters, 0:9), size = 8, replace = TRUE), collapse = "")
  path <- file.path(nrd_cache_dir(), paste0("easyNRD_", Sys.getpid(), "_", token))
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  path
}

# Apply the required easyNRD DuckDB settings for a new connection.
.nrd_configure_connection <- function(con) {
  temp_dir <- .nrd_make_session_temp_dir()
  temp_dir_sql <- as.character(DBI::dbQuoteString(con, normalizePath(temp_dir, winslash = "/", mustWork = TRUE)))

  DBI::dbExecute(con, paste0("SET temp_directory = ", temp_dir_sql))
  DBI::dbExecute(con, "SET preserve_insertion_order = false")

  temp_dir
}

# Store connection ownership metadata for deterministic cleanup.
.nrd_make_owner <- function(con, temp_dir) {
  owner <- new.env(parent = emptyenv())
  owner$con <- con
  owner$temp_dir <- temp_dir
  owner$closed <- FALSE
  owner
}

# Recover easyNRD ownership metadata from a managed object.
.nrd_get_owner <- function(data) {
  if (is.environment(data) && !is.null(data$con) && !is.null(data$closed)) {
    return(data)
  }

  if (inherits(data, "tbl_lazy")) {
    owner <- attr(data, "nrd_owner", exact = TRUE)
    if (is.environment(owner)) {
      return(owner)
    }

    con <- .nrd_remote_con(data)
    return(.nrd_get_owner(con))
  }

  if (inherits(data, "duckdb_connection")) {
    owner <- attr(data, "nrd_owner", exact = TRUE)
    if (is.environment(owner)) {
      return(owner)
    }
  }

  NULL
}

# Attach ownership metadata to a lazy table and its connection.
.nrd_attach_owner <- function(data, owner) {
  attr(data, "nrd_owner") <- owner

  con <- .nrd_remote_con(data)
  if (inherits(con, "duckdb_connection")) {
    attr(con, "nrd_owner") <- owner
  }

  data
}

# Check whether verbose linkage checkpoint logging is enabled.
.nrd_verbose_enabled <- function() {
  identical(Sys.getenv("EASYNRD_VERBOSE", unset = ""), "1")
}

# Materialize a lazy table, optionally logging checkpoint timing and row count.
.nrd_compute_checkpoint <- function(data, label) {
  start <- proc.time()[["elapsed"]]
  out <- dplyr::compute(data)

  if (.nrd_verbose_enabled()) {
    rows <- out |>
      dplyr::summarise(.nrd_rows = dplyr::n()) |>
      dplyr::collect() |>
      dplyr::pull(.nrd_rows)
    elapsed <- proc.time()[["elapsed"]] - start
    message(sprintf("[%s] rows=%s elapsed=%.3fs", label, rows, elapsed))
  }

  out
}

# Preserve the legacy helper for resolving readmission variable selections.
.nrd_resolve_readmit_vars <- function(data, readmit_vars) {
  vars_quo <- rlang::enquo(readmit_vars)
  if (rlang::quo_is_null(vars_quo)) {
    return(character(0))
  }

  names(tidyselect::eval_select(vars_quo, data = data))
}
