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

# Read one environment variable, distinguishing unset from empty-string values.
.nrd_env_value <- function(name) {
  Sys.getenv(name, unset = NA_character_)
}

# Detect available physical CPU cores for optional DuckDB tuning.
.nrd_available_physical_cores <- function() {
  cores <- tryCatch(parallel::detectCores(logical = FALSE), error = function(e) NA_integer_)

  if (!is.numeric(cores) || length(cores) != 1 || is.na(cores) || cores < 1) {
    return(1L)
  }

  as.integer(cores)
}

# Validate and resolve the optional easyNRD DuckDB threads setting.
.nrd_resolve_max_threads <- function() {
  env_value <- .nrd_env_value("EASYNRD_MAX_THREADS")
  if (!is.na(env_value)) {
    value <- env_value
    source_name <- "EASYNRD_MAX_THREADS"
  } else {
    option_value <- getOption("easynrd.max_threads")
    if (is.null(option_value)) {
      return(NULL)
    }
    value <- option_value
    source_name <- "easynrd.max_threads"
  }

  if (is.character(value)) {
    if (length(value) != 1 || is.na(value) || !nzchar(value) || !grepl("^[0-9]+$", value)) {
      rlang::abort(sprintf("`%s` must be a single positive integer.", source_name))
    }
    value <- suppressWarnings(as.integer(value))
  } else if (is.numeric(value)) {
    if (length(value) != 1 || is.na(value) || value < 1 || value != as.integer(value)) {
      rlang::abort(sprintf("`%s` must be a single positive integer.", source_name))
    }
    value <- as.integer(value)
  } else {
    rlang::abort(sprintf("`%s` must be a single positive integer.", source_name))
  }

  if (is.na(value) || value < 1L) {
    rlang::abort(sprintf("`%s` must be a single positive integer.", source_name))
  }

  min(value, .nrd_available_physical_cores())
}

# Validate and resolve the optional easyNRD DuckDB memory limit setting.
.nrd_resolve_memory_limit <- function() {
  env_value <- .nrd_env_value("EASYNRD_MEMORY_LIMIT")
  if (!is.na(env_value)) {
    if (!nzchar(env_value)) {
      rlang::abort("`EASYNRD_MEMORY_LIMIT` must be a single non-empty string.")
    }
    return(env_value)
  }

  option_value <- getOption("easynrd.memory_limit")
  if (is.null(option_value)) {
    return(NULL)
  }

  if (!is.character(option_value) || length(option_value) != 1 || is.na(option_value) || !nzchar(option_value)) {
    rlang::abort("`easynrd.memory_limit` must be a single non-empty string.")
  }

  option_value
}

# Apply the required easyNRD DuckDB settings for a new connection.
.nrd_configure_connection <- function(con) {
  temp_dir <- .nrd_make_session_temp_dir()
  temp_dir_sql <- as.character(DBI::dbQuoteString(con, normalizePath(temp_dir, winslash = "/", mustWork = TRUE)))

  DBI::dbExecute(con, paste0("SET temp_directory = ", temp_dir_sql))
  DBI::dbExecute(con, "SET preserve_insertion_order = false")

  max_threads <- .nrd_resolve_max_threads()
  if (!is.null(max_threads)) {
    DBI::dbExecute(con, paste0("SET threads = ", max_threads))
  }

  memory_limit <- .nrd_resolve_memory_limit()
  if (!is.null(memory_limit)) {
    memory_limit_sql <- as.character(DBI::dbQuoteString(con, memory_limit))
    DBI::dbExecute(con, paste0("SET memory_limit = ", memory_limit_sql))
  }

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
