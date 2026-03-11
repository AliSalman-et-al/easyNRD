test_that("nrd_cleanup removes orphaned easyNRD directories", {
  tmp <- tempfile("nrd_cleanup_orphan_")
  dir.create(tmp, recursive = TRUE)
  on.exit(unlink(tmp, recursive = TRUE, force = TRUE), add = TRUE)

  dead_dir <- file.path(tmp, "easyNRD_111111")
  alive_dir <- file.path(tmp, paste0("easyNRD_", Sys.getpid()))
  keep_dir <- file.path(tmp, "other_cache")

  dir.create(dead_dir)
  dir.create(alive_dir)
  dir.create(keep_dir)

  removed <- testthat::with_mocked_bindings(
    nrd_cleanup(temp_directory = tmp, force = FALSE),
    .nrd_pid_is_alive = function(pid) {
      if (pid == 111111L) {
        return(FALSE)
      }
      TRUE
    },
    .package = "easyNRD"
  )

  expect_identical(removed, 1L)
  expect_false(dir.exists(dead_dir))
  expect_true(dir.exists(alive_dir))
  expect_true(dir.exists(keep_dir))
})

test_that("nrd_cleanup force mode removes all easyNRD directories", {
  tmp <- tempfile("nrd_cleanup_force_")
  dir.create(tmp, recursive = TRUE)
  on.exit(unlink(tmp, recursive = TRUE, force = TRUE), add = TRUE)

  dir_a <- file.path(tmp, "easyNRD_1")
  dir_b <- file.path(tmp, "easyNRD_2")
  keep_dir <- file.path(tmp, "manual_folder")

  dir.create(dir_a)
  dir.create(dir_b)
  dir.create(keep_dir)

  removed <- nrd_cleanup(temp_directory = tmp, force = TRUE)

  expect_identical(removed, 2L)
  expect_false(dir.exists(dir_a))
  expect_false(dir.exists(dir_b))
  expect_true(dir.exists(keep_dir))
})
