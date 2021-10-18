#' @importFrom haven read_sas
#' @author Shaunus McNielsen
read_sas_chunked <- function(..., chunk_size = 10000, callback){

  stopifnot('callback must be a function' = is.function(callback))

  gc()

  skip = 0L
  i = 1L
  data_list <- list()

  n_max = chunk_size

  while (TRUE) {

    current_chunk <- haven::read_sas(..., skip = skip, n_max = n_max)

    eof = nrow(current_chunk) == 0

    if (eof) break

    data_list[[i]] <- callback(current_chunk)

    skip = chunk_size * i
    i = i + 1L
  }

  do.call(rbind.data.frame, data_list)
}

