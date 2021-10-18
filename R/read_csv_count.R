#' Reads a sas file by chunks and summarize everything
#'
#' `read_csv_count` and `read_csv2_count`allows you to wrap the readr functions
#' in order to make the usual dplyr counts without exploding the ram.
#'
#' Everything is summarized, grouped by the output columns, and
#' counted into the variable of name `name` with a weight equal to `weight`.
#' You can add or modify the columns with the `...` as you would into a
#' `mutate`, and you can filter the rows with a the argument  `row_filter`
#' (as you would with `filter`).
#'
#' @param data_file Path to data and catalog files
#' @param col_select the selected columns
#' @param \dots some mutate to be done
#' @param row_filter the filtering expression
#' @param chunk_size the size of the chunks
#' @param name the name of the columns for counts
#' @param weight a column to be taken as weights for the counts
#' @param col_types the column, types, like in  readr.
#' @return a tibble
#'
#' @importFrom magrittr `%>%`
#' @importFrom rlang enquo
#' @importFrom rlang quo
#' @importFrom rlang enquos
#' @importFrom rlang ensym
#' @importFrom rlang syms
#' @importFrom rlang sym
#' @importFrom rlang quo_is_null
#' @importFrom readr DataFrameCallback
#' @importFrom dplyr filter
#' @importFrom dplyr mutate
#' @importFrom dplyr group_by
#' @importFrom dplyr across
#' @importFrom dplyr tally
#' @importFrom dplyr ungroup
#' @importFrom readr read_csv_chunked
read_csv_count <- function(data_file,col_select, ..., row_filter = NULL,
                           chunk_size = 10000,name = "n",weight=NULL,
                           col_types = NULL) {

  if (missing(col_select)) stop("col_select must be specified", call. = FALSE)

  col_select <- rlang::enquo(col_select)

  row_filter <- rlang::enquo(row_filter)
  if (rlang::quo_is_null(row_filter)) row_filter <- rlang::quo(TRUE)

  weight <- rlang::enquo(weight)
  if (! rlang::quo_is_null(weight)) weight <- rlang::ensym(weight)

  mutate_quos <- rlang::enquos(...)
  mutate_cols_calculated <- rlang::syms(unname(do.call(c,lapply(mutate_quos,all.vars))))

  callback <- function(x,pos) x %>%
    dplyr::filter(!! row_filter) %>%
    dplyr::mutate(!!! mutate_quos) %>%
    dplyr::group_by(dplyr::across(c(!! col_select,!!! mutate_cols_calculated))) %>%
    dplyr::tally(name = name,wt = !! weight)

  data_file %>%
    readr::read_csv_chunked(DataFrameCallback$new(callback),
                            col_types = col_types) %>%
    dplyr::group_by(c(!! col_select,!!! mutate_cols_calculated)) %>%
    dplyr::tally(name = name,wt = !! rlang::sym(name)) %>%
    dplyr::ungroup()

}

#' @importFrom magrittr `%>%`
#' @importFrom rlang enquo
#' @importFrom rlang enquos
#' @importFrom rlang syms
#' @importFrom rlang sym
#' @importFrom readr DataFrameCallback
#' @importFrom dplyr filter
#' @importFrom rlang quo_is_null
#' @importFrom dplyr mutate
#' @importFrom dplyr group_by
#' @importFrom dplyr tally
#' @importFrom dplyr ungroup
#' @importFrom readr read_csv_chunked
read_csv2_count <- function(data_file,col_select, ..., row_filter = NULL,
                            chunk_size = 10000,name = "n",weight=NULL,
                            col_types = NULL) {

  if (missing(col_select)) stop("col_select must be specified", call. = FALSE)

  col_select <- rlang::enquo(col_select)

  row_filter <- rlang::enquo(row_filter)
  if (rlang::quo_is_null(row_filter)) row_filter <- rlang::quo(TRUE)

  weight <- enquo(weight)
  if (! rlang::quo_is_null(weight)) weight <- rlang::ensym(weight)

  mutate_quos <- rlang::enquos(...)
  mutate_cols_calculated <- rlang::syms(unname(do.call(c,lapply(mutate_quos,all.vars))))

  callback <- function(x,pos) x %>%
    dplyr::filter(!! row_filter) %>%
    dplyr::mutate(!!! mutate_quos) %>%
    dplyr::group_by(dplyr::across(c(!! col_select,!!! mutate_cols_calculated))) %>%
    dplyr::tally(name = name,wt = !! weight)

  data_file %>%
    readr::read_csv2_chunked(callback = readr::DataFrameCallback$new(callback),
                             col_types = col_types) %>%
    dplyr::group_by(c(!! col_select,!!! mutate_cols_calculated)) %>%
    dplyr::tally(name = name,wt = !! rlang::sym(name)) %>%
    dplyr::ungroup()

}
