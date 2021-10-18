#' Reads a sas file by chunks and summarize everything
#'
#' `read_sas_count` allows you to wrap `read_sas` in order to make the usual
#' dplyr counts without exploding the ram.
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
#' @return a tibble
#' @importFrom magrittr `%>%`
#' @importFrom rlang enquo
#' @importFrom rlang quo
#' @importFrom rlang enquos
#' @importFrom rlang ensym
#' @importFrom rlang syms
#' @importFrom rlang sym
#' @importFrom dplyr filter
#' @importFrom dplyr mutate
#' @importFrom dplyr group_by
#' @importFrom dplyr across
#' @importFrom dplyr tally
#' @importFrom dplyr ungroup
#' @importFrom readr read_csv_chunked
read_sas_count <- function(data_file,col_select, ..., row_filter = NULL,
                           chunk_size = 10000,name = "n",weight=NULL) {

  if (missing(col_select)) stop("col_select must be specified", call. = FALSE)

  col_select <- rlang::enquo(col_select)

  row_filter <- rlang::enquo(row_filter)
  if (rlang::quo_is_null(row_filter)) row_filter <- rlang::quo(TRUE)

  weight <- rlang::enquo(weight)
  if (! rlang::quo_is_null(weight)) weight <- rlang::ensym(weight)

  mutate_quos <- rlang::enquos(...)
  mutate_cols_calculated <- rlang::syms(names(mutate_quos))
  mutate_cols_needed <- rlang::syms(unname(do.call(c,lapply(mutate_quos,all.vars))))

  callback <- function(tbl) tbl %>%
    dplyr::filter(!! row_filter) %>%
    dplyr::mutate(!!! mutate_quos) %>%
    dplyr::group_by(across(c(!! col_select,!!! mutate_cols_calculated))) %>%
    dplyr::tally(name = name,wt = !! weight)

  data_file %>%
    read_sas_chunked(callback = callback,
                     col_select = c(!! weight,
                                    !!! mutate_cols_needed,
                                    !! col_select,
                                    !!! rlang::syms(all.vars(row_filter)))) %>%
    dplyr::group_by(dplyr::across(c(!! col_select,!!! mutate_cols_calculated))) %>%
    dplyr::tally(name = name,wt = !! rlang::sym(name)) %>%
    dplyr::ungroup()

}
