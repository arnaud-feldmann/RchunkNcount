% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/read_csv_count.R
\name{read_csv_count}
\alias{read_csv_count}
\title{Reads a sas file by chunks and summarize everything}
\usage{
read_csv_count(
  data_file,
  col_select,
  ...,
  row_filter = NULL,
  chunk_size = 10000,
  name = "n",
  weight = NULL,
  col_types = NULL
)
}
\arguments{
\item{data_file}{Path to data and catalog files}

\item{col_select}{the selected columns}

\item{\dots}{some mutate to be done}

\item{row_filter}{the filtering expression}

\item{chunk_size}{the size of the chunks}

\item{name}{the name of the columns for counts}

\item{weight}{a column to be taken as weights for the counts}

\item{col_types}{the column, types, like in  readr.}
}
\value{
a tibble
}
\description{
`read_csv_count` and `read_csv2_count`allows you to wrap the readr functions
in order to make the usual dplyr counts without exploding the ram.
}
\details{
Everything is summarized, grouped by the output columns, and
counted into the variable of name `name` with a weight equal to `weight`.
You can add or modify the columns with the `...` as you would into a
`mutate`, and you can filter the rows with a the argument  `row_filter`
(as you would with `filter`).
}
