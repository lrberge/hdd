#' Easy manipulation of out of memory data sets
#'
#' \pkg{hdd} offers a class of data, hard drive data, allowing the easy importation/manipulation of out of memory data sets. The data sets are located on disk but look like in-memory, the syntax for manipulation is similar to \code{\link[data.table]{data.table}}. Operations are performed "chunk-wise" behind the scene.
#'
#' @aliases hdd-package
#'
#' @author Laurent Berge
#'
#' @details
#'
#' The functions for importations is \code{\link[hdd]{txt2hdd}}. The loading of a hdd data set is done with \code{\link[hdd]{hdd}} and the data is extracted with \code{\link[hdd]{sub-.hdd}} which has a \code{\link[data.table]{data.table}} syntax. You can alternatively create a \code{hdd} data set with \code{\link[hdd]{hdd_slice}}. Other utilities include \code{\link[hdd]{hdd_merge}}, or \code{\link[hdd]{peek}} to have a quick look into a text file containing data.
#'
#'
#'
"_PACKAGE"



