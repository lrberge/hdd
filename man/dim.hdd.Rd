% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/core.R
\name{dim.hdd}
\alias{dim.hdd}
\title{Dimension of a HDD object}
\usage{
\method{dim}{hdd}(x)
}
\arguments{
\item{x}{A \code{HDD} object.}
}
\value{
It returns a vector of length 2 containing the number of rows and the number of columns of the HDD object.
}
\description{
Gets the dimension of a hard drive data set (HDD).
}
\examples{

# Toy example with iris data
iris_path = tempfile()
fwrite(iris, iris_path)

# destination path
hdd_path = tempfile()

# reading the text file with 50 rows chunks:
txt2hdd(iris_path, dirDest = hdd_path, rowsPerChunk = 50)

# creating a HDD object
base_hdd = hdd(hdd_path)

# Summary information on the whole data set
summary(base_hdd)

# Looking at it like a regular data.frame
print(base_hdd)
dim(base_hdd)
names(base_hdd)



}
\author{
Laurent Berge
}
