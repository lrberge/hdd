\name{NEWS}
\alias{NEWS}

\title{News for \R Package \pkg{hdd}}

\section{Changes in version 0.1.0 (2019-10-26)}{
    \subsection{First version!}{
        \itemize{
            \item This package is an effort to provide a simple way to deal with out of memory data sets within R. The tasks \pkg{hdd} does is to import and perfom simple manipulations: like subsetting, creating new variables, etc.
            \item As in other data base management systems (DBMS), the data is split into several chunks that fit in memory, and operations are performed "chunk-wise". These operations are hidden to the user.
            \item Why creating this package? First, it avoids having to switch to alternative software like DBMS to deal with out of memory data sets (you can also use DBMS in R but it is cumbersome). Second, it allows the user to use the full power of R to create complex new variables. Finally, and maybe most importantly, the syntax is very simple and requires little effort from the user.
        }
    }
}

