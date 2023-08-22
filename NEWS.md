
# `hdd` version 0.1.1 (2023-08-21)

## User visible changes

 - `txt2hdd`: argument path now accepts vectors and even patterns. All files matching the pattern will be imported to the `hdd` format.

## Minor bug fixes

 - Fixed a small bug in the function `guess_col_types` when the data contained dates.
 
## Other

- update the maintainer's email address

# `hdd` version 0.1.0 (06-11-2019)

## First version

- This package is an effort to provide a simple way to deal with out of memory data sets within R. The tasks \pkg{hdd} does is to import and perfom simple manipulations: like subsetting, creating new variables, etc.
- As in other data base management systems (DBMS), the data is split into several chunks that fit in memory, and operations are performed "chunk-wise". These operations are hidden to the user.
- Why creating this package? First, it avoids having to switch to alternative software like DBMS to deal with out of memory data sets (you can also use DBMS in R but it is cumbersome). Second, it allows the user to use the full power of R to create complex new variables. Finally, and maybe most importantly, the syntax is very simple and requires little effort from the user.
