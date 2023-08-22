# hdd: Out of memory data manipulation

Say you're a `R` user and have this 150GB text file containing a great data set, critical for your research. This too large for memory file can hardly be worked with in `R` and you don't want to invest time into a data base management system. Scratching your head, you're wondering what to do... That's when the package `hdd` kicks in!

`hdd` offers a simple way to deal with out of memory data sets in `R`. The main functions are `txt2hdd` for importation and the method `[.hdd` for manipulation of the data in a similar way to [data.table](https://github.com/Rdatatable/data.table/wiki).

The idea of `hdd` is very simple and similar to other DBMS: The data sets are located on disk and split into multiple files (chunks), each "workable" in memory. Within `R`, `hdd` data sets look like in-memory and all operations are performed "chunk-wise" behind the scene. No more excuse not to work with large data sets in R!

For a step-by-step example, please see the [walk-through](https://cran.r-project.org/package=hdd/vignettes/hdd_walkthrough.html).
