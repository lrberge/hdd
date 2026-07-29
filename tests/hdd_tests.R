

library(hdd)
test = hdd:::test

####
#### [.hdd ####
####

dir_iris = tempdir()
write_hdd(iris, dir = dir_iris, rowsPerChunk = 100)

iris_hdd = hdd(dir_iris)

# with = FALSE
# .()
# by
# .(a = var_in_env, othervar)
# `file` present or absent
# destDir present or absent
# return dt return vector
# index: flat number / contains variables / missing

dt = iris_hdd[]
test(nrow(dt), nrow(iris))
test(ncol(dt), ncol(iris))

dt = iris_hdd[1:5]
test(nrow(dt), 5)

dt = iris_hdd[1:5, .(avg_sl = mean(Sepal.Length)), by = Species]
test(nrow(dt), 1)
test(names(dt), c("Species", "avg_sl"))

vars = c("Species", "Sepal.Length")
dt = iris_hdd[1:5, vars, with = FALSE]
test(names(dt), vars)

vec = iris_hdd[, Species]
test(length(vec), 150)
test(is.factor(vec), TRUE)

fun_nested = function(){
  my_species = "versicolor"
  iris_hdd[Species %in% my_species]
}

dt = fun_nested()
test(nrow(dt), 50)

dir_agg = file.path(tempdir(), "iris_agg")
iris_hdd[, .(avg_sl = mean(Sepal.Length)), by = Species, destDir = dir_agg]

dt = hdd(dir_agg)
test(dim(dt), c(4, 2))
