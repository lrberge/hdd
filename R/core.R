#----------------------------------------------#
# Author: Laurent Berge                        #
# Date creation: Mon Apr 01 11:43:06 2019      #
# Purpose: HDD suite functions                 #
#----------------------------------------------#

# roxygen2::roxygenise(roclets = "rd")

#-----------------------------------------------------#
# Internal variables:                                 #
# .nrow, .row_cum, .ncol, .size, .size_cum, .fileName #
#_____________________________________________________#



#' Read fst or HDD files as DT
#'
#' This is the function \code{\link[fst]{read_fst}} but with automatic conversion to data.table. It also allows to read \code{hdd} data.
#'
#' @inherit hdd seealso
#'
#' @param path Path to \code{fst} file -- or path to \code{hdd} data. For hdd files, there is a
#' @param columns Column names to read. The default is to read all columns. Ignored for \code{hdd} files.
#' @param from Read data starting from this row number. Ignored for \code{hdd} files.
#' @param to Read data up until this row number. The default is to read to the last row of the stored data set. Ignored for \code{hdd} files.
#' @param confirm If the HDD file is larger than ten times the variable \code{getHdd_extract.cap()}, then by default an error is raised. To anyway read the data, use \code{confirm = TRUE}. You can set the data cap with the function \code{\link[hdd]{setHdd_extract.cap}}, the default being 1GB.
#'
#' @author Laurent Berge
#'
#' @examples
#'
#' # Toy example with the iris data set
#'
#' # writing a hdd file
#' hdd_path = tempfile()
#' write_hdd(iris, hdd_path, rowsPerChunk = 30)
#'
#' # reading the full data in memory
#' base_mem = readfst(hdd_path)
#'
#' # is equivalent to:
#' base_hdd = hdd(hdd_path)
#' base_mem_bis = base_hdd[]
#'
#'
readfst = function(path, columns = NULL, from = 1, to = NULL, confirm = FALSE){
	# it avoids adding as.data.table = TRUE
	# + reads hdd files

	check_arg(path, "character scalar")
	check_arg(from, "integer scalar GE{1}")
	check_arg(to, "null integer scalar GE{1}")
	check_arg(confirm, "logical scalar")

	if(grepl("\\.fst", path)){

		if(!file.exists(path)){
			stop("The file the argument 'path' refers to does not exists.")
		}

		res = read_fst(path, columns, from, to, as.data.table = TRUE)
	} else {

		# Checking the path
		if(grepl("\\.[[:alpha:]]+$", path)){
			if(!file.exists(path)) stop("Argument 'path' points to a non-existing file.")
		} else if(!dir.exists(path)){
			stop("Argument 'path' points to a non-existing directory.")
		}

		res = try(hdd(path), silent = TRUE)
		if("try-error" %in% class(res)){
			stop("Argument path must be either a fst file (this is not the case), either a HDD folder. Using path as hdd raises an error:\n", res)
		}

		mc = match.call()
		qui_pblm = intersect(names(mc), c("columns", "from", "to"))
		if(length(qui_pblm) > 0){
			stop("When 'path' leads to a HDD file, the full data set is read. Thus the argument", enumerate_items(qui_pblm, "s.is"), " ignored: for sub-selections use hdd(path) instead.")
		}

		res_size = object_size(res) / 1e6
		size_cap = getHdd_extract.cap() * 10
		if(res_size > size_cap && isFALSE(confirm)){
			stop("Currently the size of the hdd data is ", numberFormat(res_size), "MB which exceeds the cap of ", signif_plus(size_cap), "MB. Please use argument 'confirm' to proceed.")
		}

		res = res[]
	}

	res
}

#' Applies a function to slices of data to create a HDD data set
#'
#' This function is useful to apply complex R functions to large data sets (out of memory). It slices the input data, applies the function, then saves each chunk into a hard drive folder. This can then be a HDD data set.
#'
#' @inherit hdd seealso
#'
#' @param x A data set (data.frame, HDD).
#' @param fun A function to be applied to slices of the data set. The function must return a data frame like object.
#' @param chunkMB The size of the slices, default is 500MB. That is: the function \code{fun} is applied to each 500Mb of data \code{x}. If the function creates a lot of additional information, you may want this number to go down. On the other hand, if the function reduces the information you may want this number to go up. In the end it will depend on the amount of memory available.
#' @param rowsPerChunk Integer, default is missing. Alternative to the argument \code{chunkMB}. If provided, the functions will be applied to chunks of \code{rowsPerChunk} of \code{x}.
#' @param dir The destination directory where the data is saved.
#' @param replace Whether all information on the destination directory should be erased beforehand. Default is \code{FALSE}.
#' @param verbose Integer, defaults to 1. If greater than 0 then the progress is displayed.
#' @param ... Other parameters to be passed to \code{fun}.
#'
#' @author Laurent Berge
#'
#' @details
#' This function splits the original data into several slices and then apply a function to each of them, saving the results into a HDD data set.
#'
#' You can perform merging operations with \code{hdd_slice}, but for regular merges not that you have the function \code{\link[hdd]{hdd_merge}} that may prove more convenient (not need to write a ad hoc function).
#'
#' @return
#' It doesn't return anything, the output is a "hard drive data" saved in the hard drive.
#'
#' @examples
#'
#' # Toy example with iris data.
#' # Say you want to perform a cartesian merge
#' # If the results of the function is out of memory
#' # you can use hdd_slice (not the case for this example)
#'
#' # preparing the cartesian merge
#' iris_bis = iris
#' names(iris_bis) = c(paste0("x_", 1:4), "species_bis")
#'
#'
#' fun_cartesian = function(x){
#' 	# Note that x is treated as a data.table
#' 	# => we need the argument allow.cartesian
#' 	merge(x, iris_bis, allow.cartesian = TRUE)
#' }
#'
#' hdd_result = tempfile() # => folder where results are saved
#' hdd_slice(iris, fun_cartesian, dir = hdd_result, rowsPerChunk = 30)
#'
#' # Let's look at the result
#' base_hdd = hdd(hdd_result)
#' summary(base_hdd)
#' head(base_hdd)
#'
#'
#'
hdd_slice = function(x, fun, dir, chunkMB = 500, rowsPerChunk, replace = FALSE, verbose=1, ...){
	# This function is useful for performing memory intensive operations
	# it slices the operation in several chunks of the initial data
	# then you need to use the function recombine to obtain the result
	# x: the main vector/matrix to which apply fun
	# fun: the function to apply to x
	# chunkMB: the size of the chunks of x, in mega bytes // default is a "smart guess"
	# dir: the repository where to make the temporary savings. Default is "."


	mc = match.call()

	# Controls

	if(missing(x)){
		stop("Argument 'x' is missing but is required.")
	}

	if(missing(fun)){
		stop("Argument 'fun' is missing but is required.")
	} else if(!is.function(fun)){
		stop("Argument 'fun' must be a function. Currently its class is ", class(fun)[[1]], ".")
	}

	check_arg(dir, "character scalar mbt")
	check_arg(chunkMB, "numeric scalar GT{0}")
	check_arg(rowsPerChunk, "integer scalar GE{1}")
	check_arg(replace, "logical scalar")

	if(is.null(dim(x))){
		isTable = FALSE
		n = length(x)
	} else {
		isTable = TRUE
		n = nrow(x)
	}

	# Determining the number of chunks
	if(!missing(rowsPerChunk)){
		if("chunkMB" %in% names(mc)) warning("The value of argument 'chunkMB' is neglected since argument 'rowsPerChunk' is provided.")

		if(rowsPerChunk > 1e9){
			stop("The value of argument 'rowsPerChunk' cannot exceed 1 billion.")
		}

		n_chunks = ceiling(nrow(x) / rowsPerChunk)
	} else {
		if(class(x)[1] == "fst_table"){
			# we estimate the size of x
			n2check = min(1000, ceiling(nrow(x) / 10))
			size_x_subset = as.numeric(object.size(x[1:n2check, ]) / 1e6) # in MB
			size_x = size_x_subset * nrow(x) / n2check
		} else {
			size_x = as.numeric(object_size(x) / 1e6) # in MB
		}

		n_chunks = ceiling(size_x / chunkMB)
	}


	if(n_chunks == 1){
		message("Only one chunk: Function hdd_slice() is not needed.")
	}

	start = floor(seq(1, n, by = n/n_chunks))
	start = start[1:n_chunks]
	end = c(start[-1] - 1, n)

	# The directory
	dir = clean_path(dir)
	dir = gsub("/?$", "/", dir)

	if(!dir.exists(dir)){
		dir.create(dir)
	}

	# cleaning (if necessary)
	all_files = clean_path(list.files(dir, full.names = TRUE))
	all_files2clean = all_files[grepl("/(slice_[[:digit:]]+\\.fst|_hdd\\.txt|info\\.txt)$", all_files)]
	if(length(all_files2clean) > 0){
		if(!replace) stop("The destination diretory contains existing information. To replace it use argument replace=TRUE.")
		for(fname in all_files2clean) unlink(fname)
	}

	# writing the information file
	call = match.call()
	info = c(deparse(call), paste0("CHUNK: ", n_chunks, " chunks of ", round(chunkMB), "MB."))
	writeLines(info, paste0(dir, "info.txt"))


	n_digits = ceiling(log10(n_chunks)) + (log10(n_chunks) %% 1 == 0)

	ADD = FALSE
	call_txt = deparse_long(match.call())

	# The main loop
	for(i in 1:n_chunks){
		if(verbose > 0) message(i, "..", appendLF = FALSE)

		if(isTable){
			x_small = x[start[i]:end[i], ]
		} else {
			x_small = x[start[i]:end[i]]
		}

		res_small = fun(x_small, ...)

		if(!is.data.frame(res_small)){
			res_small = as.data.table(res_small)
		}

		# we save the result in the temporary repository
		if(nrow(res_small) > 0){
			write_hdd(res_small, dir, add = ADD, replace = TRUE, chunkMB = Inf, call_txt = call_txt)
			ADD = TRUE
		}


	}

	if(verbose > 0) message("end.")
}



#' Hard drive data set
#'
#' This function connects to a hard drive data set (HDD). You can access the hard drive data in a similar way to a \code{data.table}.
#'
#' @param dir The directory where the hard drive data set is.
#'
#' @author Laurent Berge
#'
#' @details
#' HDD has been created to deal with out of memory data sets. The data set exists in the hard drive, split in multiple files -- each file being workable in memory.
#'
#' You can perform extraction and manipulation operations as with a regular data set with \code{\link[hdd]{sub-.hdd}}. Each operation is performed chunk-by-chunk behind the scene.
#'
#' In terms of performance, working with complete data sets in memory will always be faster. This is because read/write operations on disk are order of magnitude slower than read/write in memory. However, this might be the only way to deal with out of memory data.
#'
#' @seealso
#' See \code{\link[hdd]{hdd}}, \code{\link[hdd]{sub-.hdd}} and \code{\link[hdd]{cash-.hdd}} for the extraction and manipulation of out of memory data. For importation of HDD data sets from text files: see \code{\link[hdd]{txt2hdd}}.
#'
#' See \code{\link[hdd]{hdd_slice}} to apply functions to chunks of data (and create HDD objects) and \code{\link[hdd]{hdd_merge}} to merge large files.
#'
#' To create/reshape HDD objects from memory or from other HDD objects, see \code{\link[hdd]{write_hdd}}.
#'
#' To display general information from HDD objects: \code{\link[hdd]{origin}}, \code{\link[hdd]{summary.hdd}}, \code{\link[hdd]{print.hdd}}, \code{\link[hdd]{dim.hdd}} and \code{\link[hdd]{names.hdd}}.
#'
#' @examples
#'
#' # Toy example with iris data
#' iris_path = tempfile()
#' fwrite(iris, iris_path)
#'
#' # destination path
#' hdd_path = tempfile()
#'
#' # reading the text file with 50 rows chunks:
#' txt2hdd(iris_path, dirDest = hdd_path, rowsPerChunk = 50)
#'
#' # creating a HDD object
#' base_hdd = hdd(hdd_path)
#'
#' # Summary information on the whole data set
#' summary(base_hdd)
#'
#' # Looking at it like a regular data.frame
#' print(base_hdd)
#' dim(base_hdd)
#' names(base_hdd)
#'
#'
#'
hdd = function(dir){
	# Rd Note: The example section is used in summary/print/names/dim

	# This function creates a link to a repository containing fst files
	# NOTA: The HDD files are all named "sliceXX.fst"

	check_arg(dir, "character scalar mbt")
	dir = clean_path(dir)

	# The directory + prefix
	if(grepl("\\.fst$", dir)) {
		# we get the directory where the file is
		dir = gsub("/[^/]+$", "/", dir)
	} else {
		# regular directory: we add / at the end
		dir = gsub("/?$", "/", dir)
	}

	if(!dir.exists(dir)){
		stop("In argument 'dir': The directory ", dir, " does not exists.")
	}

	# all_files: valid files containing data: i.e. dir/slice_xx.fst
	all_files = clean_path(list.files(dir, full.names = TRUE))
	all_files = sort(all_files[grepl("/slice_[[:digit:]]+\\.fst$", all_files)])

	if(length(all_files) == 0){
		stop("The current directory is not a valid HDD data set (i.e. no HDD files in it).")
	}

	# we gather the information from the files:
	all_sizes = as.vector(sapply(all_files, function(x) file.info(x)$size))
	all_row = as.vector(sapply(all_files, function(x) nrow(fst(x))))
	all_col = as.vector(sapply(all_files, function(x) ncol(fst(x))))

	if(!all(diff(all_col) == 0)){
		i = which(all_col != all_col[1])[1]
		warning("Consistency problem: File 1 has ", all_col[1], " columns while File ", i, " has ", all_col[i], ".")
	}

	info_files = data.table(.nrow = all_row, .row_cum = cumsum(all_row), .ncol = all_col, .size = all_sizes, .size_cum = cumsum(all_sizes), .fileName = all_files)

	# class(info_files) = c("hdd", "data.table", "data.frame")

	# the result is the first 5 rows!
	res = read_fst(info_files$.fileName[1], to = 5, as.data.table = TRUE)

	infoFile = paste0(dir, "_hdd.txt")
	if(file.exists(infoFile)){
		info = readLines(infoFile)
		key = strsplit(info[2], "\t")[[1]]

		if(key[2] != "NA"){
			attr(res, "key") = key[-1]
		}
	} else {
		info = c("hdd file", "key\tNA", paste0(numberFormat(sum(all_row)), " rows and ", ncol(res), " variables."), "\n", paste0(names(res), collapse= "\t"), apply(res, 1, function(x) paste0(x, collapse = "\t")), "\n",  "log:", "? (original file did not have _hdd.txt file)")
		writeLines(info, con = infoFile)
	}

	# return(info_files)

	setattr(res, "class", c("hdd", "data.table", "data.frame"))
	setattr(res, "meta", info_files)

	res
}



#' Extraction of HDD data
#'
#' This function extract data from HDD files, in a similar fashion as data.table but with more arguments.
#'
#' @inherit hdd seealso
#'
#' @param x A hdd file.
#' @param index An index, you can use \code{.N} and variable names, like in data.table.
#' @param ... Other components of the extraction to be passed to \code{\link[data.table]{data.table}}.
#' @param file Which file to extract from? (Remember hdd data is split in several files.) You can use \code{.N}.
#' @param all.vars Logical, default is \code{FALSE}. By default, if the first argument of \code{...} is provided (i.e. argument \code{j}) then only variables appearing in all \code{...} plus the variable names found in \code{index} are extracted. If \code{TRUE} all variables are extracted before any selection is done. (This can be useful when the algorithm getting the variable names gets confused in case of complex queries.)
#' @param newfile A destination directory. Default is missing. Should be result of the query be saved into a new HDD directory? Otherwise, it is put in memory.
#' @param replace Only used if argument \code{newfile} is not missing: default is \code{FALSE}. If the \code{newfile} points to an existing HDD data, then to replace it you must have \code{replace = TRUE}.
#'
#' @author Laurent Berge
#'
#' @details
#' The extraction of variables look like a regular \code{data.table} extraction but in fact all operations are made chunk-by-chunk behind the scene.
#'
#' The extra arguments \code{file}, \code{newfile} and \code{replace} are added to a regular \code{\link[data.table]{data.table}} call. Argument \code{file} is used to select the chunks, you can use the special variable \code{.N} to identify the last chunk.
#'
#' By default, the operation loads the data in memory. But if the expected size is still too large, you can use the argument \code{newfile} to create a new HDD data set without size restriction. If a HDD data set already exists in the \code{newfile} destination, you can use the argument \code{replace=TRUE} to override it.
#'
#' @return
#' Returns a data.table extracted from a HDD file (except if newwfile is not missing).
#'
#' @examples
#'
#' # Toy example with iris data
#'
#' # First we create a hdd data set to run the example
#' hdd_path = tempfile()
#' write_hdd(iris, hdd_path, rowsPerChunk = 40)
#'
#' # your data set is in the hard drive, in hdd format already.
#' data_hdd = hdd(hdd_path)
#'
#' # summary information on the whole file:
#' summary(data_hdd)
#'
#' # You can use the argument 'file' to subselect slices.
#' # Let's have some descriptive statistics of the first slice of HDD
#' summary(data_hdd[, file = 1])
#'
#' # It extract the data from the first HDD slice and
#' # returns a data.table in memory, we then apply summary to it
#' # You can use the special argument .N, as in data.table.
#'
#' # the following query shows the first and last lines of
#' # each slice of the HDD data set:
#' data_hdd[c(1, .N), file = 1:.N]
#'
#' # Extraction of observations for which the variable
#' # Petal.Width is lower than 0.1
#' data_hdd[Petal.Width < 0.2, ]
#'
#' # You can apply data.table syntax:
#' data_hdd[, .(pl = Petal.Length)]
#'
#' # and create variables
#' data_hdd[, pl2 := Petal.Length**2]
#'
#' # You can use the by clause, but then
#' # the by is applied slice by slice, NOT on the full data set:
#' data_hdd[, .(mean_pl = mean(Petal.Length)), by = Species]
#'
#' # If the data you extract does not fit into memory,
#' # you can create a new HDD file with the argument 'newfile':
#' hdd_path_new = tempfile()
#' data_hdd[, pl2 := Petal.Length**2, newfile = hdd_path_new]
#' # check the result:
#' data_hdd_bis = hdd(hdd_path_new)
#' summary(data_hdd_bis)
#' print(data_hdd_bis)
#'
"[.hdd" = function(x, index, ..., file, newfile, replace = FALSE, all.vars = FALSE){
	# newfile: creates a new hdd

	# We look at what variables to select, because it is costly to extract variables: we need the minimum!
	var_names = names(x)
	mc = match.call()
	call_txt = deparse_long(mc)

	# check_arg(file, "integer vector")
	check_arg(newfile, "character scalar", .message = "Argument 'newfile' must be a valid path to a directory.")
	check_arg(replace, "logical scalar")
	check_arg(all.vars, "logical scalar")

	mc_small = mc[!names(mc) %in% c("x", "index", "file", "all.vars")]
	if(all.vars || length(mc_small) == 1){
		# I do that because in what follows, it is just a guess,
		# if I rename a variable, I am screwed
		var2select = var_names
	} else {
		# variables in the dt call + index!
		names_all = unlist(lapply(mc_small[2:length(mc_small)], all.names))
		if(any(var_names %in% names_all)){
			var2select = intersect(var_names, names_all)
			if(!missing(index)){
				index_names = all.names(mc$index)
				var2select = unique(c(var2select, intersect(var_names, index_names)))
			}
		} else {
			var2select = var_names
		}
	}

	# To handle evaluation problems
	useDoCall = FALSE
	if("with" %in% names(mc_small)){
		args = list(x = NA, ...)
		if(!args$with){
			useDoCall = TRUE
			if(!"j" %in% names(args) && any(names(args) == "")) names(args)[which(names(args) == "")[1]] = "j"
		}
	}

	# if(any(c("by", "keyby") %in% names(mc))){
	# 	clause = ifelse("keyby" %in% names(mc), "keyby", "by")
	#
	# 	if(!is.null(attr(x, "key"))){
	# 		key = attr(x, "key")
	# 		vars_by = as.character(mc[[clause]])[-1]
	# 		if(length(vars_by) == length(key) && all(vars_by == key)){
	# 			# fine
	# 		} else if(length(vars_by) == 1 && vars_by == key[1]) {
	# 			# fine (I take that case into account in hdd_setkey())
	# 		} else {
	# 			message("Note that the '", clause, "' clause is applied chunk by chunk, this is not a '", clause, "' on the whole data set. Currently the key", enumerate_items(key, "s.is.start"), " while the ", clause, " clause requires ", enumerate_items(vars_by), ". You may have to re-run hdd_setkey().")
	# 		}
	# 	} else {
	# 		message("Note that the '", clause, "' clause is applied chunk by chunk, this is not a '", clause, "' on the whole data set. To have a result on the 'whole' data set, the data must be sorted beforehand with hdd_setkey() on the appropriate key.")
	# 	}
	#
	# }

	doWrite = FALSE
	if(!missing(newfile)){
		# we save in a new document
		doWrite = TRUE

		dir = clean_path(newfile)
		if(grepl("\\.fst$", dir)) {
			# we get the directory where the file is
			dir = gsub("/[^/]+$", "/", dir)
		} else {
			# regular directory: we add / at the end
			dir = gsub("([^/])$", "\\1/", dir)
		}
	}

	if(!missing(file)){
		# special treatment!
		nfiles_max = length(x$.row_cum)
		file_nb = eval(mc$file, list(.N = nfiles_max), enclos = parent.frame())
		if(!is.numeric(file_nb) || !checkVector(file_nb)){
			stop("The argument 'file' must be an integer vector.")
		} else if(any(file_nb > nfiles_max | file_nb < 1)){
			stop("The argument 'file' must be an integer vector with values from 1 to ", nfiles_max, " (you can use .N).")
		}

		# we check whether the index is relative or not
		dt_call = FALSE
		onlyN = FALSE
		if(!missing(index)){
			if(any(var_names %in% all.names(mc$index))){
				# This is (likely) a call to data.table!
				dt_call = TRUE
			} else if(".N" %in% all.names(mc$index)) {
				onlyN = TRUE
			} else if(!is.numeric(index) || !checkVector(index)){
				stop("'index' must be a data.table expresison or a numeric vector.")
			}
		}

		res = list()
		for(i in seq_along(file_nb)){
			fileName = x$.fileName[file_nb[i]]

			if(missing(index)){
				# easy case
				# cat("i = ", i, sep = "")
				p = proc.time()
				x_tmp = read_fst(fileName, as.data.table = TRUE, columns = var2select)
				# cat(", in ", (proc.time()-p)[3], "s.\n", sep = "")
				res[[i]] = x_tmp[, ...]
			} else {
				# now we have to see if it's worth of downloading everything
				if(dt_call){
					# if it's a call to data.table, then we NEED to download everything
					x_tmp = read_fst(fileName, as.data.table = TRUE, columns = var2select)
					index2text = deparse(substitute(index))
					if(length(index2text) > 1){
						index2text = paste0(index2text, collapse = "")
					}
					res[[i]] = eval(parse(text = paste0("x_tmp[", index2text, ", ...]")))
				} else if(onlyN){
					x_tmp = fst(fileName)
					new_index = eval(mc$index, list(.N = nrow(x_tmp)))
					if(ncol(x_tmp) == 1){
						x_tmp = data.table(x_tmp[new_index, ])
						names(x_tmp) = names(x)
					} else {
						x_tmp = x_tmp[new_index, ]
						setDT(x_tmp)
					}

					# res[[i]] = x_tmp[, ...]

					if(useDoCall){
						args$x = x_tmp
						res[[i]] = do.call("[", args)
					} else {
						res[[i]] = x_tmp[, ...]
					}
				} else {
					# we use the fst format to download small chunks
					x_tmp = fst(fileName)

					if(ncol(x_tmp) == 1){
						x_tmp = data.table(x_tmp[index, ])
						names(x_tmp) = names(x)
					} else {
						x_tmp = x_tmp[index, ]
						setDT(x_tmp)
					}

					# res[[i]] = x_tmp[, ...]

					if(useDoCall){
						args$x = x_tmp
						res[[i]] = do.call("[", args)
					} else {
						res[[i]] = x_tmp[, ...]
					}
				}
			}

			if(doWrite){
				if(!is.data.frame(res[[i]])){
					stop("You cannot save to a new file when the outcome of the call is not a data.frame (here it is a vector: use .(var) instead).")
				}

				write_hdd(res[[i]], dir = dir, replace = replace, add = i!=1, call_txt = call_txt)
				res[[i]] = NULL # we clean memory
			}

		}

		if(doWrite) {
			return(invisible(NULL))
		} else if(length(file_nb) == 1){
			return(res[[1]])
		} else {
			if(!is.data.frame(res[[which.max(lengths(res))]])){
				# when what is returned is a vector
				res_all = unlist(res)
			} else {
				res_all = rbindlist(res)
			}
			return(res_all)
		}
	}


	if(missing(index)){
		# isOrder = FALSE
		# index_sorted = 1:nrow(x)
		if(doWrite){
			x[, ..., file = 1:.N, newfile=newfile, replace=replace]
			return(invisible(NULL))
		} else {
			return(x[, ..., file = 1:.N])
		}

	} else if(".N" %in% all.names(mc$index)){
		stop("The variable .N is not supported in 'index' when you do not use argument 'file'.")
	} else if(any(var_names %in% all.names(mc$index))){
		# means it is a dt call
		# we apply it to all files!

		index2text = deparse(substitute(index))
		if(length(index2text) > 1){
			index2text = paste0(index2text, collapse = "")
		}

		if(doWrite){
			res = eval(parse(text = paste0("x[", index2text, ", ..., file = 1:.N, newfile =\"", dir, "\", replace = ", replace, "]")))
			return(invisible(NULL))
		} else {
			res = eval(parse(text = paste0("x[", index2text, ", ..., file = 1:.N]")))
		}

		return(res)
	} else if(is.unsorted(index) || index[1] > tail(index, 1)){
		isOrder = TRUE
		index_order = order(index)
		index_sorted = index[index_order]
	} else {
		isOrder = FALSE
		index_sorted = index
	}

	# Finding out which tables to use
	n_max = tail(x$.row_cum, 1)

	ind_min = min(index_sorted)
	ind_max = max(index_sorted)
	# controls
	if(ind_min < 1){
		stop("indexes must be greater or equal than 1!")
	}
	if(ind_max > n_max){
		stop("The maximum number of lines is ", numberFormat(n_max), ". You cannot provide indexes greater than this number!")
	}

	file_start = which.max(ind_min <= x$.row_cum)
	file_end = which.max(ind_max <= x$.row_cum)

	remaining_index = index_sorted
	res = list()
	i_running = 0
	for(i in file_start:file_end){
		if(remaining_index[1] > x$.row_cum[i]){
			next
		}
		i_running = i_running + 1

		if(tail(remaining_index, 1) <= x$.row_cum[i]){
			current_index = remaining_index
			remaining_index = NULL
		} else {
			current_index = remaining_index[remaining_index <= x$.row_cum[i]]
			remaining_index = remaining_index[-seq_along(current_index)]
		}

		if(i > 1){
			# correction
			current_index = current_index - x$.row_cum[i - 1]
		}

		# Extraction of the data
		if(length(current_index) < x$.row_cum[i]/1000){
			x_tmp = fst(x$.fileName[i])
			if(ncol(x_tmp) == 1){
				x_tmp = data.table(x_tmp[current_index, ])
				names(x_tmp) = names(x)
			} else {
				x_tmp = x_tmp[current_index, ]
				setDT(x_tmp)
			}

			if(useDoCall){
				args$x = x_tmp
				res[[i]] = do.call("[", args)
			} else {
				res[[i]] = x_tmp[, ...]
			}
		} else {
			x_tmp = read_fst(x$.fileName[i], as.data.table = TRUE)

			if(useDoCall){
				args = list(x = x_tmp, i = current_index, ...)
				if(!"j" %in% names(args) && any(names(args) == "")) names(args)[which(names(args) == "")[1]] = "j"
				res[[i]] = do.call("[", args)
			} else {
				res[[i]] = x_tmp[current_index, ...]
			}
		}

		if(doWrite){
			stop("newfile not implemented for 'regular' indexes. Think to the design.")
			write_hdd(res[[i]], dir = dir, replace = replace, add = i_running!=1, call_txt = call_txt)
			res[[i]] = NULL # we clean memory
		}

		if(length(remaining_index) == 0){
			break
		}
	}

	if(!is.data.frame(res[[which.max(lengths(res))]])){
		# when what is returned is a vector
		res_all = unlist(res)
	} else {
		res_all = rbindlist(res)
	}

	# we put the stuff in order
	if(isOrder){
		xxReorderxx = order(index_order)
		res_all = res_all[xxReorderxx]
	}

	res_all
}

#' Extracts a single variable from a HDD object
#'
#' This method extracts a single variable from a hard drive data set (HDD). There is an automatic protection to avoid extracting too large data into memory. The bound is set by the function \code{\link[hdd]{setHdd_extract.cap}}.
#'
#' @inheritParams dim.hdd
#' @inherit hdd seealso
#'
#' @param name The variable name to be extracted.Note that there is an automatic protection for not trying to import data that would not fit into memory. The extraction cap is set with the function \code{\link[hdd]{setHdd_extract.cap}}.
#'
#' @author Laurent Berge
#'
#' @details
#' By default if the expected size of the variable to extract is greater than the value given by \code{\link[hdd]{getHdd_extract.cap}} an error is raised.
#' For numeric variables, the expected size is exact. For non-numeric data, the expected size is a guess that considers all the non-numeric variables being of the same size. This may lead to an over or under estimation depending on the cases.
#' In any case, if your variable is large and you don't want to change the extraction cap (\code{\link[hdd]{setHdd_extract.cap}}), you can still extract the variable with \code{\link[hdd]{sub-.hdd}} for which there is no such protection.
#'
#' Note that you cannot create variables with \code{$}, e.g. like \code{base_hdd$x_new <- something}. To create variables, use the \code{[} instead (see \code{\link[hdd]{sub-.hdd}}).
#'
#' @return
#' It returns a vector.
#'
#' @examples
#'
#' # Toy example with iris data
#' # We first create a hdd dataset with approx. 100KB
#' hdd_path = tempfile() # => folder where the data will be saved
#' write_hdd(iris, hdd_path)
#' for(i in 1:10) write_hdd(iris, hdd_path, add = TRUE)
#'
#' base_hdd = hdd(hdd_path)
#' summary(base_hdd) # => 11 files
#'
#' # we can extract the data from the 11 files with '$':
#' pl = base_hdd$Sepal.Length
#'
#' #
#' # Illustration of the protection mechanism:
#' #
#'
#' # By default you cannot extract a variable with '$'
#' # when its size would be too large (default is greater than 1000MB)
#' # You can set the cap with setHdd_extract.cap.
#'
#' # Following code raises an error:
#' setHdd_extract.cap(sizeMB = 0.005) # new cap of 5KB
#' try(pl <- base_hdd$Sepal.Length)
#'
#' # To extract the variable without changing the cap:
#' pl <- base_hdd[, Sepal.Length] # => no size control is performed
#'
#' # Resetting the default cap
#' setHdd_extract.cap()
#'
"$.hdd" = function(x, name){

	# meta information
	meta_vars = c(".nrow", ".row_cum", ".ncol", ".size", ".size_cum", ".fileName")
	if(name %in% meta_vars){
		y = attr(x, "meta")
		res = y[[name]]
		return(res)
	}

	# real variables
	if(!name %in% names(x)){
		mc = match.call()
		stop(name, " is not a variable of the HDD data ", deparse(mc$x), ".")
	}

	i = which(names(x) == name)

	n_row = nrow(x)
	x_sample = head(x, 5)
	x_num = sapply(x_sample, is.numeric)

	# estimate of the size of the data
	TRUE_SIZE = TRUE
	isNum = FALSE
	if(x_num[i]){
		# numeric: 8 bytes
		isNum = TRUE
		current_size = 8 * n_row
	} else {
		# for non numeric data: we don't know!
		# we have an upper bound
		x_size = object_size(x)
		current_size_non_numeric = x_size - sum(x_num) * 8 * n_row
		if(sum(!x_num) == 1){
			current_size = current_size_non_numeric
		} else {
			TRUE_SIZE = FALSE
			# we consider an even repartition + 50% to be on the safe side
			# NOTA: we could have a much better estimation by extracting
			# 10K observations randomly and then computing the size share of
			# each variable.
			# PBLM: fst is not very efficient at extracting svl distant observations
			# in a single file.
			# Consequence: it can be slow => it is always disappointing from an user
			# perspective to have to wait to get an error message.
			# Thus we sacrifice precision for speed.
			current_size = current_size_non_numeric / sum(!x_num)
			current_size = 1.5 * current_size
		}

	}
	# we tranform in MB
	current_sizeMB = current_size / 1e6

	size_cap = getHdd_extract.cap()
	if(current_sizeMB > size_cap){
		stop("Cannot extract variable ", name, " because its ", ifelse(TRUE_SIZE, "", " approximated "), "size (", addCommas(mysignif(current_sizeMB, r = 0)), " MB) is greater than the cap of ", addCommas(size_cap), " MB. You can change the cap using setHdd_extract.cap(new_cap).")
	}

	# now extraction
	res = eval(parse(text = paste0("x[, ", name, "]")))

	res
}


#' Saves or appends a data set into a HDD file
#'
#' This function saves in-memory/HDD data sets into HDD repositories. Useful to append several data sets.
#'
#' @inherit hdd seealso
#'
#' @param x A data set.
#' @param dir The HDD repository, i.e. the directory where the HDD data is.
#' @param chunkMB If the data has to be split in several files of \code{chunkMB} sizes. Default is \code{Inf}.
#' @param rowsPerChunk Integer, default is missing. Alternative to the argument \code{chunkMB}. If provided, the data will be split in several files of \code{rowsPerChunk} rows.
#' @param compress Compression rate to be applied by \code{\link[fst]{write_fst}}. Default is 50.
#' @param add Should the file be added to the existing repository? Default is \code{FALSE}.
#' @param replace If \code{add = FALSE}, should any existing document be replaced? Default is \code{FALSE}.
#' @param showWarning If the data \code{x} has no observation, then a warning is raised if \code{showWarning = TRUE}. By default, it occurs only if \code{write_hdd} is NOT called within a function.
#' @param ... Not currently used.
#'
#' @author Laurent Berge
#'
#' @details
#' Creating a HDD data set with this function always create an additional file named \dQuote{_hdd.txt} in the HDD folder. This file contains summary information on the data: the number of rows, the number of variables, the first five lines and a log of how the HDD data set has been created. To access the log directly from \code{R}, use the function \code{\link[hdd]{origin}}.
#'
#' @examples
#'
#' # Toy example with iris data
#'
#' # Let's create a HDD data set from iris data
#' hdd_path = tempfile() # => folder where the data will be saved
#' write_hdd(iris, hdd_path)
#' # Let's add data to it
#' for(i in 1:10) write_hdd(iris, hdd_path, add = TRUE)
#'
#' base_hdd = hdd(hdd_path)
#' summary(base_hdd) # => 11 files, 1650 lines, 48.7KB on disk
#'
#' # Let's save the iris data by chunks of 1KB
#' # we use replace = TRUE to delete the previous data
#' write_hdd(iris, hdd_path, chunkMB = 0.001, replace = TRUE)
#'
#' base_hdd = hdd(hdd_path)
#' summary(base_hdd) # => 8 files, 150 lines, 10.2KB on disk
#'
write_hdd = function(x, dir, chunkMB = Inf, rowsPerChunk, compress = 50, add = FALSE, replace = FALSE, showWarning, ...){
	# data: the data (in memory or fst or hdd)
	# dir: the hdd repository
	# write hdd data
	# _hdd.txt => file avec infos
	# variables, head du file, si'il y a une key ou pas
	# we may add a chunk option later on  -- maybe not so useful

	mc = match.call()

	# controls
	check_arg(dir, "character scalar mbt", .message = "Argument 'dir' must be a valid path.")
	check_arg(chunkMB, "numeric scalar GT{0}")
	check_arg(rowsPerChunk, "integer scalar GE{1}")
	check_arg(compress, "integer scalar GE{0} LE{100}")
	check_arg(add, "logical scalar")
	check_arg(replace, "logical scalar")
	check_arg(showWarning, "logical scalar")

	check_arg(x, "data.frame mbt")

	# hidden arguments
	dots = list(...)
	is_ext_call = FALSE
	if("call_txt" %in% names(dots)){
		call_txt = dots$call_txt
		is_ext_call = TRUE
	} else {
		call_txt = deparse_long(mc)
	}

	# if no observation
	if(missing(showWarning)){
		if(sys.nframe() == 1){
			showWarning = TRUE
		} else {
			showWarning = FALSE
		}
	}

	if(nrow(x) == 0){
		if(showWarning){
			warning("No observation in x, nothing is done.")
		}
		return(invisible(NULL))
	}

	# The repository
	dir = clean_path(dir)
	if(grepl("\\.fst$", dir)) {
		# we get the directory where the file is
		dir = gsub("/[^/]+$", "/", dir)
	} else {
		# regular directory: we add / at the end
		dir = gsub("([^/])$", "\\1/", dir)
	}

	file_head = paste0(dir, "slice_")
	dirExists = dir.exists(dir)
	all_files = clean_path(list.files(dir, full.names = TRUE))
	all_files_fst = ggrepl("/slice_[[:digit:]]+\\.fst$", all_files)

	hddExists = dirExists && length(all_files_fst) > 0

	if(hddExists){
		if(!add){
			if(!replace){
				stop("A hdd data set already exists in ", dir, ". To replace it, use replace = TRUE.")
			} else {
				for(fname in all_files_fst) unlink(fname)
			}
		}
	} else {
		add = FALSE
	}

	if(!dirExists){
		dir.create(dir, recursive = TRUE)
	}

	memoryData = FALSE # flag indicating that the data is in memory
	isKey = FALSE # flag of whether the data is sorted => Only for HDD files
	if("fst_table" %in% class(x)){
		file2copy = unclass(unclass(x)$meta)$path
	} else if("hdd" %in% class(x)){
		file2copy = x$.fileName
		if(!is.null(attr(x, "key"))){
			isKey = TRUE
		}
	} else if("data.frame" %in% class(x)){
		memoryData = TRUE
	} else {
		stop("The class of data is not supported. Only data.frame like data sets can be written in hdd.")
	}

	if(nrow(x) == 0) return(invisible(NULL))

	if(add){

		if(isKey) stop("At the moment add = TRUE with x a HDD data base with key is not supported.")

		nb_files_existing = length(all_files_fst)
		# We also check consistency of appending data
		if(hddExists){
			x_exist_head = read_fst(all_files_fst[1], to = 10)
			x_exist_types = sapply(x_exist_head, typeof)
			x_exist_names = names(x_exist_head)

			x_new_head = x[1:min(5, nrow(x)), ]
			x_new_types = sapply(x_new_head, typeof)
			x_new_names = names(x_new_head)

			if(ncol(x_exist_head) != ncol(x_new_head)){
				stop("The number of variables of the data to append does not match the original data: ", ncol(x_new_head), " vs ", ncol(x_exist_head), ".")
			}

			if(!all(x_exist_names == x_new_names)){
				qui_pblm = which(x_exist_names != x_new_names)
				stop("The variables names of the data to append do not match the original data!\nNew: ", paste0(x_new_names[qui_pblm], collapse = ", "), "\nOld: ", paste0(x_exist_names[qui_pblm], collapse = ", "))
			}

			if(!all(x_exist_types == x_new_types)){
				qui_pblm = which(x_exist_types != x_new_types)[1]
				stop("The variables types of the data to append do not match the original data!\nNew: ", x_new_names[qui_pblm], " is ", x_new_types[qui_pblm], ".\nOld: ", x_exist_names[qui_pblm], " is ", x_exist_types[qui_pblm], ".")
			}

		}
	} else {
		nb_files_existing = 0
	}

	# Adding is the same as the not adding, we just have to update the names of the files
	# and start numbering from higher

	if(!missing(rowsPerChunk)){
		if("chunkMB" %in% names(mc)) warning("The value of argument 'chunkMB' is neglected since argument 'rowsPerChunk' is provided.")

		if(rowsPerChunk > 1e9){
			stop("The value of argument 'rowsPerChunk' cannot exceed 1 billion.")
		}

		# we set chunkMB to an arbitraty value => to get into chunking
		chunkMB = 1
	}


	if(memoryData || is.finite(chunkMB)){

		if(!is.finite(chunkMB)){
			# this is memory data
			n_chunks = 1
		} else if(!missing(rowsPerChunk)) {
			n_chunks = ceiling(nrow(x) / rowsPerChunk)
		} else {
			if("fst_table" %in% class(x)){
				# we estimate the size of x
				n2check = min(1000, ceiling(nrow(x) / 10))
				size_x_subset = as.numeric(object.size(x[1:n2check, ]) / 1e6) # in MB
				size_x = size_x_subset * nrow(x) / n2check
			} else {
				size_x = as.numeric(object_size(x) / 1e6) # in MB
			}
			n_chunks = ceiling(size_x / chunkMB)
		}

		n = nrow(x)

		if(isKey && n_chunks > 1){
			key = attr(x, "key")
			start = find_n_split(x, key, n_chunks)
			end = c(start[-1] - 1, n)
		} else {
			start = floor(seq(1, n, by = n/n_chunks))
			start = start[1:n_chunks]
			end = c(start[-1] - 1, n)
		}

		nb_all = nb_files_existing + n_chunks
		n_digits = ceiling(log10(nb_all)) + (log10(nb_all) %% 1 == 0)

		for(i in 1:n_chunks){
			write_fst(x[start[i]:end[i], ], path = paste0(file_head, sprintf("%0*i", n_digits, nb_files_existing + i), ".fst"), compress = compress)
		}
	} else {
		# we just copy the files except there is the need to recreate the files to account for the key
		n_chunks = length(file2copy)

		nb_all = nb_files_existing + n_chunks
		n_digits = ceiling(log10(nb_all)) + (log10(nb_all) %% 1 == 0)
		files_new = paste0(file_head, sprintf("%0*i", n_digits, nb_files_existing+(1:n_chunks)), ".fst")

		# To handle the cases with key
		justCopy = TRUE
		if(isKey && n_chunks > 1){
			key = attr(x, "key")
			first = x[1, key, file = 1:.N, with = FALSE]
			last = x[.N, key, file = 1:.N, with = FALSE]
			if(any(first[-1] == last[-n_chunks])){
				justCopy = FALSE
				start = find_n_split(x, key, n_chunks)
				end = c(start[-1] - 1, sum(x$.nrow))
			}
		}

		if(justCopy){
			for(i in 1:n_chunks){
				file.copy(file2copy[i], files_new[i])
			}
		} else {
			for(i in 1:n_chunks){
				write_fst(x[start[i]:end[i], ], path = files_new[i], compress = compress)
			}
		}
	}

	# Creating an information file
	infoFile = paste0(dir, "_hdd.txt")
	if(!add){

		key = attr(x, "key")
		if(length(key) == 0){
			key_txt = "key\tNA"
		} else {
			key_txt = paste0("key\t", paste0(key, collapse = "\t"))
		}

		# formatting the log message
		if(is_ext_call){
			first_msg = ifelse(n_chunks == 1, "1 file", paste0(n_chunks, " files"))
			log_msg = paste0(first_msg, " ; ", numberFormat(nrow(x)), " rows ; ", call_txt)
		} else {
			first_msg = ifelse(n_chunks == 1, "# 1", paste0("# 1-", n_chunks))
			log_msg = paste0(first_msg, " ; ", numberFormat(nrow(x)), " rows ; ", call_txt)
		}


		info = c("hdd file", key_txt, paste0(numberFormat(nrow(x)), " rows and ", ncol(x), " variables."), "\n", paste0(names(x), collapse= "\t"), apply(head(x, 5), 1, function(x) paste0(x, collapse = "\t")), "\n",  "log:", log_msg)
		writeLines(info, con = infoFile)
	} else {
		# we update the information file AND update the original file names

		# Updating file names
		n_digits_original = ceiling(log10(nb_files_existing)) + (log10(nb_files_existing) %% 1 == 0)
		if(n_digits > n_digits_original){
			new_file_names = paste0(file_head, sprintf("%0*i", n_digits, 1:nb_files_existing), ".fst")
			for(i in 1:nb_files_existing){
				file.rename(all_files_fst[i], new_file_names[i])
			}
		}

		# updating the information file
		if(file.exists(infoFile)){
			info = readLines(infoFile)

			# update key if add = TRUE
			keys = strsplit(info[2], "\t")[[1]]
			if(keys[2] != "NA"){
				info[2] = "key\tNA"
				warning("You need to re-run hdd_setkey() to have the data sorted.")
			}

			# update rows
			x = hdd(dir)
			old_rows = as.numeric(gsub("[^[:digit:]]", "", gsub(" rows.+", "", info[3])))
			info[3] = paste0(numberFormat(nrow(x)), " rows and ", ncol(x), " variables.")

			# update log
			if(is_ext_call){
				# The functions that use ext_call are:
				#	- extract with newfile, hdd_slice, merge_hdd and txt2hdd
				#	- they MUST create a new document
				#	- hence we're sure the last line contains the word "file"
				# we take the last line and update it
				log_msg = paste0(length(x$.nrow), " files ; ", numberFormat(nrow(x)), " rows ; ", call_txt)
				# we replace the line
				info[grepl("^[^;]+files? ;", info)] = log_msg

			} else {
				# We add the line with information on the file nber and nber of rows
				if(n_chunks == 1){
					nb_show = nb_files_existing + 1
				} else {
					nb_show = paste0(nb_files_existing + 1, "-", nb_files_existing + n_chunks)
				}

				log_msg = paste0("# ", nb_show, " ; ", numberFormat(nrow(x) - old_rows), " rows ; ", call_txt)
				info = c(info, log_msg)
				# We DON'T reformat the line numbers because we don't know what's before
				# it is then too risky
			}

			writeLines(info, con = infoFile)
		} else {
			x = hdd(dir) # This automatically writes the info file
			# info = c("hdd file", "key\tNA", paste0(numberFormat(nrow(x)), " rows and ", ncol(x), " variables."), "\n", paste0(names(x), collapse= "\t"), apply(x[1:5, ], 1, function(x) paste0(x, collapse = "\t")), "\n",  "log:", "? (original file did not have _hdd.txt file)",  deparse(mc))
			# writeLines(info, con = infoFile)
		}
	}

}


#' Sorts HDD objects
#'
#' This function sets a key to a HDD file. It creates a copy of the HDD file sorted by the key. Note that the sorting process is very time consuming.
#'
#' @inherit hdd seealso
#' @inheritParams hdd_merge
#'
#' @param x A hdd file.
#' @param key A character vector of the keys.
#' @param chunkMB The size of chunks used to sort the data. Default is 500MB. The bigger this number the faster the sorting is (depends on your memory available though).
#' @param verbose Numeric, default is 1. Whether to display information on the advancement of the algorithm. If equal to 0, nothing is displayed.
#'
#' @details
#' This function is provided for convenience reason: it does the job of sorting the data and ensuring consistency across files, but it is very slow since it involves copying several times the entire data set. To be used parsimoniously.
#'
#' @author Laurent Berge
#'
#'
#' @examples
#'
#' # Toy example with iris data
#'
#' # Creating HDD data to be sorted
#' hdd_path = tempfile() # => folder where the data will be saved
#' write_hdd(iris, hdd_path)
#' # Let's add data to it
#' for(i in 1:10) write_hdd(iris, hdd_path, add = TRUE)
#'
#' base_hdd = hdd(hdd_path)
#' summary(base_hdd)
#'
#' # Sorting by Sepal.Width
#' hdd_sorted = tempfile()
#' # we use a very small chunkMB to show how the function works
#' hdd_setkey(base_hdd, key = "Sepal.Width",
#' 		   newfile = hdd_sorted, chunkMB = 0.010)
#'
#'
#' base_hdd_sorted = hdd(hdd_sorted)
#' summary(base_hdd_sorted) # => additional line "Sorted by:"
#' print(base_hdd_sorted)
#'
#' # Sort with two keys:
#' hdd_sorted = tempfile()
#' # we use a very small chunkMB to show how the function works
#' hdd_setkey(base_hdd, key = c("Species", "Sepal.Width"),
#' 		   newfile = hdd_sorted, chunkMB = 0.010)
#'
#'
#' base_hdd_sorted = hdd(hdd_sorted)
#' summary(base_hdd_sorted)
#' print(base_hdd_sorted)
#'
hdd_setkey = function(x, key, newfile, chunkMB = 500, replace = FALSE, verbose = 1){
	# on va creer une base HDD triee
	# The operation is very simple, so we can use big chunks => much more efficient!

	# for 2+ keys, use : rowidv(DT, cols=c("x", "y"))

	# On va cree un hdd file tmp
	# un autre tmp2
	# an final, on coupera-collera dans newfile

	# key can be a data.table call? No, not at the moment

	check_arg(x, "class(hdd) mbt")
	check_arg_plus(key, "multi match", .choices = names(x), .message = "The key must be a variable name (partial matching on).")
	check_arg(newfile, "character scalar mbt")
	check_arg(chunkMB, "numeric scalar GT{0}")
	check_arg(replace, "logical scalar")
	check_arg(verbose, "numeric scalar")

	newfile = clean_path(newfile)
	if(dir.exists(newfile)){
		# cleaning (if necessary)
		all_files = clean_path(list.files(newfile, full.names = TRUE))
		all_files2clean = all_files[grepl("/(slice_[[:digit:]]+\\.fst|_hdd\\.txt)$", all_files)]
		if(length(all_files2clean) > 0){
			if(!replace) stop("The destination diretory contains existing information. To replace it use argument replace=TRUE.")
			for(fname in all_files2clean) unlink(fname)
		}
	}

	nfiles_origin = length(x$.nrow)

	call_txt = deparse_long(match.call())

	# 1st step: sort all + recreation of all the objects

	# estimation of the size of the data in R:
	# used to recreate the data at an appropriate chunk size
	if(verbose > 0) message("Guessing R size", appendLF = FALSE)
	sample = x[file = 1]
	size_sample = object.size(sample)
	factor = as.numeric(size_sample / x$.size[1])
	r_size = sum(x$.size) * factor / 1e6
	if(verbose > 0) message("...", appendLF = FALSE)
	# cleaning
	rm(sample)

	nfiles = ceiling(r_size / chunkMB)
	n = sum(x$.nrow)

	if(nfiles == 1){
		# memory enough to fit all
		x_all = x[]
		setorderv(x_all, key)
		write_hdd(x_all, dir = newfile, replace = replace)
		if(verbose > 0) message("done.")
		return(invisible(NULL))
	}

	if(verbose > 0) message(nfiles, " files...", appendLF = FALSE)

	start = floor(seq(1, n, by = n/nfiles))
	start = start[1:nfiles]
	end = c(start[-1] - 1, n)

	tmpdir = paste0(gsub("/[^/]+$", "/", x$.fileName[1]), "tmp_hdd_setkey/")
	all_files = clean_path(list.files(tmpdir, full.names = TRUE))
	for(fname in all_files) unlink(fname)

	# flag for warning if key spans multiple files
	WARNED_ALREADY = FALSE

	# creation of the tmp directory + first pair sort
	i = 1
	while(i < nfiles){
		if(verbose > 0) message(".", appendLF = FALSE)
		if(i + 2 == nfiles){
			ij = i:(i+2)
			i = i + 3
			nfiles = nfiles - 1
		} else {
			ij = c(i, i+1)
			i = i + 2
		}

		# x_mem = x[, file = ij]
		start_current = start[ij[1]]
		end_current = end[tail(ij, 1)]
		x_mem = x[start_current:end_current]

		setorderv(x_mem, key)

		obs_mid = find_split(x_mem[, key, with = FALSE])
		if(is.null(obs_mid)){
			if(!WARNED_ALREADY) warning("A single key (", x_mem[1, key, with = FALSE][1], ") spans at least two complete chunks (", nrow(x_mem), " lines). Thus the algorithm cannot ensure there will be only one key value per file. If you really care about ensuring a key does not span multiple files: stop the algorithm now and re-run with a higher value for 'chunkMB' or for 'rowsPerChunk' (to increase the size of the files).", immediate. = TRUE)
			WARNED_ALREADY = TRUE
			obs_mid = ceiling(nrow(x_mem) / 2)
		}

		write_hdd(x_mem[1:obs_mid], tmpdir, add = TRUE)
		write_hdd(x_mem[(obs_mid+1):.N], tmpdir, add = TRUE)
	}

	# finding all the file names
	all_files = clean_path(list.files(tmpdir, full.names = TRUE))
	all_files_fst = ggrepl("\\.fst", all_files)

	changeFileNames = function(new_order){
		tmp_names = paste0(all_files_fst, "_tmp")
		for(i in 1:nfiles) file.rename(from = all_files_fst[i], to = tmp_names[i])
		for(i in 1:nfiles) file.rename(from = tmp_names[new_order[i]], to = all_files_fst[i])
	}

	# Loop:

	x_tmp = hdd(tmpdir)
	nbsort = ceiling(nfiles/2)
	while(TRUE){
		if(verbose > 0) message("+", appendLF = FALSE)

		# I need to use a call, otherwise there are evaluation problems
		# with one key: x_tmp[.N, key, file = 1:.N, with=FALSE] does not work
		# (because [.hdd uses the globally set key variable)
		myCall = parse(text = paste0("x_tmp[c(1, .N), .(", paste0(key, collapse = ", "), "), file = 1:.N]"))
		first_last = numID(eval(myCall))
		first_item = first_last[1 + 2*(0:(nfiles-1))]
		last_item = first_last[2*1:nfiles]

		file_order = order(first_item)

		changeFileNames(file_order)

		first_item_new = first_item[file_order]
		last_item_new = last_item[file_order]

		# we create the pairs to sort
		pairs2sort = list()
		i = 1
		index = 1
		nbsort = 0
		while(i <= nfiles){

			if(i == nfiles){
				pairs2sort[[index]] = i
			} else if(last_item_new[i] > first_item_new[i+1]){
				pairs2sort[[index]] = c(i, i+1)
				i = i + 1
				nbsort = nbsort + 1
			} else {
				pairs2sort[[index]] = i
			}
			index = index + 1
			i = i+1
		}

		if(verbose > 0) message(nbsort, appendLF = FALSE)

		if(all(lengths(pairs2sort) == 1)){
			break
		}

		for(ij in pairs2sort[lengths(pairs2sort) == 2]){
			if(verbose > 0) message(".", appendLF = FALSE)
			x_mem = x_tmp[, file = ij]
			setorderv(x_mem, key)
			# x_mem = x_mem[order(id)]

			# obs_mid = find_split(x_mem[, id])
			obs_mid = find_split(x_mem[, key, with = FALSE])
			if(is.null(obs_mid)){
				if(!WARNED_ALREADY) warning("A single key (", x_mem[1, key, with = FALSE][1], ") spans at least two complete chunks (", nrow(x_mem), " lines). Thus the algorithm cannot ensure there will be only one key value per file. If you really care about ensuring a key does not span multiple files: stop the algorithm now and re-run with a higher value for 'chunkMB' or for 'rowsPerChunk' (to increase the size of the files).", immediate. = TRUE)
				WARNED_ALREADY = TRUE
				obs_mid = ceiling(nrow(x_mem) / 2)
			}

			write_fst(x_mem[1:obs_mid], all_files_fst[ij[1]])
			write_fst(x_mem[(obs_mid+1):.N], all_files_fst[ij[2]])
		}

	}

	if(verbose > 0) message("\n", appendLF = FALSE)

	# we need to reupdate x_tmp (to have the appropriate meta information)
	x_tmp = hdd(tmpdir)
	# If warned already: means key is not valid
	if(WARNED_ALREADY == FALSE) attr(x_tmp, "key") = key

	# NOW: NO RESHAPING!
	# START: DEPREC ----------------------------------------------------- =
	# if(nfiles != nfiles_origin){
	# 	if(verbose > 0) message("Reshaping...", appendLF = FALSE)
	# 	write_hdd(x_tmp, dir = newfile, replace = TRUE, chunkMB = mean(x$.size) / 1e6, call_txt = call_txt)
	# 	if(verbose > 0) message("done")
	# } else {
	# 	write_hdd(x_tmp, dir = newfile, replace = TRUE, call_txt = call_txt)
	# }
	# __END: DEPREC ----------------------------------------------------- =
	write_hdd(x_tmp, dir = newfile, replace = TRUE, call_txt = call_txt)

	# Cleaning the tmp directory
	files2clean = list.files(tmpdir, full.names = TRUE)
	for(fname in files2clean) unlink(fname)
	unlink(tmpdir, recursive = TRUE)

}

#' Merges data to a HDD file
#'
#' This function merges in-memory/HDD data to a HDD file.
#'
#' @inherit hdd seealso
#'
#' @param x A HDD object or a \code{data.frame}.
#' @param y A data set either a data.frame of a HDD object.
#' @param newfile Destination of the result, i.e., a destination folder that will receive the HDD data.
#' @param chunkMB Numeric, default is missing. If provided, the data 'x' is split in chunks of 'chunkMB' MB and the merge is applied chunkwise.
#' @param rowsPerChunk Integer, default is missing. If provided, the data 'x' is split in chunks of 'rowsPerChunk' rows and the merge is applied chunkwise.
#' @param all Default is \code{FALSE}.
#' @param all.x Default is \code{all}.
#' @param all.y Default is \code{all}.
#' @param allow.cartesian Logical: whether to allow cartesian merge. Defaults to \code{FALSE}.
#' @param replace Default is \code{FALSE}: if the destination folder already contains data, whether to replace it.
#' @param verbose Numeric. Whether information on the advancement should be displayed. If equal to 0, nothing is displayed. By default it is equal to 1 if the size of \code{x} is greater than 1GB.
#'
#' @details
#' If \code{x} (resp \code{y}) is a HDD object, then the merging will be operated chunkwise, with the original chunks of the objects. To change the size of the chunks for \code{x}: you can use the argument \code{chunkMB} or \code{rowsPerChunk.}
#'
#' To change the chunk size of \code{y}, you can rewrite \code{y} with a new chunk size using \code{\link[hdd]{write_hdd}}.
#'
#' Note that the merging operation could also be achieved with \code{\link[hdd]{hdd_slice}} (although it would require setting up a ad hoc function).
#'
#' @author Laurent Berge
#'
#' @examples
#'
#' # Toy example with iris data
#'
#' # Cartesian merge example
#' iris_bis = iris
#' names(iris_bis) = c(paste0("x_", 1:4), "species_bis")
#' # We must have a common key on which to merge
#' iris_bis$id = iris$id = 1
#'
#' # merge, we chunk 'x' by 50 rows
#' hdd_path = tempfile()
#' hdd_merge(iris, iris_bis, newfile = hdd_path,
#' 		  rowsPerChunk = 50, allow.cartesian = TRUE)
#'
#' base_merged = hdd(hdd_path)
#' summary(base_merged)
#' print(base_merged)
#'
hdd_merge = function(x, y, newfile, chunkMB, rowsPerChunk, all = FALSE, all.x = all, all.y = all, allow.cartesian = FALSE, replace = FALSE, verbose){
	# Function to merge Hdd files
	# It returns a HDD file
	# x: hdd
	# y: data.frame or hdd
	# LATER: add possibility to subset/select variables of x before evaluation

	# CONTROLS

	mc = match.call()

	check_arg(x, y, "class(hdd) | data.frame mbt")
	check_arg(newfile, "character scalar", .message = "Argument 'newfile' must be a path to a directory.")

	check_arg(all, all.x, all.y, replace, allow.cartesian, "logical scalar")
	check_arg(verbose, "numeric scalar")

	call_txt = deparse_long(match.call())

	if(missing(verbose)){
		verbose = object_size(x)/1e6 > 1000
	}

	y_hdd = FALSE
	if("hdd" %in% class(y)){
		y_hdd = TRUE
	}

	IS_HDD = "hdd" %in% class(x)
	if(!IS_HDD){
		if(missing(rowsPerChunk) && missing(chunkMB)){
			if(y_hdd){
				stop("Since 'x' is not HDD, please provide the argument 'chunkMB' or 'rowsPerChunk' to make the merge chunkwise. Otherwise, since 'y' is a HDD object you may consider switching arguments x and y.")
			} else {
				stop("Since 'x' is not HDD, please provide the argument 'chunkMB' or 'rowsPerChunk' to make the merge chunkwise. Otherwise, you'd be better off just using a regular merge.")
			}

		}
	}

	names_x = names(x)
	names_y = names(y)
	by = intersect(names_x, names_y)

	if(length(by) == 0){
		stop("The two tables MUST have common variable names! Currently this is not the case.")
	}


	# Formatting the repository of destination
	dir = clean_path(newfile)
	if(grepl("\\.fst$", dir)) {
		# we get the directory where the file is
		dir = gsub("/[^/]+$", "/", dir)
	} else {
		# regular directory: we add / at the end
		dir = gsub("/?$", "/", dir)
	}

	dirDest = dir

	if(dir.exists(dirDest)){
		# cleaning (if necessary)
		all_files = clean_path(list.files(dirDest, full.names = TRUE))
		all_files2clean = all_files[grepl("/(slice_[[:digit:]]+\\.fst|_hdd\\.txt)$", all_files)]
		if(length(all_files2clean) > 0){
			if(!replace) stop("The destination diretory contains existing information. To replace it use argument replace=TRUE.")
			for(fname in all_files2clean) unlink(fname)
		}
	}

	# Determining the number of chunks
	DO_RESIZE = FALSE
	if(!missing(rowsPerChunk)){
		DO_RESIZE = TRUE
		use_rows = TRUE
		if("chunkMB" %in% names(mc)) warning("The value of argument 'chunkMB' is neglected since argument 'rowsPerChunk' is provided.")

		if(rowsPerChunk > 1e9){
			stop("The value of argument 'rowsPerChunk' cannot exceed 1 billion.")
		}

		n_chunks = ceiling(nrow(x) / rowsPerChunk)
	} else if(!missing(chunkMB)){
		DO_RESIZE = TRUE
		use_rows = FALSE
		if(class(x)[1] == "fst_table"){
			# we estimate the size of x
			n2check = min(1000, ceiling(nrow(x) / 10))
			size_x_subset = as.numeric(object.size(x[1:n2check, ]) / 1e6) # in MB
			size_x = size_x_subset * nrow(x) / n2check
		} else {
			size_x = as.numeric(object_size(x) / 1e6) # in MB
		}

		n_chunks = ceiling(size_x / chunkMB)
	}

	if(DO_RESIZE){
		# We resize x
		# simple way: creating a new HDD file
		tmpdir = paste0(dirDest, "/tmp_hdd_merge/")
		if(use_rows){
			write_hdd(x, tmpdir, rowsPerChunk = rowsPerChunk, replace = TRUE)
		} else {
			write_hdd(x, tmpdir, chunkMB = chunkMB, replace = TRUE)
		}

		x = hdd(tmpdir)
	}

	#
	# Merging
	#

	all_files_x = x$.fileName

	ADD = FALSE
	no_obs = TRUE
	for(fname in all_files_x){
		if(verbose > 0) message(".", appendLF = FALSE)
		x_chunk = read_fst(fname, as.data.table = TRUE)

		if(y_hdd){
			all_files_y = y$.fileName
			for(fname_y in all_files_y){
				y_chunk = read_fst(fname_y, as.data.table = TRUE)
				res_chunk = merge(x_chunk, y_chunk, by = by, all = all, all.x = all.x, all.y = all.y, allow.cartesian = allow.cartesian)
				if(nrow(res_chunk) > 0){
					no_obs = FALSE
					write_hdd(res_chunk, dirDest, chunkMB = Inf, add = ADD, replace = TRUE, call_txt = call_txt)
					ADD = TRUE
				}
			}

		} else {
			res_chunk = merge(x_chunk, y, by = by, all = all, all.x = all.x, all.y = all.y, allow.cartesian = allow.cartesian)
			if(nrow(res_chunk) > 0){
				no_obs = FALSE
				write_hdd(res_chunk, dirDest, chunkMB = Inf, add = ADD, replace = TRUE, call_txt = call_txt)
				ADD = TRUE
			}
		}

	}

	if(no_obs){
		message(ifelse(verbose > 0, "\n", ""), "No key", ifsingle(by, "", "s"), " in common found in the two data sets.")
	}

	if(DO_RESIZE){
		# Cleaning up
		files2clean = list.files(tmpdir, full.names = TRUE)
		for(fname in files2clean) unlink(fname)
		unlink(tmpdir, recursive = TRUE)
	}

}



#' Transforms text data into a HDD file
#'
#' Imports text data and saves it into a HDD file. It uses \code{\link[readr]{read_delim_chunked}} to extract the data. It also allows to preprocess the data.
#'
#' @inherit hdd seealso
#'
#' @param path Character vector that represents the path to the data. Note that it can be equal to patterns if multiple files with the same name are to be imported (if so it must be a fixed pattern, NOT a regular expression).
#' @param dirDest The destination directory, where the new HDD data should be saved.
#' @param chunkMB The chunk sizes in MB, defaults to 500MB. Instead of using this argument, you can alternatively use the argument \code{rowsPerChunk} which decides the size of chunks in terms of lines.
#' @param rowsPerChunk Number of rows per chunk. By default it is missing: its value is deduced from argument \code{chunkMB} and the size of the file. If provided, replaces any value provided in \code{chunkMB}.
#' @param col_names The column names, by default is uses the ones of the data set. If the data set lacks column names, you must provide them.
#' @param col_types The column types, in the \code{readr} fashion. You can use \code{\link{guess_col_types}} to find them.
#' @param nb_skip Number of lines to skip.
#' @param delim The delimiter. By default the function tries to find the delimiter, but sometimes it fails.
#' @param preprocessfun A function that is applied to the data before saving. Default is missing. Note that if a function is provided, it MUST return a data.frame, anything other than data.frame is ignored.
#' @param replace If the destination directory already exists, you need to set the argument \code{replace=TRUE} to overwrite all the HDD files in it.
#' @param verbose Integer. If verbose > 0, then the evolution of the importing process is reported. By default: equal to 1 when the expected number of chunks is greater than 1.
#' @param ... Other arguments to be passed to \code{\link[readr]{read_delim_chunked}}, \code{quote = ""} can be interesting sometimes.
#'
#' @details
#' This function uses \code{\link[readr]{read_delim_chunked}} from \code{readr} to read a large text file per chunk, and generate a HDD data set.
#'
#' Since the main function for importation uses \code{readr}, the column specification must also be in readr's style (namely \code{\link[readr]{cols}} or \code{\link[readr]{cols_only}}).
#'
#' By default a guess of the column types is made on the first 10,000 rows. The guess is the application of \code{\link[hdd]{guess_col_types}} on these rows.
#'
#' Note that by default, columns that are found to be integers are imported as double (in want of integer64 type in readr). Note that for large data sets, sometimes integer-like identifiers can be larger than 16 digits: in these case you must import them as character not to lose information.
#'
#' The delimiter is found with the function \code{\link[hdd]{guess_delim}}, which uses the guessing from \code{\link[data.table]{fread}}. Note that fixed width delimited files are not supported.
#'
#' @author Laurent Berge
#'
#' @examples
#'
#' # Toy example with iris data
#'
#' # we create a text file on disk
#' iris_path = tempfile()
#' fwrite(iris, iris_path)
#'
#' # destination path
#' hdd_path = tempfile()
#' # reading the text file with HDD, with approx. 50 rows per chunk:
#' txt2hdd(iris_path, dirDest = hdd_path, rowsPerChunk = 50)
#'
#' base_hdd = hdd(hdd_path)
#' summary(base_hdd)
#'
#' # Same example with preprocessing
#' sl_keep = sort(unique(sample(iris$Sepal.Length, 40)))
#' fun = function(x){
#' 	# we keep only some observations & vars + renaming
#' 	res = x[Sepal.Length %in% sl_keep, .(sl = Sepal.Length, Species)]
#' 	# we create some variables
#' 	res[, sl2 := sl**2]
#' 	res
#' }
#' # reading with preprocessing
#' hdd_path_preprocess = tempfile()
#' txt2hdd(iris_path, hdd_path_preprocess,
#' 		preprocessfun = fun, rowsPerChunk = 50)
#'
#' base_hdd_preprocess = hdd(hdd_path_preprocess)
#' summary(base_hdd_preprocess)
#'
#'
txt2hdd = function(path, dirDest, chunkMB = 500, rowsPerChunk, col_names, col_types, nb_skip, delim, preprocessfun, replace = FALSE, verbose, ...){
	# This function reads a large text file thanks to readr
	# and trasforms it into a HDD document

	#
	# Control
	#

	check_arg(path, dirDest, "character vector mbt no na")
	check_arg(chunkMB, "numeric scalar GT{0}")
	check_arg(col_names, "character vector no na")
	check_arg(nb_skip, "integer scalar GE{0}")
	check_arg(delim, "character scalar")
	check_arg(replace, "logical scalar")
	check_arg(verbose, "numeric scalar")
	check_arg(rowsPerChunk, "integer scalar GE{1}")
	check_arg(preprocessfun, "function arg(1)")

	path_all = path

	INFORM_PATTERN = FALSE

	path_all_list = list()

	for(i in seq_along(path_all)){
		path = path_all[i]

		if(path %in% c(".", "")) path = "./"

		path_clean = gsub("\\\\", "/", path)
		path_clean = gsub("/+", "/", path_clean)
		root = gsub("/[^/]+$", "/", path_clean)

		all_files = list.files(root, full.names = TRUE)
		# Dropping directories
		dir_names = setdiff(all_files, list.files(root, full.names = TRUE, recursive = TRUE))
		all_files = setdiff(all_files, dir_names)

		fpattern = gsub(".+/", "", path_clean)
		all_files_clean = gsub(".+/", "", all_files)

		qui = which(grepl(fpattern, all_files_clean, fixed = TRUE))
		if(length(qui) == 0){
			stop("The path '", path, "' leads to no file.")
		} else if(length(qui) > 1){
			INFORM_PATTERN = TRUE
		}

		path_all_list[[i]] = all_files[qui]

	}

	path_all = unique(unlist(path_all_list))

	mc = match.call()

	DO_PREPROCESS = !missing(preprocessfun)

	if(!missing(col_types) && !inherits(col_types, "col_spec")){
		stop("Argument 'col_types' must be a 'col_spec' object, obtained from, e.g., readr::cols() or readr::cols_only(), or from guess_cols_type()).")
	}

	#
	# Preliminary stuff
	#

	# If multiple files: we check the consistency
	n = length(path_all)
	fileSize_all = numeric(n)
	nb_col_all = numeric(n)
	col_names_all = list()

	for(i in seq_along(path_all)){
		path = path_all[i]

		# sample DT to get first information
		first_lines = readr::read_lines(path, n_max = 10000)
		sample_table = data.table::fread(paste0(first_lines, collapse = "\n"))

		nb_col = ncol(sample_table)
		nb_col_all[i] = nb_col

		if(missing(col_types)){
			col_types = guess_col_types(sample_table, col_names)
		} else if(length(col_types$cols) == nb_col){
			if(missing(col_names)){
				col_names = names(col_types$cols)
			}
		}

		# are there column names?
		is_col_names = ifelse(all(grepl("^V", names(sample_table))), FALSE, TRUE)

		if(missing(nb_skip)){
			if(is_col_names){
				nb_skip = 1
			} else {
				nb_skip = 0
			}
		}

		prefix = ifelse(n == 1, "", paste0("[file ", i, ": ", path, "] "))
		if(missing(col_names)){
			if(!is_col_names){
				stop(prefix, "The text file has no header, you MUST provide ", nb_col, " column names (col_names).")
			} else {
				col_names = names(sample_table)
			}
		} else {
			if(length(col_names) != nb_col){
				if(i > 1){
					stop(prefix, "The data has ", nb_col, " columns instead of ", length(col_names), " like the previous files.")
				} else {
					stop(prefix, "The variable col_names should be of length ", nb_col, ". (At the moment it is of length ", length(col_names), ").")
				}

			}
		}

		col_names_all[[i]] = col_names

		#
		# finding out the delimiter
		#

		if(i == 1){
			if(missing(delim)){
				fl = head(first_lines, 100)
				attr(fl, "from_hdd") = TRUE
				delim = guess_delim(fl)
				if(is.null(delim)){
					stop("The delimiter could not be automatically determined. Please provide argument 'delim'. FYI, here is the first line:\n", first_lines[1])
				}
			}
			delimiter = delim
		}

		# Just for information on nber of chunks
		fileSize_all[i] = file.size(path)

	}

	fileSize = sum(fileSize_all) / 1e6

	# Consistency across multiple files
	if(!all(nb_col_all == nb_col_all[1])) {
		qui = which.max(nb_col_all != nb_col_all[1])
		stop("The number of columns across files differ: file 1 has ", nb_col_all[1], " columns while file ", qui, " has ", nb_col_all[qui], " columns (", path_all[1], " vs ", path_all[qui], ").")
	}

	if(!all(sapply(col_names_all, function(x) x == col_names_all[[1]]))) {
		qui = which.max(sapply(col_names_all, function(x) x != col_names_all[[1]]))
		stop("The column names across files differ:\n File 1:", paste(col_names_all[[1]], collapse = ", "), "\nFile ", qui, ": ", paste(col_names_all[[qui]], collapse = ", "))
	}

	# Information on the number of files found (if needed)
	if(INFORM_PATTERN && ((!missing(verbose) && verbose > 0) || (missing(verbose) && fileSize > 2000))){
		message(n, " files (", signif_plus(fileSize), " MB)")
	}


	if(!missing(rowsPerChunk)){
		if("chunkMB" %in% names(mc)) warning("The value of argument 'chunkMB' is neglected since argument 'rowsPerChunk' is provided.")

		if(rowsPerChunk > 1e9){
			# prevents readr hard bug
			stop("Argument 'rowsPerChunk' cannot be greater than one billion.")
		}

		# getting the nber of chunks
		chunkMB_approx = ceiling(as.numeric(object.size(sample_table) / 1e6) / nrow(sample_table) * rowsPerChunk)

		nbChunks_approx = ceiling(fileSize / chunkMB_approx)

		if(missing(verbose)) verbose = nbChunks_approx > 1

		if(verbose > 0) message("Approx. number of chunks: ", nbChunks_approx, " (", ifelse(chunkMB_approx <= 1, "< 1", paste0("~", addCommas(chunkMB_approx))), "MB per chunk)")

	} else {
		nbChunks_approx = fileSize / chunkMB

		# Nber of rows per extraction (works for small files too)
		rowsPerChunk = ceiling(nrow(sample_table) / as.numeric(object.size(sample_table) / 1e6) * min(chunkMB, 100*fileSize))
		# Limit of 500M lines
		rowsPerChunk = min(rowsPerChunk, 500e6)

		if(missing(verbose)) verbose = nbChunks_approx > 1

		if(verbose > 0) message("Approx. number of chunks: ", ceiling(nbChunks_approx), " (", addCommas(rowsPerChunk), " rows per chunk)")
	}


	# Function to apply to each chunk

	dirDest = clean_path(dirDest)
	REP_PBLM = gsub("/?$", "/problems", dirDest)
	REP_DEST = dirDest
	ADD = FALSE
	IS_PBLM = FALSE

	# We check replacement
	if(dir.exists(REP_DEST)){

		# cleaning (if necessary)
		all_files = clean_path(list.files(REP_DEST, full.names = TRUE))
		all_files2clean = all_files[grepl("/(slice_[[:digit:]]+\\.fst|_hdd\\.txt)$", all_files)]
		if(length(all_files2clean) > 0){
			if(!replace) stop("The destination diretory contains existing information. To replace it use argument replace=TRUE.")
			for(fname in all_files2clean) unlink(fname)
		}

		# We also clean the problems folder
		if(dir.exists(REP_PBLM)){
			all_files_pblm = list.files(REP_PBLM, full.names = TRUE)
			for(fname in all_files_pblm) unlink(fname)
		}
	}

	CALL_TXT = deparse_long(match.call())

	funPerChunk = function(x, pos){

		# get the problems
		pblm = readr::problems(x)
		if(nrow(pblm) > 0){
			setDT(pblm)
			write_hdd(pblm, REP_PBLM, replace = TRUE, add = IS_PBLM, call_txt = paste0("PROBLEMS: ", CALL_TXT))
			IS_PBLM <<- TRUE
		}

		setDT(x)

		# preprocess if needed
		if(DO_PREPROCESS){
			x = preprocessfun(x)
			if(!is.data.frame(x) || nrow(x) == 0){
				return(NULL)
			}
		}

		# save the data
		write_hdd(x, dir = REP_DEST, replace = TRUE, add = ADD, call_txt = CALL_TXT)

		if(!ADD) ADD <<- TRUE


	}

	for(path in path_all){
		readr::read_delim_chunked(file = path, callback = funPerChunk, chunk_size = rowsPerChunk, col_names = col_names, col_types = col_types, skip = nb_skip, delim = delimiter, ...)
	}

	invisible(NULL)
}

# This function is conservative, it will suggest to import as characters large integers in order not to lose information (remember we should deal with big data here!).

#' Guesses the columns types of a file
#'
#' This function is a facility to guess the column types of a text document. It returns columns formatted a la readr.
#'
#' @param dt_or_path Either a data frame or a path.
#' @param col_names Optional: the vector of names of the columns, if not contained in the file. Must match the number of columns in the file.
#' @param n Number of observations used to make the guess. By default, \code{n = 100000}.
#'
#' @author Laurent Berge
#'
#' @details
#'
#' The guessing of the column types is based on the 10,000 (set with argument \code{n}) first rows.
#'
#' Note that by default, columns that are found to be integers are imported as double (in want of integer64 type in readr). Note that for large data sets, sometimes integer-like identifiers can be larger than 16 digits: in these case you must import them as character not to lose information.
#'
#' @return
#' It returns a \code{\link[readr]{cols}} object a la \code{readr}.
#'
#' @seealso
#' See \code{\link[hdd]{peek}} to have a convenient look at the first lines of a text file. See \code{\link[hdd]{guess_delim}} to guess the delimiter of a text data set. See \code{\link[hdd]{guess_col_types}} to guess the column types of a text data set.
#'
#'
#' See \code{\link[hdd]{hdd}}, \code{\link[hdd]{sub-.hdd}} and \code{\link[hdd]{cash-.hdd}} for the extraction and manipulation of out of memory data. For importation of HDD data sets from text files: see \code{\link[hdd]{txt2hdd}}.
#'
#'
#'
#' @examples
#'
#' # Example with the iris data set
#' iris_path = tempfile()
#' fwrite(iris, iris_path)
#'
#' # returns a readr columns set:
#' guess_col_types(iris_path)
#'
#'
guess_col_types = function(dt_or_path, col_names, n = 10000){
	# guess the column types of a text document

	if(missing(dt_or_path)) stop("Argument 'dt_or_path' is required.")

	if(length(dt_or_path) == 1 && is.character(dt_or_path)){
		first_lines = readr::read_lines(dt_or_path, n_max = n)
		sample_dt = data.table::fread(paste0(first_lines, collapse = "\n"))
	} else if(is.data.frame(dt_or_path)){
		sample_dt = dt_or_path
	} else {
		stop("Object dt_or_path must be a data.frame or a path.")
	}

	check_arg(col_names, "character vector")
	check_arg(n, "integer scalar GE{1}")

	# The column names
	if(missing(col_names)){
		col_names = names(sample_dt)
	} else {
		if(length(col_names) != length(names(sample_dt))){
			stop("The length of col_names (", length(col_names), ") does not match the lenght of the data (", length(names(sample_dt)), ").")
		}
		names(sample_dt) = col_names
	}

	# guessing the types of each column
	all_classes = sapply(sample_dt, function(x) tolower(class(x)[length(class(x))]))
	qui_int = which(all_classes == "integer")
	qui_int64 = which(all_classes == "integer64")
	qui_double = which(all_classes == "double")
	qui_character = which(all_classes == "character")
	qui_numeric = which(all_classes == "numeric")

	all_classes[qui_int] = "double" # double is (almost) Int64 in terms of precision
	all_classes[qui_int64] = "double"
	all_classes[qui_numeric] = "double"

	for(v in qui_character){
		new_type = readr::guess_parser(sample_dt[[v]])
		if(any(grepl("(?i)date", new_type))){
			all_classes[v] = "date"
		}
	}

	# Now we write the types into the readr syntax
	all_classes[all_classes == "character"] = "c"
	all_classes[all_classes == "double"] = "d"
	all_classes[all_classes == "date"] = "D"
	all_classes[all_classes == "logical"] = "l"
	myCall = paste0("readr::cols(", paste0(col_names, "='", all_classes, "'", collapse = ", "), ")")

	eval(parse(text = myCall))
}


#' Guesses the delimiter of a text file
#'
#' This function uses \code{\link[data.table]{fread}} to guess the delimiter of a text file.
#'
#' @inherit guess_col_types seealso
#'
#' @param path The path to a text file containing a rectangular data set.
#'
#' @return
#' It returns a character string of length 1: the delimiter.
#'
#' @author Laurent Berge
#'
#' @examples
#'
#' # Example with the iris data set
#' iris_path = tempfile()
#' fwrite(iris, iris_path)
#'
#' guess_delim(iris_path)
#'
guess_delim = function(path){


	check_arg(path, "character vector", .message = "Argument path must be a valid path.")

	# importing a sample
	FROM_HDD = FALSE
	if(!is.null(attr(path, "from_hdd"))){
		first_lines = path
		FROM_HDD = TRUE
	} else if(length(path) > 1){
		stop("Argument path must be a valid path. Currenlty it is of length ", length(path), ".")
	} else {
		first_lines = readLines(path, n = 100)
	}

	sample_dt = fread(paste0(first_lines, collapse = "\n"))

	#
	# Finding the delimiter
	#

	if(ncol(sample_dt) >= 2){
		# very simple check -- real work here is made by fread
		find_candidate = function(x){
			tx = table(strsplit(gsub(" ", "", x), ""))
			candidate = tx[tx >= ncol(sample_dt) - 1]
			names(candidate[!grepl("[[:alnum:]]|\"|'|-|\\.", names(candidate))])
		}
		l1_candidate = find_candidate(first_lines[1])

		delim = NULL
		if(length(l1_candidate) == 1){
			delim = l1_candidate
		} else if(length(l1_candidate) > 1){
			candid_all = lapply(first_lines, find_candidate)
			t_cand = table(unlist(candid_all))
			t_cand = names(t_cand)[t_cand == length(first_lines)]
			if(length(t_cand) == 1){
				delim = t_cand
			}
		}

		if(is.null(delim) && !FROM_HDD){
			stop("Could not determine the delimiter. Here is the first line:\n", first_lines[1])
		}
	} else {
		delim = ","
	}

	return(delim)
}


#' Peek into a text file
#'
#' This function looks at the first elements of a file, format it into a data frame and displays it. It can also just show the first lines of the file without formatting into a DF.
#'
#' @inherit guess_col_types seealso
#'
#' @param path Path linking to the text file.
#' @param onlyLines Default is \code{FALSE}. If \code{TRUE}, then the first \code{n} lines are directly displayed without formatting.
#' @param n Integer. The number of lines to extract from the file. Default is 100 or 5 if \code{onlyLine = TRUE}.
#' @param view Logical, default it \code{TRUE}: whether the data should be displayed on the viewer. Only when \code{onlyLines = FALSE}.
#'
#' @return
#' Returns the data invisibly.
#'
#' @author Laurent Berge
#'
#' @examples
#'
#' # Example with the iris data set
#' iris_path = tempfile()
#' fwrite(iris, iris_path)
#'
#' \donttest{
#' # The first lines of the text file on viewer
#' peek(iris_path)
#' }
#'
#' # displaying the first lines:
#' peek(iris_path, onlyLines = TRUE)
#'
#' # only getting the data from the first observations
#' base = peek(iris_path, view = FALSE)
#' head(base)
#'
peek = function(path, onlyLines = FALSE, n, view = TRUE){

	# Controls

	check_arg(path, "character scalar mbt")
	check_arg(onlyLines, "logical scalar")
	check_arg(view, "logical scalar")
	check_arg(n, "integer scalar GE{1}")

	if(!file.exists(path)) stop("The path does not lead to an existing file.")

	if(missing(n)){
		if(onlyLines){
			n = 5
		} else {
			n = 100
		}
	}

	first_lines = readLines(path, n = n)

	if(onlyLines){
		return(first_lines)
	}

	sample_dt = fread(paste0(first_lines, collapse = "\n"))

	#
	# Finding the delimiter
	#

	if(ncol(sample_dt) >= 2){

		fl = head(first_lines, 100)
		attr(fl, "from_hdd") = TRUE
		delim = guess_delim(fl)

		if(is.null(delim)){
			message("Could not determine the delimiter. FYI, here is the first line:\n", first_lines[1])
		} else {
			if(delim == ","){
				delim = "CSV"
			} else if(delim == "\t"){
				delim = "TSV"
			}
			message("Delimiter: ", delim)
		}
	}

	dt_name = paste0("peek_", gsub("\\.[^\\.]+$", "", gsub("^.+/", "", clean_path(path))))

	if(view) {
		myView <- get("View", envir = as.environment("package:utils"))
		myView(x = sample_dt, title = dt_name)
	}
	invisible(sample_dt)
}



#' Extracts the origin of a HDD object
#'
#' Use this function to extract the information on how the HDD data set was created.
#'
#' @inherit hdd seealso
#'
#' @param x A HDD object.
#'
#' @details
#'
#' Each HDD lives on disk and a \dQuote{_hdd.txt} is always present in the folder containing summary information. The function \code{origin} extracts the log from this information file.
#'
#' @return
#' A character vector, if the HDD data set has been created with several instances of \code{\link[hdd]{write_hdd}} its length will be greater than 1.
#'
#' @examples
#'
#' # Toy example with iris data
#'
#' hdd_path = tempfile()
#' write_hdd(iris, hdd_path, rowsPerChunk = 20)
#'
#' base_hdd = hdd(hdd_path)
#' origin(base_hdd)
#'
#' # Let's add something
#' write_hdd(head(iris), hdd_path, add = TRUE)
#' write_hdd(iris, hdd_path, add = TRUE, rowsPerChunk = 50)
#'
#' base_hdd = hdd(hdd_path)
#' origin(base_hdd)
#'
#'
origin = function(x){

	if(!"hdd" %in% class(x)){
		stop("Argument 'x' must be a HDD object.")
	}

	dir = gsub("/[^/]+$", "/", x$.fileName[1])
	infoFile = paste0(dir, "_hdd.txt")
	if(file.exists(infoFile)){
		info = readLines(infoFile)
		qui = which(grepl("^log:", info))
		all_log = info[(qui+1):length(info)]
	} else {
		all_log = "The HDD data did not have a _hdd.txt file. Was it deleted by the user?"
	}

	all_log
}


####
#### S3 methods ####
####

#' Dimension of a HDD object
#'
#' Gets the dimension of a hard drive data set (HDD).
#'
#' @inherit hdd examples
#'
#' @param x A \code{HDD} object.
#'
#' @return
#' It returns a vector of length 2 containing the number of rows and the number of columns of the HDD object.
#'
#' @author Laurent Berge
#'
#'
dim.hdd = function(x){
	n = length(x$.size)
	c(x$.row_cum[n], x$.ncol[1])
}

#' Variables names of a HDD object
#'
#' Gets the variable names of a hard drive data set (HDD).
#'
#' @inherit hdd seealso
#'
#' @inheritParams dim.hdd
#' @inherit hdd examples
#'
#' @author Laurent Berge
#'
#' @return
#' A character vector.
#'
#'
#'
names.hdd = function(x){
	x_tmp = fst(x$.fileName[1])
	names(x_tmp)
}

#' Print method for HDD objects
#'
#' This functions displays the first and last lines of a hard drive data set (HDD).
#'
#' @inheritParams dim.hdd
#' @inherit hdd examples
#' @inherit hdd seealso
#'
#' @param ... Not currently used.
#'
#' @details
#' Returns the first and last 3 lines of a HDD object. Also formats the values displayed on screen (typically: add commas to increase the readability of large integers).
#'
#' @author Laurent Berge
#'
#' @return
#' Nothing is returned.
#'
#'
print.hdd = function(x, ...){
	n = nrow(x)
	if(n < 8){
		print(x[])
	} else {
		quoi = as.data.frame(rbindlist(list(head(x, 4), tail(x, 3))))
		quoi = formatTable(quoi, d = 3)
		quoi[4, ] = rep(" " , ncol(quoi))
		nmax = tail(x$.row_cum, 1)
		dmax = log10(nmax) + 1
		row.names(quoi) = c(1:3, substr('------------', 1, max(3, 4/3*dmax)), numberFormat(nmax - 2:0))
		print(quoi)
	}
}

#' Summary information for HDD objects
#'
#' Provides summary information -- i.e. dimension, size on disk, path, number of slices -- of hard drive data sets (HDD).
#'
#' @inheritParams dim.hdd
#' @inherit hdd examples
#' @inherit hdd seealso
#'
#' @param object A HDD object.
#' @param ... Not currently used.
#'
#' @details
#' Displays concisely general information on the HDD object: its size on disk, the number of files it is made of, its location on disk and the number of rows and columns.
#'
#' Note that each HDD object contain the text file \dQuote{_hdd.txt} in their folder also containing this information.
#'
#' To obtain how the HDD object was constructed, use function \code{\link[hdd]{origin}}.
#'
#' @author Laurent Berge
#'
#'
#'
summary.hdd = function(object, ...){
	n = length(object$.size)
	cat("Hard drive data of ", osize(object), " Made of ", n, " file", ifelse(n>1, "s", ""), ".\n", sep = "")

	key = attr(object, "key")
	if(!is.null(key)){
		cat("Sorted by:", paste0(key, collapse = ", "), "\n")
	}

	cat("Location: ", gsub("/[^/]+$", "/", object$.fileName[1]), "\n", sep = "")
	nb = object$.row_cum[n]
	nb = numberFormat(nb)
	cat(nb, " lines, ", object$.ncol[1], " variables.\n", sep = "")

}






























