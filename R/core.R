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



#' Read fst or hdd files as DT
#'
#' This is the function \code{\link[fst]{read_fst}} but with automatic conversion to data.table. It also allows to read \code{hdd} data.
#'
#' @param path Path to \code{fst} file -- or path to \code{hdd} data. For hdd files, there is a
#' @param columns Column names to read. The default is to read all columns. Ignored for \code{hdd} files.
#' @param from Read data starting from this row number. Ignored for \code{hdd} files.
#' @param to Read data up until this row number. The default is to read to the last row of the stored dataset. Ignored for \code{hdd} files.
#' @param confirm If the hdd file is larger than the variable \code{getHdd_extract.cap()}, then by default an error is raised. To anyway read the data, use \code{confirm = TRUE}. You can set the data cap with the function \code{\link[hdd]{setHdd_extract.cap}}, the default being 1GB.
#'
#' @examples
#'
#' \dontrun{
#' base = readfst("path.fst")
#' # is exactly equivalent to:
#' base = read_fst("path.fst", as.data.table = TRUE)
#'
#' # reading the full data from a hdd file
#' base = readfst("hdd_path")
#' # is equivalent to:
#' base = hdd("hdd_path")[]
#'
#' }
#'
#'
#'
#'
readfst = function(path, columns = NULL, from = 1, to = NULL, confirm = FALSE){
	# it avoids adding as.data.table = TRUE
	# + reads hdd files

	check_arg(path, "singleCharacter")
	check_arg(from, "singleIntegerGE1")
	check_arg(to, "nullSingleIntegerGE1")
	check_arg(confirm, "singleLogical")

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
			stop("Argument path must be either a fst file (this is not the case), either a hdd folder. Using path as hdd raises an error:\n", res)
		}

		mc = match.call()
		qui_pblm = intersect(names(mc), c("columns", "from", "to"))
		if(length(qui_pblm) > 0){
			stop("When 'path' leads to a hdd file, the full dataset is read. Thus the argument", enumerate_words(qui_pblm, addS = TRUE), " ignored: for sub-selections use hdd(path) instead.")
		}

		res_size = object_size(res) / 1e6
		size_cap = getHdd_extract.cap()
		if(res_size > size_cap && isFALSE(confirm)){
			stop("Currently the size of the hdd data is ", numberFormat(res_size), "MB which exceeds the cap of ", size_cap, "MB. Please use argument 'confirm' to proceed.")
		}

		res = res[]
	}

	res
}

#' Applies a function to slices of data (and save them)
#'
#' This function is useful to apply complex R functions to large datasets (out of memory). It slices the input data, applies the function, then saves each chunk into a hard drive folder. This can then be a hdd dataset.
#'
#' @param x A dataset (data.frame, hdd).
#' @param fun A function to be applied to slices of the dataset. The function must return a data frame like object.
#' @param chunkMB The size of the slices, default is 500MB. That is: the function \code{fun} is applied to each 500Mb of data \code{x}. If the function creates a lot of additionnal information, you may want this number to go down. On the other hand, if the function reduces the information you may want this number to go up. In the end it will depend on the amount of memory available.
#' @param dir The destination directory where the data is saved.
#' @param replace Whether all information on the destination directory should be erased beforehand. Default is \code{FALSE}.
#' @param ... Other parameters to be passed to \code{fun}.
#'
#' @return
#' It doesn't return anything, the output is a "hard drive data" saved in the hard drive.
#'
#' @examples
#'
#' \dontrun{
#'
#' # Example in the context of patent data.
#'
#' # Assume you have a data set, pat_abstract, with two variables:
#' # - patent_id: an identification number (here a patent number)
#' # - abstract: the abstract of the patent
#' # You want to create the data set, pat_word, with three variables:
#' # - patent_id: same as before
#' # - word: a word appearing in the patent abstract
#' # - word_freq: the number of times the word appeared in the patent abstract
#'
#' # Typically, if you have millions of patents, it will
#' # create a data set that won't fit in memory.
#' # This is what hdd_slice is for.
#'
#' # Now the code:
#' fun = function(x){
#'   abstract_split = strsplit(x$abstract, " ")
#'   base_split = data.table(patent_id = rep(x$patent_id,
#'                                           lengths(abstract_split)),
#'                           word = unlist(abstract_split))
#'
#'   res = base_split[, .(word_freq = .N), by = .(patent_id, word)]
#'   res
#' }
#'
#' hdd_slice(pat_abstract, fun, "path/pat_word")
#'
#' }
#'
hdd_slice = function(x, fun, dir, chunkMB = 500, replace = FALSE, ...){
	# This function is useful for performing memory intensive operations
	# it slices the operation in several chunks of the initial dat
	# then you need to use the function recombine to obtain the result
	# x: the main vector/matrix to which apply fun
	# fun: the function to apply to x
	# chunkMB: the size of the chunks of x, in mega bytes // default is a "smart guess"
	# dir: the repository where to make the temporary savings. Default is "."


	# Controls

	if(missing(x)){
		stop("Argument 'x' is missing but is required.")
	}

	if(missing(fun)){
		stop("Argument 'fun' is missing but is required.")
	} else if(!is.function(fun)){
		stop("Argument 'fun' must be a function. Currently its class is ", class(fun)[[1]], ".")
	}

	check_arg(dir, "singleCharacterMbt")
	check_arg(chunkMB, "singleNumericGT0")
	check_arg(replace, "singleLogical")

	if(is.null(dim(x))){
		isTable = FALSE
		n = length(x)
	} else {
		isTable = TRUE
		n = nrow(x)
	}

	# size of x
	if(class(x)[1] == "fst_table"){
		# we estimate the size of x
		n2check = min(1000, ceiling(nrow(x) / 10))
		size_x_subset = as.numeric(object.size(x[1:n2check, ]) / 1e6) # in MB
		size_x = size_x_subset * nrow(x) / n2check
	} else {
		size_x = as.numeric(object_size(x) / 1e6) # in MB
	}

	n_chunks = ceiling(size_x / chunkMB)

	if(n_chunks == 1){
		stop("Size of 'x' is lower than 'chunkMB'. Function hdd_slice() is useless.")
	}

	start = floor(seq(1, n, by = n/n_chunks))
	start = start[1:n_chunks]
	end = c(start[-1] - 1, n)

	# The directory
	dir = gsub("([^/])$", "\\1/", dir)

	if(!dir.exists(dir)){
		dir.create(dir)
	}

	# cleaning (if necessary)
	all_files = list.files(dir, full.names = TRUE)
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
		cat(i, "..", sep = "")
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

	cat("end.\n")
}



#' Hard drive data set
#'
#' This function connects to a hard drive data set (hdd). You can access the hard drive data in a similar way as a \code{data.table}.
#'
#' @param dir The directory where the hard drive data set is.
#'
#' @examples
#'
#' \dontrun{
#' # your data set is in the hard drive, in hdd format already
#' data_hdd = hdd("path/my_big_data")
#' summary(data_hdd)
#'
#' }
#'
#'
hdd = function(dir){
	# This function creates a link to a repository containing fst files
	# NOTA: The HDD files are all named "sliceXX.fst"

	check_arg(dir, "singleCharacterMbt")

	# The directory + prefix
	if(grepl("\\.fst", dir)) {
		dir = gsub("[[:digit:]]+\\.fst", "", dir)
	} else {
		dir = paste0(gsub("([^/])$", "\\1/", dir), "slice")
	}

	dir = gsub("/[^/]+$", "/", dir)
	file_head = paste0(dir, "slice_")

	if(!dir.exists(dir)){
		stop("In argument 'dir': The directory ", dir, " does not exists.")
	}

	# all_files: valid files containing data: i.e. dir/slice_xx.fst
	all_files = list.files(dir, full.names = TRUE)
	all_files = sort(all_files[grepl(file_head, all_files) & grepl("\\.fst", all_files)])

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
			attr(info_files, "key") = key[-1]
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



#' Extraction of hdd data
#'
#' This function extract data from HDD files, in a similar fashion as data.table but with more arguments.
#'
#' @param x A hdd file.
#' @param index An index, you can use \code{.N} and variable names, like in data.table.
#' @param ... Other components of the extraction to be passed to \code{\link[data.table]{data.table}}.
#' @param file Which file to extract from? (Remember hdd data is split in several files.) You can use \code{.N}.
#' @param all.vars Logical, default is \code{FALSE}. By default, if the first argument of \code{...} is provided (i.e. argument \code{j}) then only variables appearing in all \code{...} plus the variable names found in \code{index} are extracted. If \code{TRUE} all variables are extracted before any selection is done. (This can be useful when the algorithm getting the variable names gets confused in case of complex queries.)
#' @param newfile A destination directory. Default is missing. Should be result of the query be saved into a new HDD directory? Otherwise, it is put in memory.
#' @param replace Only used if argument \code{newfile} is not missing: default is \code{FALSE}. If the \code{newfile} points to an existing HDD data, then to replace it you must have \code{replace = TRUE}.
#'
#' @return
#' Returns a data.table extracted from a hdd file (except if newwfile is not missing).
#'
#' @examples
#'
#' \dontrun{
#'
#' # your data set is in the hard drive, in hdd format already.
#' # Say this data is made of two variables: x and id.
#' data_hdd = hdd("path/big_data")
#'
#' # You can use the argument 'file' to subselect slices.
#' # Let's have some descriptive statistics of the first slice of HDD
#' summary(data_hdd[, file = 1])
#' # It extract the data from the first HDD slice and
#' # returns a data.table in memory, we then apply summary to it
#'
#' # You can use the special argument .N, as in data.table.
#' # the following query shows the first and last lines of
#' # each slice of the HDD data set:
#' data_hdd[c(1, .N), file = 1:.N]
#'
#' # Extraction of observations for which variable x is equal to 1
#' data_hdd[x == 1, ]
#'
#' # You can apply data.table syntax:
#' data_hdd[, .(x, mean_x = mean(x))]
#'
#' # You can use the by clause, but then
#' # the by is applied slice by slice, NOT on the full data set:
#' data_hdd[, .(mean_x = mean(x)), by = id]
#'
#' # If the data you extract does not fit into memory,
#' # you can create a new HDD file with the argument 'newfile':
#' data_hdd[, .(x, mean_x = mean(x)), newfile = "path/big_data_bis"]
#' # check the result:
#' data_hdd_bis = hdd("path/big_data_bis")
#' summary(data_hdd_bis)
#' print(data_hdd_bis)
#'
#'
#' }
#'
"[.hdd" = function(x, index, ..., file, newfile, replace = FALSE, all.vars = FALSE){
	# newfile: creates a new hdd

	# We look at what variables to select, because it is costly to extract variables: we need the minimum!
	var_names = names(x)
	mc = match.call()
	call_txt = deparse_long(mc)

	# check_arg(file, "integerVector")
	check_arg(newfile, "singleCharacter", "Argument 'newfile' must be a valid path to a directory. REASON")
	check_arg(replace, "singleLogical")
	check_arg(all.vars, "singleLogical")

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
	# 			message("Note that the '", clause, "' clause is applied chunk by chunk, this is not a '", clause, "' on the whole data set. Currently the key", enumerate_items(key, addS = TRUE, start_verb = TRUE), " while the ", clause, " clause requires ", enumerate_items(vars_by, verb = FALSE), ". You may have to re-run hdd_setkey().")
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

		dir = newfile
		if(grepl("\\.fst", dir)) {
			dir = gsub("[[:digit:]]+\\.fst", "", dir)
		} else {
			dir = paste0(gsub("([^/])$", "\\1/", dir), "slice")
		}

		dir = gsub("/[^/]+$", "/", dir)
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
				x_tmp = read_fst(fileName, as.data.table = TRUE, columns = var2select)
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
#'
#' @param name The variable name to be extracted.Note that there is an automatic protection for not trying to import data that would not fit into memory. The extraction cap is set with the function \code{\link[hdd]{setHdd_extract.cap}}.
#'
#' @return
#' It returns a vector.
#'
#' @examples
#'
#' \dontrun{
#' # your data set is in the hard drive, in hdd format already.
#' # Say this data is made of two variables: x and id.
#' # 'x' is integer and 'id' is of type character.
#' data_hdd = hdd("path/big_data")
#'
#' # The two variables, if loaded into memory, would be of size:
#' # - x :  800MB
#' # - id: 5000MB
#'
#' # There is a protection for not loading variable that are too large.
#' # By default, the cap is set at 1000MB, you can change it
#' # with setHdd_extract.cap(new_cap).
#'
#' # By default, the following would run:
#' x = data_hdd$x
#'
#' # But this would raise an error:
#' id = data_hdd$id
#'
#' # To make it would you need at least to set a 5GB cap:
#' setHdd_extract.cap(6000)
#' id = data_hdd$id
#' # Now it would work (provided you have enough memory!!!)
#'
#' }
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
		stop(name, " is not a variable of the hdd data ", deparse(mc$x), ".")
	}

	i = which(names(x) == name)

	n_row = nrow(x)
	x_sample = head(x, 5)
	x_num = sapply(x_sample, is.numeric)

	# estimate of the size of the data
	isNum = FALSE
	if(x_num[i]){
		# numeric: 8 bytes
		isNum = TRUE
		current_size = 8 * n_row
	} else {
		# for non numeric data: we don't know!
		# we have an upper bound
		x_size = object_size(x)
		current_size = x_size - sum(x_num) * 8 * n_row
	}
	# we tranform in MB
	current_size = addCommas(round(current_size / 1e6))

	size_cap = getHdd_extract.cap()
	if(current_size > size_cap){
		stop("Cannot extract variable ", name, " because its expected size (", current_size, " MB) is greater than the cap of ", addCommas(size_cap), " MB. You can change the cap using setHdd_extract.cap(new_cap).")
	}

	# now extraction
	res = eval(parse(text = paste0("x[, ", name, "]")))

	res
}


#' Saves or appends a dataset into a HDD file
#'
#' This function saves in-memory/HDD data sets into HDD repositories. Useful to append several data sets.
#'
#' @param x A data set.
#' @param dir The HDD repository, i.e. the directory where the HDD data is.
#' @param chunkMB If the data has to be split in several files of \code{chunkMB} sizes. Default is \code{Inf}.
#' @param compress Compression rate to be applied by \code{\link[fst]{write_fst}}. Default is 50.
#' @param add Should the file be added to the existing repository? Default is \code{FALSE}.
#' @param replace If \code{add = FALSE}, should any existing document be replaced? Default is \code{FALSE}.
#' @param showWarning If the data \code{x} has no observation, then a warning is raised if \code{showWarning = TRUE}. By default, it occurs only if \code{write_hdd} is NOT called within a function.
#' @param ... Not currently used.
#'
#' @examples
#'
#' \dontrun{
#'
#' # Say you have a data set split into 25 different files.
#' # You want to save them all in a single HDD file:
#' write_hdd(data_01, "path/big_data")
#' write_hdd(data_02, "path/big_data", add = TRUE)
#' # etc...
#' write_hdd(data_25, "path/big_data", add = TRUE)
#'
#' # Now say you want to reformat a HDD file
#' # to increase the chunk sizes:
#' write_hdd("path/big_data", "path/big_data_smallChunk", chunkMB = 1000)
#'
#' }
#'
write_hdd = function(x, dir, chunkMB = Inf, compress = 50, add = FALSE, replace = FALSE, showWarning, ...){
	# data: the data (in memory or fst or hdd)
	# dir: the hdd repository
	# write hdd data
	# _hdd.txt => file avec infos
	# variables, head du file, si'il y a une key ou pas
	# we may add a chunk option later on  -- maybe not so useful

	mc = match.call()

	# controls
	check_arg(dir, "singleCharacterMbt", "Argument 'dir' must be a valid path. REASON")
	check_arg(chunkMB, "singleNumericGT0")
	check_arg(compress, "singleIntegerGE0LE100")
	check_arg(add, "singleLogical")
	check_arg(replace, "singleLogical")
	check_arg(showWarning, "singleLogical")

	control_variable(x, "data.frame", mustBeThere = TRUE)

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
	if(grepl("\\.fst", dir)) {
		dir = gsub("[[:digit:]]+\\.fst", "", dir)
	} else {
		dir = paste0(gsub("([^/])$", "\\1/", dir), "slice_")
	}

	dir = gsub("/[^/]+$", "/", dir)
	file_head = paste0(dir, "slice_")
	dirExists = dir.exists(dir)
	all_files = list.files(dir, full.names = TRUE)
	all_files_fst = ggrepl("\\.fst", all_files)

	hddExists = dirExists && length(all_files_fst) > 0

	if(hddExists){
		if(!add){
			if(!replace){
				stop("A hdd data set already exists in ", dir, ". To replace it, use replace = TRUE.")
			} else {
				for(fname in ggrepl("\\.fst", all_files)) unlink(fname)
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
		stop("The class of data is not supported. Only data.frame like datasets can be written in hdd.")
	}

	if(nrow(x) == 0) return(invisible(NULL))

	if(add){

		if(isKey) stop("At the moment add = TRUE with x a hdd data base with key is not supported.")

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


	if(memoryData || is.finite(chunkMB)){

		if(!is.finite(chunkMB)){
			# this is memory data
			n_chunks = 1
		} else{
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
			first_msg = ifelse(n_chunks == 1, "1 file ; ", paste0(n_chunks, " files ; "))
			log_msg = paste0(first_msg, numberFormat(nrow(x)), " rows ; ", call_txt)
		} else {
			log_msg = paste0("#1 ; ", numberFormat(nrow(x)), " rows ; ", call_txt)
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
			info[3] = paste0(numberFormat(nrow(x)), " rows and ", ncol(x), " variables.")

			# update log
			if(is_ext_call){
				# The functions that use ext_call are:
				#	- extract with newfile, hdd_slice, merge_hdd and txt2hdd
				#	- they MUST create a new document
				#	- hence we're sure the last line contains the word "file"
				# we take the last line and update it
				log_msg = paste0(nb_files_existing, " files ; ", numberFormat(nrow(x)), " rows ; ", call_txt)
				# we replace the line
				info[grepl("^[^;]+files? ;", info)] = log_msg

			} else {
				# We add the line with information on the file nber and nber of rows
				if(n_chunks == 1){
					nb_show = nb_files_existing + 1
				} else {
					nb_show = paste0(nb_files_existing + 1, "-", nb_files_existing + n_chunks)
				}

				log_msg = paste0("#", nb_show, " ; ", numberFormat(nrow(x)), " rows ; ", call_txt)
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
#' @inheritParams hdd_merge
#'
#' @param x A hdd file.
#' @param key A character vector of the keys.
#' @param chunkMB The size of chunks used to sort the data. Default is 500MB. The bigger this number the faster the sorting is (depends on your memory available though).
#'
#'
#'
#' @examples
#'
#' \dontrun{
#' # your data set is in the hard drive, in hdd format already.
#' # Say this data is made of two variables: x and id.
#' data_hdd = hdd("path/big_data")
#'
#' # We want to create a new HDD file, sorted by id:
#' hdd_setkey(data_hdd, "id", "path/big_data_sorted")
#'
#' }
#'
hdd_setkey = function(x, key, newfile, chunkMB = 500, replace = FALSE, verbose = 1){
	# on va creer une base HDD triee
	# The operation is very simple, so we can use big chunks => much more efficient!

	# for 2+ keys, use : rowidv(DT, cols=c("x", "y"))

	# On va cree un hdd file tmp
	# un autre tmp2
	# en final, on coupera-collera dans newfile

	# key can be a data.table call? No, not at the moment

	check_arg(key, "characterVectorMbt")
	check_arg(newfile, "singleCharacterMbt")
	check_arg(chunkMB, "singleNumericGT0")
	check_arg(replace, "singleLogical")
	check_arg(verbose, "singleNumeric")

	if(missing(x)){
		stop("Argument 'x' must be a hdd object, but it is currently missing.")
	}

	if(!"hdd" %in% class(x)){
		stop("x must be a hdd object.")
	}

	if(!all(key %in% names(x))){
		stop("The key must be a variable name.")
	}

	if(dir.exists(newfile)){
		# cleaning (if necessary)
		all_files = list.files(newfile, full.names = TRUE)
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

	if(verbose > 0) message(nfiles, " files...", appendLF = FALSE)

	start = floor(seq(1, n, by = n/nfiles))
	start = start[1:nfiles]
	end = c(start[-1] - 1, n)

	tmpdir = paste0(gsub("/[^/]+$", "/", x$.fileName[1]), "tmp_hdd_setkey/")
	all_files = list.files(tmpdir, full.names = TRUE)
	for(fname in all_files) unlink(fname)

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

		write_hdd(x_mem[1:obs_mid], tmpdir, add = TRUE)
		write_hdd(x_mem[(obs_mid+1):.N], tmpdir, add = TRUE)
	}

	# finding all the file names
	all_files = list.files(tmpdir, full.names = TRUE)
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

			write_fst(x_mem[1:obs_mid], all_files_fst[ij[1]])
			write_fst(x_mem[(obs_mid+1):.N], all_files_fst[ij[2]])
		}

	}

	if(verbose > 0) message("\n", appendLF = FALSE)

	# we need to reupdate x_tmp (to have the appropriate meta information)
	x_tmp = hdd(tmpdir)
	attr(x_tmp, "key") = key

	if(nfiles != nfiles_origin){
		if(verbose > 0) message("Reshaping...", appendLF = FALSE)
		write_hdd(x_tmp, dir = newfile, replace = TRUE, chunkMB = mean(x$.size) / 1e6, call_txt = call_txt)
		if(verbose > 0) message("done")
	} else {
		write_hdd(x_tmp, dir = newfile, replace = TRUE, call_txt = call_txt)
	}

	# Cleaning the tmp directory
	files2clean = list.files(tmpdir, full.names = TRUE)
	for(fname in files2clean) unlink(fname)
	unlink(tmpdir)

}

#' Merges data to an hdd file
#'
#' This function merges in-memory/HDD data to an hdd file.
#'
#' @param x A hdd file.
#' @param y A dataset either a data.frame of a HDD object.
#' @param newfile Destination of the result, i.e., a destination folder that will receive the HDD data.
#' @param all Default is \code{FALSE}.
#' @param all.x Default is \code{all}.
#' @param all.y Default is \code{all}.
#' @param replace Default is \code{FALSE}: if the destination folder already contains data, whether to replace it.
#'
#' @examples
#'
#' \dontrun{
#'
#' y = data.frame(xx = 1:3, Species = c("virginica", "setosa", "versicolor"))
#' iris_hdd = hdd("iris_hdd")
#'
#' hdd_merge(iris_hdd, y, "iris_merged_with_y")
#'
#' }
#'
hdd_merge = function(x, y, newfile, all = FALSE, all.x = all, all.y = all, replace = FALSE, verbose){
	# Function to merge Hdd files
	# It returns a hdd file
	# x: hdd
	# y: data.frame or hdd
	# LATER: add possiblity to subset/select variables of x before evaluation

	# CONTROLS

	if(missing(x)) stop("Argument 'x' is required.")
	if(missing(y)) stop("Argument 'y' is required.")
	check_arg(newfile, "singleCharacter", "Argument 'newfile' must be a path to a directory. REASON")

	check_arg(all, "singleLogical")
	check_arg(all.x, "singleLogical")
	check_arg(all.y, "singleLogical")
	check_arg(replace, "singleLogical")
	check_arg(verbose, "singleNumeric")

	call_txt = deparse_long(match.call())

	if(!"hdd" %in% class(x)){
		stop("x must be a hdd file!")
	}

	if(missing(verbose)){
		verbose = object_size(x)/1e6 > 1000
	}

	y_hdd = FALSE
	if("hdd" %in% class(y)){
		y_hdd = TRUE
	} else if(!"data.frame" %in% class(y)){
		stop("y must be either a data.frame or a hdd file.")
	}

	names_x = names(x)
	names_y = names(y)
	by = intersect(names_x, names_y)

	if(length(by) == 0){
		stop("The two tables MUST have common variable names!")
	}


	# Formatting the repository of destination
	control_variable(newfile, "singleCharacter", mustBeThere = TRUE)
	dir = newfile
	if(grepl("\\.fst", dir)) {
		dir = gsub("[[:digit:]]+\\.fst", "", dir)
	} else {
		dir = paste0(gsub("([^/])$", "\\1/", dir), "slice")
	}

	dirDest = gsub("/[^/]+$", "/", dir)

	if(dir.exists(dirDest)){
		# cleaning (if necessary)
		all_files = list.files(dirDest, full.names = TRUE)
		all_files2clean = all_files[grepl("/(slice_[[:digit:]]+\\.fst|_hdd\\.txt)$", all_files)]
		if(length(all_files2clean) > 0){
			if(!replace) stop("The destination diretory contains existing information. To replace it use argument replace=TRUE.")
			for(fname in all_files2clean) unlink(fname)
		}
	}

	# Merging
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
				res_chunk = merge(x_chunk, y_chunk, by = by, all = all, all.x = all.x, all.y = all.y)
				if(nrow(res_chunk) > 0){
					no_obs = FALSE
					write_hdd(res_chunk, dirDest, chunkMB = Inf, add = ADD, replace = TRUE, call_txt = call_txt)
					ADD = TRUE
				}
			}

		} else {
			res_chunk = merge(x_chunk, y, by = by, all = all, all.x = all.x, all.y = all.y)
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

}



#' Transforms text data into a HDD file
#'
#' Imports text data and saves it into a HDD file. It uses \code{\link[readr]{read_delim_chunked}} to extract the data. It also allows to preprocess the data.
#'
#' @param path Path where the data is.
#' @param dirDest The destination directory, where the new HDD data should be saved.
#' @param chunkMB The chunk sizes in MB, no default.
#' @param col_names The column names, by default is uses the ones of the dataset. If the dataset lacks column names, you must provide them.
#' @param col_types The column types, in the \code{readr} fashion. You can use \code{\link{guess_col_types}} to find them.
#' @param nb_skip Number of lines to skip.
#' @param delim The delimiter. By default the function tries to find the delimiter, but sometimes it fails.
#' @param preprocessfun A function that is applied to the data before saving. Default is missing. Note that if a function is provided, it MUST return a data.frame, anything other than data.frame is ignored.
#' @param replace If the destination directory already exists, you need to set the argument \code{replace=TRUE} to overwrite all the HDD files in it.
#' @param verbose Integer. If verbose > 0, then the evolution of the importing process is reported.
#' @param ... Other arguments to be passed to \code{\link[readr]{read_delim_chunked}}, \code{quote = ""} can be interesting sometimes.
#'
#' @examples
#'
#' \dontrun{
#'
#' # Example in the context of publication data.
#' # Assume we have a large text file containing the following variables:
#' # - publication_id: unique publication identifier
#' # - abstract: the abstract of the publicaiton
#' # - ...: many other variables
#' #
#' # We want to import the data, but keep only the observations for which
#' # the abstract is written in English.
#' # We have created beforehand the function 'check_english' which checks if
#' # a text is in English language.
#'
#' # To import the original data, but only with English abstract,
#' # you can do:
#' fun = function(x){
#'   x[check_english(abstract)]
#' }
#'
#' txt2hdd("abstracts.txt", "path/pub_english", chunkMB = 1000, preprocessfun = fun)
#' }
#'
#'
txt2hdd = function(path, dirDest, chunkMB, col_names, col_types, nb_skip, delim, preprocessfun, replace = FALSE, verbose = 1, ...){
	# This function reads a large text file thanks to readr
	# and trasforms it into a hdd document

	#
	# Control
	#

	check_arg(path, "singleCharacterMbt")
	check_arg(dirDest, "singleCharacterMbt")
	check_arg(chunkMB, "singleNumericGT0Mbt")
	check_arg(col_names, "characterVector")
	check_arg(nb_skip, "singleIntegerGE0")
	check_arg(delim, "singleCharacter")
	check_arg(replace, "singleLogical")
	check_arg(verbose, "singleNumeric")


	DO_PREPROCESS = FALSE
	if(!missing(preprocessfun)){
		if(!is.function(preprocessfun)){
			stop("Argument 'preprocessfun' must be a function.")
		}
		DO_PREPROCESS = TRUE
	}

	if(!missing(col_types) && (!length(class(col_types) == 1) || class(col_types) != "col_spec")){
		stop("Argument 'col_types' must be a 'col_spec' object, obtained from, e.g., readr::cols() or readr::cols_only(), or from guess_cols_type()).")
	}

	#
	# Preliminary stuff
	#

	# sample DT to get first information
	first_lines = readr::read_lines(path, n_max = 10000)
	sample_table = data.table::fread(paste0(first_lines, collapse = "\n"))

	nb_col = ncol(sample_table)

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

	if(missing(col_names)){
		if(!is_col_names){
			stop("The text file has no header, you MUST provide ", nb_col, " column names (col_names).")
		} else {
			col_names = names(sample_table)
		}
	} else {
		if(length(col_names) != nb_col){
			stop("The variable col_names should be of length ", nb_col, ". (At the moment it is of length ", length(col_names), ".")
		}
	}

	#
	# finding out the delimiter
	#

	if(missing(delim)){
		delim = guess_delim(first_lines[1:100])
	}
	delimiter = delim


	# # The chunked readr function
	# fun2read = ifelse(isTsv, read_tsv_chunked, read_csv_chunked)

	control_variable(chunkMB, "singleNumericGT0", mustBeThere = TRUE)

	# Just for information on nber of chunks
	fileSize = file.size(path) / 1e6
	nbChunks_approx = fileSize / chunkMB
	if(verbose > 0) message("Approx. number of chunks:", ceiling(nbChunks_approx), "\n")

	# Nber of rows per extraction
	rowPerChunk = ceiling(10000 / as.numeric(object.size(sample_table) / 1e6) * chunkMB)

	# Function to apply to each chunk

	REP_PBLM = gsub("/?$", "/problems", dirDest)
	REP_DEST = dirDest
	ADD = FALSE
	IS_PBLM = FALSE

	# We check replacement
	if(dir.exists(REP_DEST)){

		# cleaning (if necessary)
		all_files = list.files(REP_DEST, full.names = TRUE)
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

	readr::read_delim_chunked(file = path, callback = funPerChunk, chunk_size = rowPerChunk, col_names = col_names, col_types = col_types, skip = nb_skip, delim = delimiter, ...)

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
#' @return
#' It returns a \code{cols} object a la \code{readr.}
#'
#' @examples
#'
#' \dontrun{
#'
#' # Find the columns of a text file
#' guess_cols("path/my_data.txt")
#'
#' }
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

	check_arg(col_names, "characterVector")
	check_arg(n, "singleIntegerGE1")

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
	all_classes = sapply(sample_dt, class)
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
		if(grepl("date", new_type)){
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
#' @param path The path to a text file containing a rectangular data set.
#'
#' @return
#' It returns a character string of length 1: the delimiter.
#'
#' @examples
#'
#' \dontrun{
#'
#' # Say you have data in the text file "path/data.txt".
#' # To guess the delimiter:
#' guess_delim("path/data.txt")
#'
#' }
#'
guess_delim = function(path){


	check_arg(path, "characterVector", "Argument path must be a valid path. REASON")

	# importing a sample

	if(length(path) == 100){
		first_lines = path
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
			names(candidate[!grepl("[[:alnum:]\"'-]", names(candidate))])
		}
		l1_candidate = find_candidate(first_lines[1])

		delim = ifelse(length(l1_candidate) == 1, l1_candidate, NULL)
		if(length(l1_candidate) > 1){
			candid_all = lapply(first_lines, find_candidate)
			t_cand = table(unlist(candid_all))
			t_cand = names(t_cand)[t_cand == length(first_lines)]
			if(length(t_cand) == 1){
				delim = t_cand
			}
		}

		if(is.null(delim)){
			stop("Could not determine the delimiter. Here is the first line:\n", first_lines[1])
		} else {
			if(delim == ","){
				info = "CSV"
			} else if(delim == "\t"){
				info = "TSV"
			}
			# cat("Delimiter: ", info, "\n")
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
#' @param path Path linking to the text file.
#' @param onlyLines Default is \code{FALSE}. If \code{TRUE}, then the first \code{n} lines are directly displayed without formatting.
#' @param n Integer. The number of lines to extract from the file. Default is 100 or 5 if \code{onlyLine = TRUE}.
#' @param view Logical, default it \code{TRUE}: whether the data should be displayed on the viewer. Only when \code{onlyLines = FALSE}.
#'
#' @return
#' Returns the data invisibly.
#'
#' @examples
#'
#' \dontrun{
#'
#' # Let's have a look at the first observations from "path/data.txt")
#' peek("path/data.txt")
#'
#' # Sometimes, it can be useful to look at the unformatted lines:
#' peek("path/data.txt", onlyLines = TRUE)
#'
#' }
#'
peek = function(path, onlyLines = FALSE, n, view = TRUE){

	# Controls

	check_arg(path, "singleCharacterMbt")
	check_arg(onlyLines, "singleLogical")
	check_arg(view, "singleLogical")
	check_arg(n, "singleIntegerGE1")

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

		# cat(first_lines, sep = "\n")

		return(first_lines)
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
			names(candidate[!grepl("[[:alnum:]\"'-/]", names(candidate))])
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

		if(is.null(delim)){
			message("Could not determine the delimiter. Here is the first line:\n")
			message(first_lines[1])
		} else {
			if(delim == ","){
				delim = "CSV"
			} else if(delim == "\t"){
				delim = "TSV"
			}
			message("Delimiter:", delim)
		}
	}

	dt_name = paste0("peek_", gsub("\\..+", "", gsub("^.+/", "", path)))

	if(view) myView(sample_dt, dt_name)
	invisible(sample_dt)
}


####
#### S3 methods ####
####

#' Dimension of a hdd object
#'
#' Gets the dimension of a hard drive data set (hdd).
#'
#' @param x A \code{hdd} object.
#'
#' @return
#' It returns a vector of length 2 containing the number of rows and the columns.
#'
#' @examples
#'
#' \dontrun{
#' # your data set is in the hard drive, in hdd format already
#' data_hdd = hdd("path/my_big_data")
#' dim(data_hdd)
#'
#' }
#'
dim.hdd = function(x){
	n = length(x$.size)
	c(x$.row_cum[n], x$.ncol[1])
}

#' Variables names of an hdd object
#'
#' Gets the variable names of a hard drive data set (hdd).
#'
#' @inheritParams dim.hdd
#'
#' @return
#' A character vector.
#'
#' @examples
#'
#' \dontrun{
#' # your data set is in the hard drive, in hdd format already
#' data_hdd = hdd("path/my_big_data")
#' names(data_hdd)
#'
#' }
#'
names.hdd = function(x){
	x_tmp = fst(x$.fileName[1])
	names(x_tmp)
}

#' Print method for hdd objects
#'
#' This functions displays the first and last lines of a hard drive data set (hdd).
#'
#' @inheritParams dim.hdd
#'
#' @param ... Not currently used.
#'
#' @return
#' Nothing is returned.
#'
#' @examples
#'
#' \dontrun{
#' # your data set is in the hard drive, in hdd format already
#' data_hdd = hdd("path/my_big_data")
#' print(data_hdd)
#'
#' }
#'
print.hdd = function(x, ...){
	n = nrow(x)
	if(n < 8){
		print(x[])
	} else {
		quoi = as.data.frame(rbindlist(list(head(x, 4), tail(x, 3))))
		quoi = formatTable(quoi)
		quoi[4, ] = rep(" " , ncol(quoi))
		nmax = tail(x$.row_cum, 1)
		dmax = log10(nmax) + 1
		row.names(quoi) = c(1:3, substr('------------', 1, max(3, 4/3*dmax)), numberFormat(nmax - 2:0))
		print(quoi)
	}
}

#' Summary information for hdd objects
#'
#' Provides summary information -- i.e. dimension, size on disk, path, number of slices -- of hard drive data sets (hdd).
#'
#' @inheritParams dim.hdd
#'
#' @param object A hdd object.
#' @param ... Not currently used.
#'
#'
#' @examples
#'
#' \dontrun{
#' # your data set is in the hard drive, in hdd format already
#' data_hdd = hdd("path/my_big_data")
#' summary(data_hdd)
#'
#' }
#'
summary.hdd = function(object, ...){
	n = length(object$.size)
	cat("Hard drive data of ", osize(object), " Made of ", n, " files.\n", sep = "")

	key = attr(object, "key")
	if(!is.null(key)){
		cat("Sorted by:", paste0(key, collapse = ", "), "\n")
	}

	cat("Location: ", gsub("/[^/]+$", "/", object$.fileName[1]), "\n", sep = "")
	nb = object$.row_cum[n]
	nb = numberFormat(nb)
	cat(nb, " lines, ", object$.ncol[1], " variables.\n", sep = "")

}






























