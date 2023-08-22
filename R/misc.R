#----------------------------------------------#
# Author: Laurent Berge
# Date creation: Thu Oct 17 22:33:52 2019
# ~: Misc funs used in HDD
#----------------------------------------------#


####
#### Setters/Getters ####
####

#' Sets/gets the size cap when extracting hdd data
#'
#' Sets/gets the default size cap when extracting HDD variables with \code{\link[hdd]{cash-.hdd}} or when importing full HDD data sets with \code{\link[hdd]{readfst}}.
#'
#' @param sizeMB Size cap in MB. Default is 1000.
#'
#' @details
#' In \code{\link[hdd]{readfst}}, if the size expected size exceeds the cap, then an error is raised, which can be bypassed by using the argument \code{confirm}.
#'
#' @return
#' The size cap, a numeric scalar.
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
#' \donttest{
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
#' pl = base_hdd$Sepal.Length
#'
#' # To extract the variable without changing the cap:
#' pl = base_hdd[, Sepal.Length] # => no size control is performed
#'
#' # Resetting the default cap
#' setHdd_extract.cap()
#' }
#'
#'
setHdd_extract.cap = function(sizeMB = 1000){

	check_arg(sizeMB, "numeric scalar GT{0}")

	options("hdd_extract.cap" = sizeMB)
}

#' @rdname setHdd_extract.cap
"getHdd_extract.cap"

getHdd_extract.cap = function(){

	x = getOption("hdd_extract.cap")
	if(length(x) != 1 || !is.numeric(x) || is.na(x) || x < 0){
		stop("The value of getOption(\"hdd_extract.cap\") is currently not legal. Please use function setHdd_extract.cap to set it to an appropriate value.")
	}

	x
}

####
#### HDD utilities ####
####

object_size = function(x){
	if(inherits(x, "hdd")){
		res = tail(x$.size_cum, 1)
	} else {
		res = utils::object.size(x)
	}
	res
}

numID = function(x){
	if(ncol(x) == 1){
		x = x[[1]]
	} else {
		# we recreate the ids using rowid
		vars = names(x)
		x[, "xxOBSxx" := 1:.N]
		setorderv(x, vars)

		x[, "xxNEWIDxx" := cumsum(sign(1 - c(-100, diff(rowidv(x, vars)))))]
		setorderv(x, "xxOBSxx")
		x = x[["xxNEWIDxx"]]
	}
	return(x)
}


find_split = function(x){
	# This function to find where to cut a file.
	# we do not want the same value to be in two different files

	x = numID(x)

	n = length(x)
	obs_mid = round(n/2)
	v_left = x[obs_mid]
	v_right = x[obs_mid + 1]

	if(v_left != v_right){
		return(obs_mid)
	} else {
		# then we need to split differently
		if(v_left != x[n]){
			obs_mid = obs_mid + which.max(x[(obs_mid+1):n] != v_left) - 1
		} else if(v_left != x[1]){
			obs_mid = which.max(x[1:obs_mid] == v_left) - 1
		} else {
			# stop("No middle value found. The two files could not be split, revise the code to allow merging the files automatically.")
			return(NULL)
		}
		return(obs_mid)
	}
}

find_n_split = function(x, key, nfiles){
	# finds where to cut files.
	# we do not want the same key value to be in two different files
	# nfiles: the new number of files
	# returns a vector of the beginning observations

	nfiles_origin = length(x$.nrow)
	n_all = x$.row_cum[nfiles_origin]

	# preliminary starting point of each file
	start = floor(seq(1, n_all, by = n_all/nfiles))
	start = c(start[1:nfiles], n_all)

	# increment: 5% of file size
	DELTA = max(floor(n_all/nfiles * 0.05), 1)

	# We find the right starting point of each file
	for(i in 2:nfiles){
		start_tmp = start[i]
		id = numID(x[start_tmp + 0:1, j = key, with = FALSE])
		id_left = id[1]

		if(id[1] != id[2]){
			# OK! => nothing to do
			next
		} else {
			# we go to the right
			k = 1
			while(TRUE){
				if(start_tmp + k*DELTA > start[i + 1]){
					stop("The hdd file with key could not be split in ", nfiles, " documents (because of too many identical keys). Reduce the number of documents.")
				}

				id = numID(x[start_tmp + c(0, k*DELTA), key, with = FALSE])
				if(id[1] != id[2]){
					# we find the right point!
					id = numID(x[start_tmp + c(0, ((DELTA*(k-1)+1):(DELTA*k))), key, with = FALSE])
					start_new = start_tmp + DELTA*(k-1) + which.max(id[-1] != id[1])
					start[i] = start_new
					break
				} else {
					k = k + 1
				}
			}
		}
	}

	start_final = start[1:nfiles]
	return(start_final)
}

obs = function(x, file){
	# Finds the observation numbers of a hdd document by file

	if(!inherits(x, "hdd")){
		stop("x must be a hdd file.")
	}

	if(missing(file)){
		stop("file must be provided.")
	}

	n = length(x$.nrow)
	check_arg(file, "integer vector GT{0}")
	if(any(file > n)){
		stop("file cannot exceed ", n, ".")
	}


	row_cum = x$.row_cum
	end = row_cum
	start = (1 + c(0, row_cum))

	# creation of the vector
	res = list()
	index = 0
	for(i in file){
		index = index + 1
		res[[index]] = start[i]:end[i]
	}

	unlist(res)
}


clean_path = function(x){
	# we just want proper /
	x = gsub("\\", "/", x, fixed = TRUE)
	gsub("/+", "/", x)
}

####
#### Other Utilities ####
####

addCommas = function(x){

	addCommas_single = function(x){
		# Cette fonction ajoute des virgules pour plus de
		# visibilite pour les (tres longues) valeurs de vraisemblance

		# This is an internal function => the main is addCommas

		if(!is.finite(x) || log10(abs(x)) < 0) return(as.character(x))

		s = sign(x)
		x = abs(x)
		decimal = x - floor(x)
		if (decimal > 0){
			dec_string = substr(decimal, 2, 4)
		} else {
			dec_string = ""
		}

		entier = sprintf("%.0f", floor(x))
		quoi = rev(strsplit(entier, "")[[1]])
		n = length(quoi)
		sol = c()
		for (i in 1:n) {
			sol = c(sol, quoi[i])
			if (i%%3 == 0 && i != n) sol = c(sol, ",")
		}
		res = paste0(ifelse(s == -1, "-", ""), paste0(rev(sol), collapse = ""),
					 dec_string)
		res
	}

	sapply(x, addCommas_single)
}



formatTable = function(x, d=2, r=1){
	# This function takes in a data.frame and formats all the columns

	# Checks:
	if(checkVector(x) || is.matrix(x)){
		x_format = as.data.frame(x)
	} else if(is.data.table(x)){
		x_format = copy(x)
	} else if(!is.data.frame(x)){
		stop("Argument 'x' must be a data.frame!")
	} else {
		x_format = x
	}

	isChar = !sapply(x_format, is.numeric)

	for(i in which(isChar)){
		x_format[[i]] = .cleanPCT(as.character(x_format[[i]]))
	}

	# the formatting of numbers
	for(i in which(!isChar)){
		x_format[[i]] = numberFormat(x_format[[i]], d=d, r=r)
	}

	return(x_format)
}


checkVector = function(x){
	# it seems that when you subselect in data.table
	# sometimes it does not yield a vector
	# so i cannot use is.vector to check the consistency

	if(is.vector(x)){
		return(TRUE)
	} else {
		if(any(class(x) %in% c("integer", "numeric", "character", "factor", "Date")) && is.null(dim(x))){
			return(TRUE)
		}
	}
	return(FALSE)
}


mysignif = function(x, d=2, r=1){

	# The core function
	mysignif_single = function(x, d, r){
		if(is.na(x)) return(NA)

		if(abs(x)>=10**(d-1)) return(round(x, r))
		else return(signif(x, d))
	}

	# the return
	sapply(x, mysignif_single, d=d, r=r)
}

numberFormat = function(x, d=2, r=1){
	numb_char = as.character(x)
	quiHigh = (abs(x) >= 1e4 & !is.na(x))
	if(sum(quiHigh) > 0){
		numb_char[quiHigh] = addCommas(mysignif(x[quiHigh], d=d, r=r))
	}

	if(sum(!quiHigh) > 0){
		numb_char[!quiHigh] = as.character(mysignif(x[!quiHigh], d=d, r=r))
	}

	numb_char
}

.cleanPCT = function(x){
	# changes % into \% => to escape that character in Latex
	gsub("%", "\\%", x, fixed = TRUE)
}


osize = function(x){
	size = as.numeric(object_size(x))
	n = log10(size)

	if(n < 3){
		# cat(size, " Octets.\n")
		res = paste0(size, " Bytes.")
	} else if(n < 6){
		# cat(mysignif(size/1000, 20, 2), " Ko.\n")
		res = paste0(mysignif(size/1000, 3, 0), " KB.")
	} else {
		# cat(addCommas(mysignif(size/1000000, 20, 2)), " Mo.\n")
		res = paste0(addCommas(mysignif(size/1000000, 3, 0)), " MB.")
	}

	class(res) = "osize"

	res
}

ggrepl = function(pattern, x){
	x[grepl(pattern, x, perl = TRUE)]
}


deparse_long = function (x){
	dep_x = deparse(x)
	if (length(dep_x) == 1) {
		return(dep_x)
	} else {
		return(paste(gsub("^ +", "", dep_x), collapse = ""))
	}
}




















































































































































































































































































