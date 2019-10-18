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
#' Sets/gets the default size cap when extracting \code{hdd} data. If the size exceeds the cap, then an error is raised, which can be bypassed by using the argument \code{confirm}.
#'
#' @param sizeMB Size cap in MB. Default to 1000.
#'
#' @return
#' The size cap, a numeric scalar.
#'
setHdd_extract.cap = function(sizeMB = 1000){

	if(length(sizeMB) != 1 || !is.numeric(sizeMB) || is.na(sizeMB) || sizeMB < 0){
		stop("Argument sizeMB must be a positive scalar.")
	}

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

setHdd_extract.cap()



####
#### Utilities ####
####


addCommas = function(x){

	addCommas_single = function(x){
		# Cette fonction ajoute des virgules pour plus de
		# visibilite pour les (tres longues) valeurs de vraisemblance

		# This is an internal function => the main is addCommas

		if(!is.finite(x)) return(as.character(x))

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
		if(class(x) %in% c("integer", "numeric", "character", "factor", "Date") && is.null(dim(x))){
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

enumerate_words = function(x, endVerb = c("is", "no", "contain"), addS = FALSE){
	# This function writes:
	# "var1, var2 and var 3 are"
	# or "var1 is"

	endVerb = match.arg(endVerb)

	n = length(x)

	endWord = switch(endVerb,
					 is = ifelse(n == 1, " is", " are"),
					 no = "",
					 contain = ifelse(n == 1, " contains", " contain"))

	if(addS){
		startWord = ifelse(n == 1, " ", "s ")
	} else {
		startWord = ""
	}

	if(n == 1){
		res = paste0(startWord, x, endWord)
	} else {
		res = paste0(startWord, paste0(x[-n], collapse = ", "), " and ", x[n], endWord)
	}

	res
}

osize = function(x){
	size = as.numeric(object_size(x))
	n = log10(size)

	if(n < 3){
		# cat(size, " Octets.\n")
		res = paste0(size, " Octets.")
	} else if(n < 6){
		# cat(mysignif(size/1000, 20, 2), " Ko.\n")
		res = paste0(mysignif(size/1000, 3, 0), " Ko.")
	} else {
		# cat(addCommas(mysignif(size/1000000, 20, 2)), " Mo.\n")
		res = paste0(addCommas(mysignif(size/1000000, 3, 0)), " Mo.")
	}

	class(res) = "osize"

	res
}

ggrepl = function(pattern, x){
	x[grepl(pattern, x, perl = TRUE)]
}


####
#### CONTROL ####
####


control_variable = function(x, myType, prefix, name, charVec, mustBeThere = FALSE){
	# Format of my types:
	#   - single => must be of lenght one
	#   - Vector => must be a vector
	#   - Matrix => must be a matrix
	#   - GE/GT/LE/LT: greater/lower than a given value
	#   - predefinedType => eg: numeric, integer, etc
	#   - match.arg => very specific => should match the charVec
	#   - noNA: NAs not allowed
	#   - null: null type allowed
	# If there is a parenthesis => the class must be of specified types:
	# ex: "(list, data.frame)" must be a list of a data.frame

	ignore.case = TRUE

	if(missing(prefix)){
		msg = deparse(sys.calls()[[sys.nframe()-1]])[1] # call can have svl lines
		if(length(msg) > 1) browser()
		nmax = 40
		if(nchar(msg) > nmax) msg = paste0(substr(msg, 1, nmax-1), "...")
		prefix = paste0(msg, ": ")
	}

	x_name = deparse(substitute(x))
	if(missing(name)) name = x_name

	firstMsg = paste0(prefix, "The argument '", name, "' ")

	if(missing(x)){
		if(mustBeThere){
			stop(firstMsg, "is missing => it must be provided.", call. = FALSE)
		} else {
			return(invisible(NULL))
		}
	}


	# simple function to extract a pattern
	# ex: if my type is VectorIntegerGE1 => myExtract("GE[[:digit:]]+","VectorIntegerGE1") => 1
	myExtract = function(expr, text, trim=2){
		start = gregexpr(expr,text)[[1]] + trim
		length = attr(start,"match.length") - trim
		res = substr(text,start,start+length-1)
		as.numeric(res)
	}

	#
	# General types handling
	#

	loType = tolower(myType)

	# null type is caught first
	if(grepl("null", loType)){
		if(is.null(x)) return(invisible(NULL))
	}

	if(grepl("single", loType)){
		if(length(x) != 1) stop(firstMsg,"must be of length one.", call. = FALSE)
	}

	if(grepl("vector", loType)){
		if(!checkVector(x)) stop(firstMsg,"must be a vector.", call. = FALSE)
		if(is.list(x)) stop(firstMsg,"must be a vector (and not a list).", call. = FALSE)
	}

	res = checkTheTypes(loType, x)
	if(!res$OK) stop(firstMsg,res$message, call. = FALSE)

	# # INTEGER is a restrictive type that deserves some explanations (not included in getTheTypes)
	# if(grepl("integer",loType)){
	#     if(grepl("single",loType)){
	#         if(!is.numeric(x)) stop(firstMsg,"must be an integer (right now it is not even numeric).", call. = FALSE)
	#         if(!(is.integer(x) || x%%1==0)) stop(firstMsg,"must be an integer.", call. = FALSE)
	#     } else {
	#         if(!is.numeric(x)) stop(firstMsg,"must be composed of integers (right now it is not even numeric).", call. = FALSE)
	#         if(!(is.integer(x) || all(x%%1==0))) stop(firstMsg,"must be composed of integers.", call. = FALSE)
	#     }
	# }

	if(grepl("nona", loType)){
		if(any(is.na(x))){
			stop(firstMsg,"contains NAs, this is not allowed.", call. = FALSE)
		}
	}

	# GE: greater or equal // GT: greater than // LE: lower or equal // LT: lower than
	if(is.numeric(x)){
		x = x[!is.na(x)]

		if(grepl("ge[[:digit:]]+",loType)){
			n = myExtract("ge[[:digit:]]+", loType)
			if( !all(x>=n) ) stop(firstMsg,"must be greater than, or equal to, ", n, ".", call. = FALSE)
		}
		if(grepl("gt[[:digit:]]+",loType)){
			n = myExtract("gt[[:digit:]]+", loType)
			if( !all(x>n) ) stop(firstMsg,"must be strictly greater than ", n, ".", call. = FALSE)
		}
		if(grepl("le[[:digit:]]+",loType)){
			n = myExtract("le[[:digit:]]+", loType)
			if( !all(x<=n) ) stop(firstMsg,"must be lower than, or equal to, ", n, ".", call. = FALSE)
		}
		if(grepl("lt[[:digit:]]+",loType)){
			n = myExtract("lt[[:digit:]]+", loType)
			if( !all(x<n) ) stop(firstMsg,"must be strictly lower than ", n, ".", call. = FALSE)
		}
	}

	#
	# Specific Types Handling
	#

	if(grepl("match.arg", loType)){
		if(ignore.case){
			x = toupper(x)
			newCharVec = toupper(charVec)
		} else {
			newCharVec = charVec
		}

		if( is.na(pmatch(x, newCharVec)) ){
			n = length(charVec)
			if(n == 1){
				msg = paste0("'",charVec,"'")
			} else {
				msg = paste0("'", paste0(charVec[1:(n-1)], collapse="', '"), "' or '",charVec[n],"'")
			}
			stop(firstMsg,"must be one of:\n",msg,".", call. = FALSE)
		} else {
			qui = pmatch(x, newCharVec)
			return(charVec[qui])
		}
	}
}

matchTypeAndSetDefault = function(myList, myDefault, myTypes, prefix){
	# Cette fonction:
	#   i) verifie que tous les elements de la liste sont valides
	#   ii) mes les valeurs par defauts si elles certaines valeurs sont manquantes
	#   iii) Envoie des messages d'erreur si les typages ne sont pas bons
	# En fait cette fonction "coerce" myList en ce qu'il faudrait etre (donne par myDefault)

	# 1) check that the names of the list are valid
	if(is.null(myList)) myList = list()
	list_names = names(myList)

	if(length(list_names)!=length(myList) || any(list_names=="")){
		stop(prefix,"The elements of the list should be named.", call. = FALSE)
	}

	obj_names = names(myDefault)

	isHere = pmatch(list_names,obj_names)

	if(anyNA(isHere)){
		if(sum(is.na(isHere))==1) stop(prefix, "The following argument is not defined: ",paste(list_names[is.na(isHere)],sep=", "), call. = FALSE)
		else stop(prefix, "The following arguments are not defined: ",paste(list_names[is.na(isHere)],sep=", "), call. = FALSE)
	}

	# 2) We set the default values and run Warnings
	res = list()
	for(i in 1:length(obj_names)){
		obj = obj_names[i]
		qui = which(isHere==i) # qui vaut le numero de l'objet dans myList
		type = myTypes[i] # we extract the type => to control for "match.arg" type
		if(length(qui)==0){
			# we set to the default if it's missing
			if(type == "match.arg") {
				res[[obj]] = myDefault[[i]][1]
			} else {
				res[[obj]] = myDefault[[i]]
			}
		} else {
			# we check the integrity of the value
			val = myList[[qui]]
			if(type == "match.arg"){
				# If the value is to be a match.arg => we use our controls and not
				# directly the one of the function match.arg()
				charVec = myDefault[[i]]
				control_variable(val, "singleCharacterMatch.arg", prefix, obj, charVec)
				val = match.arg(val, charVec)
			} else {
				control_variable(val, type, prefix, obj)
			}

			res[[obj]] = val
		}
	}

	return(res)
}



checkTheTypes = function(str, x){
	# This function takes in a character string describing the types of the
	# element x => it can be of several types

	# types that are controlled for:
	allTypes = c("numeric", "integer", "character", "logical", "list", "data.frame", "matrix", "factor", "formula")

	OK = FALSE
	message = c()

	for(type in allTypes){

		if(grepl(type, str)){

			# we add the type of the control
			message = c(message, type)

			if(type == "numeric"){
				if(!OK & is.numeric(x)){
					OK = TRUE
				}
			} else if(type == "integer"){
				if(is.numeric(x) && (is.integer(x) || all(x%%1==0))){
					OK = TRUE
				}
			} else if(type == "character"){
				if(is.character(x)){
					OK = TRUE
				}
			} else if(type == "logical"){
				if(is.logical(x)){
					OK = TRUE
				}
			} else if(type == "list"){
				if(is.list(x)){
					OK = TRUE
				}
			} else if(type == "data.frame"){
				if(is.data.frame(x)){
					OK = TRUE
				}
			} else if(type == "matrix"){
				if(is.matrix(x)){
					OK = TRUE
				}
			} else if(type == "factor"){
				if(is.factor(x)){
					OK = TRUE
				}
			}  else if(type == "formula"){
				if(length(class(x)) == 1 && class(x) == "formula"){
					OK = TRUE
				}
			}
		}

		if(OK) break
	}

	if(length(message) == 0){
		OK = TRUE #ie there is no type to be searched
	} else if(length(message) >= 3){
		n = length(message)
		message = paste0("must be of type: ",  paste0(message[1:(n-1)], collapse = ", "), " or ", message[n], ".")
	} else {
		message = paste0("must be of type: ",  paste0(message, collapse = " or "), ".")
	}


	return(list(OK=OK, message=message))
}


































































































































































































































































































