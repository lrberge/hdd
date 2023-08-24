


.onLoad <- function(libname, pkgname){
	# setting the two options

	setHdd_extract.cap()
	
	if(is_r_check()){
		data.table::setDTthreads(1)
	}

	invisible()
}
