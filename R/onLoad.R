


.onLoad <- function(libname, pkgname){
	# setting the two options

	setHdd_extract.cap()
	
	if(is_r_check()){
		data.table::setDTthreads(1)
		fst::threads_fst(1)
		options(readr.num_threads = 1)
	}

	invisible()
}
