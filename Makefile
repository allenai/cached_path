.PHONY : docs
docs :
	rm -rf docs/build/
	sphinx-autobuild -b html --watch cached_path/ docs/source/ docs/build/
