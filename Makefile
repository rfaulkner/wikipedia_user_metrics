#
# E3_analysis Makefile
# ~~~~~~~~~~~~~~~~~~~~
#
# Shortcuts for various tasks.
#
# :copyright: (c) 2012 by the Wikimedia Foundation, see AUTHORS for more details.
# :license: BSD, see LICENSE for more details.
#

documentation:
	@(cd docs; make html)

release:
	python scripts/make-release.py

test:
	python setup.py test

coverage:
	@(nosetests $(TEST_OPTIONS) --with-coverage --cover-package=sartoris --cover-html --cover-html-dir=coverage_out $(TESTS))
