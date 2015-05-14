
MAIN      = main
BIBINPUTS = bibs

CLEANFILES = *~ *.aux *.log *.bbl *.blg *.dvi *.ps *.pdf

all:	latex
#	pdflatex ${MAIN}.tex
	dvips -t letter -f ${MAIN} > ${MAIN}.ps
	ps2pdf ${MAIN}.ps > ${MAIN}.pdf

latex:
	TEXINPUTS=${TEXINPUTS}:${TEXD} latex ${MAIN}.tex
	BIBINPUTS=${BIBINPUTS} bibtex ${MAIN}
	TEXINPUTS=${TEXINPUTS}:${TEXD} latex ${MAIN}.tex
	TEXINPUTS=${TEXINPUTS}:${TEXD} latex ${MAIN}.tex

bibtex:
	bibtex ${MAIN}

cleantest:
	ls -R ${CLEANFILES}

clean:
	rm -rf ${CLEANFILES}
	#cd build; rm -f *
	find ./ -name \*~ -exec rm -rf \{\} \;

