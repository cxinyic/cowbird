.PHONY: clean

CFLAGS  := -Wall -g
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := cowbird_server baseline_server

all: ${APPS}


clean:
	rm -f ${APPS}

