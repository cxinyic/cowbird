.PHONY: clean

CFLAGS  := -Wall -g
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := cowbird_client baseline_client


all: ${APPS}


clean:
	rm -f ${APPS}

