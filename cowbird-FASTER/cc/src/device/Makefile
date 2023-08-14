CC=g++
LIBS=-libverbs -lrdmacm -pthread -ltbb

SERVER_DEPS = cowbird_p4_server.cc cowbird_spot_server.cc one_sided_server.cc
SERVER_OBJ = cowbird_p4_server cowbird_spot_server one_sided_server

.PHONY: all clean

all: $(SERVER_OBJ)

cowbird_p4_server: cowbird_p4_server.cc
	$(CC) -o $@ $< $(LIBS)

cowbird_spot_server: cowbird_spot_server.cc
	$(CC) -o $@ $< $(LIBS)

one_sided_server: one_sided_server.cc
	$(CC) -o $@ $< $(LIBS)

clean:
	rm -f $(SERVER_OBJ)
