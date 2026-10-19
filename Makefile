all: oss worker

oss: oss.c
	g++ -Wall -g -o oss oss.c

worker: worker.c
	g++ -Wall -g -o worker worker.c

clean:
	rm -f oss worker
