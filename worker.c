#include<unistd.h>
#include<sys/types.h>
#include<stdio.h>
#include<stdlib.h>
#include<iostream>
#include<sys/ipc.h>
#include<sys/shm.h>
using namespace std;

const int BUFF_SZ = sizeof(int)*2;
int shm_key;
int shm_id;

int *initializeMemory();

int main(int argc, char** argv) {
        if(argc < 3) {
                cout << "Usage: ./worker seconds nanoseconds" << endl;
                return EXIT_FAILURE;
        }

        cout << "Worker starting, PID:" << getpid() << " PPID:" << getppid() << endl
             << "Called with:" << endl
             << "Interval: " << argv[1] << " seconds, " << argv[2] << " nanoseconds" << endl;

        int *clock = initializeMemory();
        int *sec = &(clock[0]);
        int *nano = &(clock[1]);

        int stop_sec = *sec + atoi(argv[1]);
        int stop_nano = *nano + atoi(argv[2]);
        if(stop_nano >= 1000000000) { //carry extra nanoseconds over into seconds
                stop_sec++;
                stop_nano -= 1000000000;
        }

        cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << endl
	     << "SysClockS: " << *sec << " SysclockNano: " << *nano << " TermTimeS: " << stop_sec << " TermTimeNano: " << stop_nano << endl
             << "--Just Starting" << endl;

        int count = 0;
        int count_sec = *sec + 1;
        while(*sec < stop_sec || (*sec == stop_sec && *nano < stop_nano)) { //keep going until the clock passes the stop time
                if(*sec >= count_sec) { //the seconds changed
                        count++;
                        count_sec++;
                        cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << endl
                             << "SysClockS: " << *sec << " SysclockNano: " << *nano << " TermTimeS: " << stop_sec << " TermTimeNano: " << stop_nano << endl
                             << "--" << count << " seconds have passed since starting" << endl;
                }
        }                                                                   
        cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << endl
             << "SysClockS: " << *sec << " SysclockNano: " << *nano << " TermTimeS: " << stop_sec << " TermTimeNano: " << stop_nano << endl
             << "--Terminating" << endl;                                    
        shmdt(clock); //detach only, oss frees the memory
        return EXIT_SUCCESS;
}

int *initializeMemory() {
        int shm_key = ftok("oss.c",0);
        if (shm_key <= 0 ) {
                fprintf(stderr,"Child:... Error in ftok\n");
                exit(1);
        }
        int shm_id = shmget(shm_key,BUFF_SZ,0700);
        if (shm_id <= 0 ) {
                fprintf(stderr,"child:... Error in shmget\n");
                exit(1);
	}
        int *clock = (int *)shmat(shm_id,0,0);
        if (clock == (int *)-1) { //shmat returns -1 on error, not 0
                fprintf(stderr,"Child:... Error in shmat\n");
                exit(1);
        }
        return clock;
}
