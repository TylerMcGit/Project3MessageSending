#include<unistd.h>
#include<sys/types.h>
#include<stdio.h>
#include<stdlib.h>
#include<iostream>
#include<sys/ipc.h>
#include<sys/shm.h>
#include<sys/msg.h>
using namespace std;
 
struct msgbuffer { //same as in oss
	long mtype; //who the message is for
	int intData; //1 = still running, 0 = going to terminate
};
 
const int BUFF_SZ = sizeof(int)*2;
int shm_key;
int shm_id;
 
int *initializeMemory();
 
int initializeQueue();
 
int main(int argc, char** argv) {
	if(argc < 3) {
		cout << "Usage: ./worker seconds nanoseconds" << endl;
		return EXIT_FAILURE;
	}
 
	int *clock = initializeMemory();
	int *sec = &(clock[0]);
	int *nano = &(clock[1]);
	int msq_id = initializeQueue();
 
	int stop_sec = *sec + atoi(argv[1]);
	int stop_nano = *nano + atoi(argv[2]);
	if(stop_nano >= 1000000000) { //carry extra nanoseconds over into seconds
		stop_sec++;
		stop_nano -= 1000000000;
	}
 
	cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << " SysClockS: " << *sec << " SysclockNano: " << *nano << endl
	     << "TermTimeS: " << stop_sec << " TermTimeNano: " << stop_nano << endl
	     << "--Just Starting" << endl;
 
	int count = 0; //messages received from oss
	int done = 0;
	struct msgbuffer buf;
	do {
		//wait for oss to send a message to this worker (mtype is our pid)
		if(msgrcv(msq_id, &buf, sizeof(buf) - sizeof(long), getpid(), 0) == -1) {
			perror("worker: msgrcv");
			exit(1);
		}
		count++;
 
		//check the clock to see if it is time to terminate
		if(*sec > stop_sec || (*sec == stop_sec && *nano >= stop_nano))
			done = 1;
 
		buf.mtype = getppid(); //send back to oss only
		if(done) {
			buf.intData = 0;
			if(msgsnd(msq_id, &buf, sizeof(buf) - sizeof(long), 0) == -1) {
				perror("worker: msgsnd");
				exit(1);
			}
			cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << " SysClockS: " << *sec << " SysclockNano: " << *nano << endl
			     << "TermTimeS: " << stop_sec << " TermTimeNano: " << stop_nano << endl
			     << "--Terminating after sending message back to oss after " << count << " received messages." << endl;
		}
		else {
			cout << "WORKER PID:" << getpid() << " PPID:" << getppid() << " SysClockS: " << *sec << " SysclockNano: " << *nano << endl
			     << "TermTimeS: " << stop_sec << " TermTimeNano: " << stop_nano << endl;
			if(count == 1)
				cout << "--1 message received from oss" << endl;
			else
				cout << "--" << count << " messages received from oss" << endl;
			buf.intData = 1;
			if(msgsnd(msq_id, &buf, sizeof(buf) - sizeof(long), 0) == -1) {
				perror("worker: msgsnd");
				exit(1);
			}
		}
	} while(!done);
 
	shmdt(clock); //detach only, oss frees the memory and the queue
	return EXIT_SUCCESS;
}
 
int *initializeMemory() {
	int shm_key = ftok("oss.c",0);
	if (shm_key == -1) {
		fprintf(stderr,"Child:... Error in ftok\n");
		exit(1);
	}
	int shm_id = shmget(shm_key,BUFF_SZ,0700);
	if (shm_id == -1) {
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
 
int initializeQueue() {
	key_t msg_key = ftok("oss.c",1); //same key oss used
	if (msg_key == -1) {
		fprintf(stderr,"Child:... Error in ftok for message queue\n");
		exit(1);
	}
	int msq_id = msgget(msg_key, 0644); //no IPC_CREAT, oss already made it
	if (msq_id == -1) {
		fprintf(stderr,"Child:... Error in msgget\n");
		exit(1);
	}
	return msq_id;
}
 
