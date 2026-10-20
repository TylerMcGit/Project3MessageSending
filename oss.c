#include<unistd.h>
#include<sys/types.h>
#include<stdio.h>
#include<stdlib.h>
#include<string>
#include<iostream>
#include<fstream>
#include<time.h>
#include<sys/wait.h>
#include<sys/ipc.h>
#include<sys/shm.h>
#include<sys/msg.h>
#include<signal.h>
using namespace std;
 
struct PCB {
	int occupied; // either true or false
	pid_t pid; // process id of this child
	int startSeconds; // time when it was forked
	int startNano; // time when it was forked
	int endingTimeSeconds; // estimated time it should end
	int endingTimeNano; // estimated time it should end
	int messagesSent; // total times oss sent a message to it
};
 
struct msgbuffer { //what gets sent through the message queue
	long mtype; //who the message is for (the pid of the process that should get it)
	int intData; //1 = still running, 0 = going to terminate
};
 
const int BUFF_SZ = sizeof(int)*2;
int shm_key;
int shm_id;
int msq_id;
struct PCB p[20];
ofstream logfile;
 
int parse(int argc, char *argv[], int &proc, int &simul, float &time_limit, float &interval, string &log_name);
 
void incrementClock(int *sec, int *nano, int proc_running);
 
int *initializeMemory();
 
void initializeQueue();
 
void signalHandler(int sig);
 
void printOut(const char *line);
 
void printTable(int sec, int nano);
 
int main(int argc, char* argv[]) {
	int proc_running = 0, total = 0;
	int status;
	int proc;
	int simul;
	float time_limit;
	float interval;
	string log_name;
	char line[200]; //holds one line of output before it goes to the screen and the log file
 
	parse(argc, argv, proc, simul, time_limit, interval, log_name); //parse first so -h doesn't leave shared memory behind
 
	logfile.open(log_name.c_str());
	if(!logfile) {
		cout << "Could not open log file " << log_name << endl;
		exit(1);
	}
 
	int *clock = initializeMemory();
	int *sec = &(clock[0]);
	int *nano = &(clock[1]);
	*sec = *nano = 0;
 
	initializeQueue();
	srand(time(NULL)); //so each run gives the workers different random times
 
	signal(SIGINT, signalHandler); //ctrl-c
	signal(SIGALRM, signalHandler); //60 real life seconds
	alarm(60);
 
	sprintf(line, "OSS starting, PID:%d PPID:%d\n", getpid(), getppid());
	printOut(line);
	sprintf(line, "Called with: -n %d -s %d -t %g -i %g -f %s\n", proc, simul, time_limit, interval, log_name.c_str());
	printOut(line);
 
	//split -t into the most seconds and nanoseconds a worker can be given
	int max_sec = time_limit;
	int max_nano = (time_limit - max_sec) * 1000000000;
 
	int i = 0;
	int last_print = -1; //last half second the table was printed
	int launch_sec = 0, launch_nano = 0; //time the last child was launched
	int last_child = -1; //process table entry that got the last message
	int total_messages = 0; //total messages oss sent
	struct msgbuffer buf;
 
	while(total < proc || proc_running > 0) { //still children to launch OR children still in the system
		incrementClock(sec, nano, proc_running);
 
		//every half second of simulated time, output the process table
		int half_sec = *sec * 2 + *nano / 500000000; //how many half seconds have passed
		if(half_sec != last_print) {
			last_print = half_sec;
			printTable(*sec, *nano);
		}
 
		//find the next child to message: the next occupied entry after the last one (goes back to 0 after 19)
		int next = -1;
		for(int k = 1; k <= 20; k++) {
			int j = (last_child + k) % 20;
			if(p[j].occupied) {
				next = j;
				break;
			}
		}
 
		if(next != -1) { //only send a message if there is a child in the system
			last_child = next;
			buf.mtype = p[next].pid; //only that child picks this message up
			buf.intData = 1;
			sprintf(line, "OSS: Sending message to worker %d PID %d at time %d:%d\n", next, p[next].pid, *sec, *nano);
			printOut(line);
			if(msgsnd(msq_id, &buf, sizeof(buf) - sizeof(long), 0) == -1) {
				perror("oss: msgsnd");
				signalHandler(0);
			}
			p[next].messagesSent++;
			total_messages++;
 
			//wait for the child to answer (workers send their message to oss's pid)
			if(msgrcv(msq_id, &buf, sizeof(buf) - sizeof(long), getpid(), 0) == -1) {
				perror("oss: msgrcv");
				signalHandler(0);
			}
			sprintf(line, "OSS: Receiving message from worker %d PID %d at time %d:%d\n", next, p[next].pid, *sec, *nano);
			printOut(line);
 
			if(buf.intData == 0) { //child said it is done
				sprintf(line, "OSS: Worker %d PID %d is planning to terminate.\n", next, p[next].pid);
				printOut(line);
				waitpid(p[next].pid, &status, 0); //give it time to clear out of the system
				p[next] = {}; //clear the entry (all 0) so it can be reused
				proc_running--;
			}
		}
 
		//possibly launch a new child: more to launch, under the -s limit, and -i time has passed since the last launch
		double since_launch = (*sec - launch_sec) + (*nano - launch_nano) / 1000000000.0;
		if(total < proc && proc_running < simul && (total == 0 || since_launch >= interval)) {
			for(i = 0; i < 20; i++) { //find the first free spot in the process table
				if(!p[i].occupied)
					break;
			}
 
			//pick a random time between 1 second and -t for this worker
			int run_sec = rand() % max_sec + 1;
			int run_nano;
			if(run_sec == max_sec)
				run_nano = rand() % (max_nano + 1); //can't go over the nanoseconds of -t
			else
				run_nano = rand() % 1000000000;
 
			p[i].startSeconds = *sec; //time right before the fork
			p[i].startNano = *nano;
			pid_t pid = fork(); //creates copy of process that runs at the same time as eachother
			if (pid == -1) {
				cout << "Failed to launch child" << endl;
				signalHandler(0); //kill children, free shared memory and the queue, and exit
			}
			else if (pid == 0) { //seperates child copy from parent copy
				execlp("./worker","./worker",to_string(run_sec).c_str(), to_string(run_nano).c_str(), (char*) NULL); //calls child and ends
				cout << "exec of ./worker failed" << endl; //only gets here if exec fails
				exit(1);
			}
			//parent fills in the rest of the process table
			p[i].occupied = 1;
			p[i].pid = pid;
			p[i].messagesSent = 0;
			proc_running++; //parent keeps count of processes running and total
			total++;
 
			//estimated end: children take turns, so its run time gets multiplied by how many are running
			long long wait_nano = ((long long)run_sec * 1000000000 + run_nano) * proc_running;
			long long end_nano = (long long)p[i].startSeconds * 1000000000 + p[i].startNano + wait_nano;
			p[i].endingTimeSeconds = end_nano / 1000000000;
			p[i].endingTimeNano = end_nano % 1000000000;
 
			launch_sec = *sec;
			launch_nano = *nano;
			printTable(*sec, *nano); //print the table right after launching
		}
	}
 
	//output summary report
	sprintf(line, "OSS PID:%d Terminating\n", getpid());
	printOut(line);
	sprintf(line, "%d workers were launched and terminated\n", total);
	printOut(line);
	sprintf(line, "OSS sent a total of %d messages\n", total_messages);
	printOut(line);
 
	shmdt(clock); // Detach from the shared memory segment
	shmctl( shm_id, IPC_RMID, NULL ); // Free shared memory segment shm_id
	msgctl(msq_id, IPC_RMID, NULL); // Free the message queue
	logfile.close();
	return 0;
}
 
void incrementClock(int *sec, int *nano, int proc_running) {
	int children = proc_running;
	if(children == 0) //no dividing by zero when nobody is running
		children = 1;
	*nano += 250000000 / children; //250ms split between the children running
	if (*nano >= 1000000000) {
		*nano -= 1000000000; //subtract before adding the second so a worker never sees the clock a second ahead
		(*sec)++;
	}
}
 
void printOut(const char *line) { //sends output to the screen and the log file
	cout << line << flush;
	logfile << line << flush;
}
 
void printTable(int sec, int nano) {
	char line[200];
	sprintf(line, "OSS PID:%d SysClockS: %d SysclockNano: %d\nProcess Table:\n", getpid(), sec, nano);
	printOut(line);
	sprintf(line, "%-6s%-9s%-8s%-8s%-11s%-10s%-11s%s\n", "Entry", "Occupied", "PID", "StartS", "StartN", "EndingTS", "EndingTN", "MessagesSent");
	printOut(line);
	for(int j = 0; j < 20; j++) {
		sprintf(line, "%-6d%-9d%-8d%-8d%-11d%-10d%-11d%d\n", j, p[j].occupied, p[j].pid, p[j].startSeconds, p[j].startNano, p[j].endingTimeSeconds, p[j].endingTimeNano, p[j].messagesSent);
		printOut(line);
	}
	printOut("\n");
}
 
void signalHandler(int sig) {
	cout << endl << "OSS: stopping early, killing children and freeing shared memory and the message queue" << endl;
	logfile << endl << "OSS: stopping early, killing children and freeing shared memory and the message queue" << endl;
	for(int j = 0; j < 20; j++) {
		if(p[j].occupied)
			kill(p[j].pid, SIGTERM);
	}
	shmctl(shm_id, IPC_RMID, NULL);
	msgctl(msq_id, IPC_RMID, NULL);
	logfile.close();
	exit(1);
}
 
int *initializeMemory() {
	int shm_key = ftok("oss.c",0);
	if (shm_key == -1) {
		fprintf(stderr,"Parent:... Error in ftok\n");
		exit(1);
	}
	shm_id = shmget(shm_key,BUFF_SZ,0700|IPC_CREAT);
	if (shm_id == -1) { //shmget returns -1 on error (0 is a real id)
		fprintf(stderr,"Parent:... Error in shmget\n");
		exit(1);
	}
	int *clock = (int *)shmat(shm_id,0,0);
	if (clock == (int *)-1) { //shmat returns -1 on error, not 0
		fprintf(stderr,"Parent:... Error in shmat\n");
		exit(1);
	}
	return clock;
}
 
void initializeQueue() {
	key_t msg_key = ftok("oss.c",1); //1 instead of 0 so it's a different key than the shared memory
	if (msg_key == -1) {
		fprintf(stderr,"Parent:... Error in ftok for message queue\n");
		signalHandler(0);
	}
	msq_id = msgget(msg_key, 0644|IPC_CREAT);
	if (msq_id == -1) {
		fprintf(stderr,"Parent:... Error in msgget\n");
		signalHandler(0);
	}
}
 
int parse(int argc, char *argv[], int &proc, int &simul, float &time_limit, float &interval, string &log_name) { //uses arguments as reference to have default arguments
	int opt;
	proc = 1; //processes
	simul = 1; //simulations
	time_limit = 1; //most time a child can be given
	interval = 1;   //interval 1 in seconds to launch children
	log_name = "log.txt"; //log file
	while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) { //goes through each paramater h,n,s,t,i,f and gets the value
		switch (opt) { //each parameter goes in and gets chosen by its equivalent case
			case 'h':
				cout << "oss [-h] [-n proc] [-s simul] [-t timelimitForChildren] [-i fractionOfSecondToLaunchChildren] [-f logfile]" << endl
				<< "The proc parameter stands for number of total children to launch," << endl
				<< "the simul parameter indicates how many children to allow to run simultaneously." << endl
				<< "timelimitForChildren is the most simulated time (seconds) a child is given, it gets a random time from 1 to this; can be a float" << endl
				<< "fractionOfSecondToLaunchChildren is the minimum time (seconds) between launching children; can be a float (e.g. 0.1 = 100 ms)" << endl
				<< "logfile is where oss output gets saved (also printed to the screen)" << endl
				<< "./oss -n 3 -s 2 -t 4 -i 0.6 -f log.txt" << endl;
				exit(0);
			case 'n':
				proc = atoi(optarg);
				break;
			case 's':
				simul = atoi(optarg);
				if (simul > 20) { //the process table only has 20 spots
                			cout << "-s can't be more than 20" << endl;
                			exit(1);
        			}
        			else if (simul < 1) {
                			cout << "-s can't be less than 1" << endl;
                			exit(1);
        			}
				break;
			case 't':
				time_limit = atof(optarg);
				if (time_limit < 1) { //workers get at least 1 second
                			cout << "-t can't be less than 1" << endl;
                			exit(1);
        			}
				break;
			case 'i':
				interval = atof(optarg);
				break;
			case 'f':
				log_name = optarg;
				break;
			default: //if there is an unexpected option (optarg is NULL here, so don't use it)
				cout << "Use ./oss -h to see how to run oss" << endl;
				exit(1);
		}
	}

	return 0;
}
