#include<unistd.h>
#include<sys/types.h>
#include<stdio.h>
#include<stdlib.h>
#include<string>
#include<iostream>
#include<sys/wait.h>
#include<sys/ipc.h>
#include<sys/shm.h>
#include<signal.h>
using namespace std;

struct PCB {
        int occupied; // either true or false
        pid_t pid; // process id of this child
        int startSeconds; // time when it was forked
        int startNano; // time when it was forked
        int endingTimeSeconds; // estimated time it should end
        int endingTimeNano; // estimated time it should end
};

const int BUFF_SZ = sizeof(int)*2;
int shm_key;
int shm_id;
struct PCB p[20];

int parse(int argc, char *argv[], int &proc, int &simul, float &time_limit, float &interval);

void incrementClock(int *sec, int *nano);

int *initializeMemory();

void signalHandler(int sig);

int main(int argc, char* argv[]) {
        int proc_running = 0, total = 0;
        int status;
        int proc;
        int simul;
        float time_limit;
        float interval;

        parse(argc, argv, proc, simul, time_limit, interval); //parse first so -h doesn't leave shared memory behind

        int *clock = initializeMemory();
        int *sec = &(clock[0]);
        int *nano = &(clock[1]);
        *sec = *nano = 0;

        signal(SIGINT, signalHandler); //ctrl-c
        signal(SIGALRM, signalHandler); //60 real life seconds
        alarm(60);

        cout << "OSS starting, PID:" << getpid() << " PPID:" << getppid() << endl
        << "Called with:" << endl
        << "-n " << proc << endl
	 << "-s " << simul << endl
        << "-t " << time_limit << endl
        << "-i " << interval << endl;

        //split -t into seconds and nanoseconds to send to each worker
        int seconds = time_limit;
        int nano_seconds = (time_limit - seconds) * 1000000000;

        int i = 0;
        int last_print = -1; //last half second the table was printed
        int launch_sec = 0, launch_nano = 0; //time the last child was launched
        int total_sec = 0, total_nano = 0; //combined time all workers ran

        while(total < proc || proc_running > 0) { //still children to launch OR children still in the system
                incrementClock(sec, nano);

                //every half second of simulated time, output the process table
                int half_sec = *sec * 2 + *nano / 500000000; //how many half seconds have passed
                if(half_sec != last_print) {
                        last_print = half_sec;                                                      cout << "OSS PID:" << getpid() << " SysClockS: " << *sec << " SysclockNano: " << *nano << endl
                        << "Process Table:" << endl;
                        printf("%-6s%-9s%-8s%-8s%-11s%-12s%s\n", "Entry", "Occupied", "PID", "StartS", "StartN", "EndingTimeS", "EndingTimeNano");
                        for(int j = 0; j < 20; j++) {
                                printf("%-6d%-9d%-8d%-8d%-11d%-12d%d\n", j, p[j].occupied, p[j].pid, p[j].startSeconds, p[j].startNano, p[j].endingTimeSeconds, p[j].endingTimeNano);
                        }
                        cout << endl;
                }

                //check if a child has terminated (nonblocking)
                int pid = waitpid(-1, &status, WNOHANG);
		if(pid > 0) {
                        for(int j = 0; j < 20; j++) {
                                if(p[j].occupied && p[j].pid == pid) {
                                        //add how long it ran to the total
                                        total_sec += *sec - p[j].startSeconds;
                                        total_nano += *nano - p[j].startNano;
                                        if(total_nano < 0) {
                                                total_sec--;
                                                total_nano += 1000000000;
                                        }
                                        if(total_nano >= 1000000000) {
                                                total_sec++;
                                                total_nano -= 1000000000;
                                        }
                                        p[j] = {}; //clear the entry (all 0) so it can be reused
                                }
                        }
                        proc_running--;
                }

                //possibly launch a new child: more to launch, under the -s limit, and -i time has passed since the last launch
                double since_launch = (*sec - launch_sec) + (*nano - launch_nano) / 1000000000.0;
                if(total < proc && proc_running < simul && (total == 0 || since_launch >= interval)) {                                                                          for(i = 0; i < 20; i++) { //find the first free spot in the process table
                                if(!p[i].occupied)                                                                  break;                                                      }
                                                                                                    p[i].startSeconds = *sec; //time right before the fork
                        p[i].startNano = *nano;
			pid = fork(); //creates copy of process that runs at the same time as eachother
                        if (pid == -1) {
                                cout << "Failed to launch child" << endl;
                                signalHandler(0); //kill children, free shared memory, and exit
                        }
                        else if (pid == 0) { //seperates child copy from parent copy
                                execlp("./worker","./worker",to_string(seconds).c_str(), to_string(nano_seconds).c_str(), (char*) NULL); //calls child and ends
                                cout << "exec of ./worker failed" << endl; //only gets here if exec fails
                                exit(1);
                        }
                        //parent fills in the rest of the process table (the child's copy of p doesn't reach the parent)
                        p[i].occupied = 1;
                        p[i].pid = pid;
                        p[i].endingTimeSeconds = p[i].startSeconds + seconds;
                        p[i].endingTimeNano = p[i].startNano + nano_seconds;
                        if(p[i].endingTimeNano >= 1000000000) {
                                p[i].endingTimeSeconds++;
                                p[i].endingTimeNano -= 1000000000;
                        }

                        proc_running++; //parent keeps count of processes running and total
                        total++;
                        launch_sec = *sec;
                        launch_nano = *nano;
                }
        }

        //output summary report
        cout << "OSS PID:" << getpid() << " Terminating" << endl
	 << total << " workers were launched and terminated" << endl
        << "Workers ran for a combined time of " << total_sec << " seconds " << total_nano << " nanoseconds." << endl;

        shmdt(clock); // Detach from the shared memory segment
        shmctl( shm_id, IPC_RMID, NULL ); // Free shared memory segment shm_id
        return 0;
}

void incrementClock(int *sec, int *nano) {
        *nano += 1000; // amount of nano seconds per loop (raise it if the clock is slower than real time, lower it if faster)
        if (*nano >= 1000000000) {                                                          *nano -= 1000000000; //subtract before adding the second so a worker never sees the clock a second ahead
                (*sec)++;                                                           }
}                                                                           
void signalHandler(int sig) {
        cout << endl << "OSS: stopping early, killing children and freeing shared memory" << endl;
        for(int j = 0; j < 20; j++) {
                if(p[j].occupied)
                        kill(p[j].pid, SIGTERM);
        }
        shmctl(shm_id, IPC_RMID, NULL);
        exit(1);
}

int *initializeMemory() {
        int shm_key = ftok("oss.c",0);
        if (shm_key <= 0) {
                fprintf(stderr,"Parent:... Error in ftok\n");
                exit(1);
        }
        shm_id = shmget(shm_key,BUFF_SZ,0700|IPC_CREAT);
        if (shm_id <= 0 ) {
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

int parse(int argc, char *argv[], int &proc, int &simul, float &time_limit, float &interval) { //uses arguments as reference to have default arguments
        int opt;
        proc = 1; //processes
        simul = 1; //simulations
        time_limit = 1; //time limit to launch children
        interval = 1;   //interval 1 in seconds to launch children
        while ((opt = getopt(argc, argv, "hn:s:t:i:")) != -1) { //goes through each paramater h,n,s,t,i and gets the value which is argv holds the parameter, argv holds how many
                switch (opt) { //each parameter goes in and gets chosen by its equivalent case
                        case 'h':                                                                           cout << "oss [-h] [-n proc] [-s simul] [-t time_limit] [-i interval]" << endl                                                                           << "The proc parameter stands for number of total children to launch," << endl                                                                          << "the simul parameter indicates how many children to allow to run simultaneously." << endl                                                            << "time limit is the simulated time (seconds) each child runs before terminating; can be a float" << endl                                              << "interval is the minimum time (seconds) between launching children; can be a float (e.g. 0.1 = 100 ms)" << endl
                                << "./oss -n 5 -s 3 -t 4 -i 0.2" << endl;
                                exit(0);
                        case 'n':
                                proc = atoi(optarg);
                                break;
                        case 's':
				simul = atoi(optarg);
                                break;
                        case 't':
                                time_limit = atof(optarg);
                                break;
                        case 'i':
                                interval = atof(optarg);
                                break;
                        default: //if there is an unexpected option (optarg is NULL here, so don't use it)
                                cout << "Use ./oss -h to see how to run oss" << endl;                                                                                                   exit(1);
                }                                                                   }
        if (simul > 20) { //the process table only has 20 spots                             cout << "-s can't be more than 20, using 20" << endl;
                simul = 20;
        }                                                                           return 0;
}
