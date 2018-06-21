#include <stdio.h>
#include <mpi.h>
#include <time.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <rados/librados.h>
#include <radosstriper/libradosstriper.h>
#include "timer.h"
struct timeval start_time[3];
float elapse[3];

int main(int argc, char* argv[]){
int err;
double t1, t2,t3,t4; 
//t1 = MPI_Wtime(); 
(void) MPI_Init(&argc, &argv);
rados_t cluster;
int my_rank, num_procs;
if(argc<8){
printf("run as\n./rados_write $CEPH_CONF file_obj_name size_MBs number_writes stripe_count object_size stripe_unit\n note:object_size>=stripe_unit\nTotal write size=size_MBs * number_writes\n");
return 0;
}
MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
if(my_rank==0)
	printf("Number of Processes %d\n",num_procs);
err = rados_create(&cluster, NULL);//this should be fine as tested in rados vol
if (err < 0) {
        fprintf(stderr, "%s: cannot create a cluster handle: %s\n", argv[0], strerror(-err));
        exit(1);
}
if(my_rank==0) printf("ceph conf file is :%s\n",argv[1]);
err = rados_conf_read_file(cluster, argv[1]);
if (err < 0) {
        fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
        exit(1);
}
if(my_rank==0) printf("conf file is read in\n");

err = rados_connect(cluster);
if (err < 0) {
        fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
        exit(1);
}
if(my_rank==0) printf("rados cluster is connected\n");
rados_ioctx_t io;

char *poolname = "swiftpool";
rados_write_op_t write_op;
//with MPI, what would happen with ioctx_create? undefined
err = rados_ioctx_create(cluster, poolname, &io);
if (err < 0) {
        fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], poolname, strerror(-err));
        rados_shutdown(cluster);
        exit(1);
}
if(my_rank==0) printf("pool connected,sizeof char: %zu,sizeof uint:%zu\n",sizeof(char),sizeof(unsigned int));
//printf("rank:%d:cluster:%u:ioctx:%u\n",my_rank,cluster,io);

//Non-striping write
int size_cur=atoi(argv[3]);
size_t wsize= 1024*1024*size_cur*sizeof(char);
int number_writes=atoi(argv[4]);
if(my_rank==0){
printf("number_writes:%d,size_cur:%dMB\n",number_writes,size_cur);
}
//return 1;
//unroll
/*
include aio_operate

another loop wait for completion. 
*/
//#include <time.h>
//#include <stdlib.h>

srand(time(NULL));   // should only be called once
int r = rand(); 
char object_name[10] = "x";
char object_name2[10] = "y";
char int_rank_x[5];
char int_rank_y[5];
sprintf(int_rank_x,"%d",r);
sprintf(int_rank_y,"%d",my_rank);
strcat(object_name,int_rank_x);
strcat(object_name2,int_rank_y);
//printf("object names:%s,%s,rank:%d\n",object_name,object_name2,my_rank);
fflush(stdin);
MPI_Barrier(MPI_COMM_WORLD);
if(size_cur*number_writes>90){
	if(my_rank==0)
		printf("non-striping write size is too large(>90MB), skip it, do striping write now\n");
}
else{
	MPI_Barrier(MPI_COMM_WORLD);
	timer_on(0);
	t1 = MPI_Wtime();
	err=0;
	char *x=calloc(1024*1024*size_cur, sizeof(char));
	write_op = rados_create_write_op();
	int iw=0;
	for (iw=0; iw<number_writes; iw++){
   		rados_write_op_write(write_op, (const char * )x, wsize, iw*wsize);
	}
	//MPI_Barrier(MPI_COMM_WORLD);
	t3=MPI_Wtime();
	err = rados_write_op_operate(write_op, io, object_name, NULL,LIBRADOS_OPERATION_NOFLAG);
	//timer_off(0);
	//err = rados_write_full(io, argv[2],(const char *) x, size_cur*1024*1024);
	if (err < 0) {
        	fprintf(stderr, "%s: cannot write pool %s: %s\n", argv[0], poolname, strerror(-err));
        	rados_ioctx_destroy(io);
        	rados_shutdown(cluster);
        	exit(1);
	}
	//MPI_Barrier(MPI_COMM_WORLD);
	//timer_off(0);
	t2 = MPI_Wtime(); 
	MPI_Barrier(MPI_COMM_WORLD);
	t4=MPI_Wtime();
	timer_off(0);
	//printf("Rank:%d,written object %s, size is %d MBytes\n",my_rank,argv[2],size_cur*number_writes);
	//printf("Rank:%d,writting time (sec):",my_rank);
	//timer_msg(0);
	//printf("\n");
	if(my_rank==0){
	  printf("Non-striping nproc:%d, %dMB per proc\n",num_procs,size_cur*number_writes);
	  double sizegb=num_procs*size_cur*number_writes/1024.0;
	  printf("Total:%.2fGB, write_op per proc:%d\n",sizegb,number_writes);
	  printf("Timer:%.2f,MPI_Wtime:%.2f",elapse[0],t4-t1);//,io_operate:%.2f\n",elapse[0],t2-t1,t2-t3);
	  //printf("Non-striping Bandwidth:%.2fMB/sec,%.2fMB/sec\n",num_procs*size_cur*number_writes/elapse[0],num_procs*size_cur*number_writes/(t2-t1));
	  //printf("non-striping free buffer\n");
	}
	printf("rank:%d:%.2f\n",my_rank,t2-t1);
	free(x);
}

/*
if(my_rank==0)
	printf("Start stripe writing:\ncreating striper handle\n");
//create striper handle
rados_striper_t st;
err=rados_striper_create(io,&st);
if(my_rank==0){
	if(err>=0)
		printf("setting stripe count and object size\n");
	else 
		printf("striper create failed\n");
}
unsigned int stripe_count = atol(argv[5]);
unsigned int object_size = atol(argv[6]);
unsigned int stripe_unit = atol(argv[7]); 
if(stripe_unit>object_size&&my_rank==0){
	printf("stripe unit:%u is larger than object size:%u\n",stripe_unit, object_size);
	exit(1);
}

err = rados_striper_set_object_layout_stripe_count(st, stripe_count);
if (err < 0){ 
	printf("Rank:%d stripe count setting failed\n",my_rank);
}
else{
	if(my_rank==0) 
	printf("Rank:%d stripe count is set to %u\n",my_rank,stripe_count);
}

err =rados_striper_set_object_layout_stripe_unit(st,stripe_unit);

if (err < 0){ 
	printf("Rank:%d stripe unit 1M setting failed\n",my_rank);
}
else{ 
	if(my_rank==0)
	printf("Rank:%d stripe unit is set to %u\n",my_rank,stripe_unit);
}

err = rados_striper_set_object_layout_object_size(st, object_size);

if (err <0){ 
	printf("Rank:%d object size setting failed,%u\n",my_rank,object_size);
}
else{ 
	if(my_rank==0)
	printf("Rank:%d object size is set to %u\n",my_rank,object_size);
}
//printf("UINT_MAX = %u =0x%x\n", UINT_MAX, UINT_MAX);
//printf("checking pool alignment and fill stripe_unit");
//err = pool_required_alignment2(io,&stripe_unit);
//err=-1;
//if (err < 0 ) printf("pool doesn't have alignemnt specification\n");
//else {
//pool has hint for alignment, so go for it
//err = rados_striper_set_object_layout_stripe_unit(st, stripe_unit);
//}
if(my_rank==0)
	printf("Striping Configuration:\nStripe Count:%u\nStripe Unit:%u\nObject Size:%u\n",stripe_count,stripe_unit,object_size);
//char *stripe_x=malloc(wsize*number_writes); //buffer for strip write

int writes_perrank = number_writes / num_procs; //e.g., 8 writes, 4 procs, then offset_perrank = 2
uint64_t offset =wsize * my_rank * writes_perrank;
size_t len = 1024*1024*size_cur * writes_perrank;
if(my_rank==num_procs-1)//adjust last rank's length 
	len = (number_writes-(num_procs-1)*writes_perrank)*1024*1024*size_cur;

char *stripe_x=calloc(len, sizeof(char));
MPI_Barrier(MPI_COMM_WORLD);
timer_on(1);
//err = rados_striper_write_full(st, object_name2,(const char *) x, wsize*number_writes);
err = rados_striper_write (st, object_name2, (const char *) stripe_x, len, offset);
MPI_Barrier(MPI_COMM_WORLD);
timer_off(1);
if(err>=0){
	printf("Rank:%d striping write successful\n",my_rank);
	free(stripe_x);
}
else {
        printf("Rank:%d striping write failed\n",my_rank);
}
if(err>=0){
        printf("written object %s, size is %d MBytes\n",argv[2],size_cur*number_writes);
        printf("writting time (sec):");
        timer_msg(1);
        printf("\n");
        printf("striping bandwidth:%.2fMB/sec\n",size_cur*number_writes/elapse[1]);
}
*/
(void) MPI_Finalize();
return 0;

}
