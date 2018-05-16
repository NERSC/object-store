#include <stdio.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <rados/librados.h>
int main(int argc, char* argv[]){
int err;
rados_t cluster;
if(argc<5){
printf("run as ./rados_connect ceph_conf file_obj_name size_MBs number_writes\n");
return 0;
}
err = rados_create(&cluster, NULL);
if (err < 0) {
        fprintf(stderr, "%s: cannot create a cluster handle: %s\n", argv[0], strerror(-err));
        exit(1);
}
printf("ceph conf file is :%s\n",argv[1]);
err = rados_conf_read_file(cluster, argv[1]);
if (err < 0) {
        fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
        exit(1);
}
printf("conf file is read in\n");

err = rados_connect(cluster);
if (err < 0) {
        fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
        exit(1);
}
printf("rados cluster is connected\n");
rados_ioctx_t io;
char *poolname = "swiftpool";
rados_write_op_t write_op;
err = rados_ioctx_create(cluster, poolname, &io);
if (err < 0) {
        fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], poolname, strerror(-err));
        rados_shutdown(cluster);
        exit(1);
}
printf("pool connected,sizeof char: %zu,sizeof uint:%zu\n",sizeof(char),sizeof(unsigned int));
int size_cur=atoi(argv[3]);
size_t wsize= 1024*1024*size_cur*sizeof(char);
char *x=malloc(wsize);
write_op = rados_create_write_op();
int number_writes=atoi(argv[4]);
int iw=0;
for (iw=0; iw<number_writes; iw++){
   rados_write_op_write(write_op, (const char * )x, wsize, iw*wsize); 
}
const char * object_name = "haas";
err = rados_write_op_operate(write_op, io, object_name, NULL,LIBRADOS_OPERATION_NOFLAG);
//err = rados_write_full(io, argv[2],(const char *) x, size_cur*1024*1024);
if (err < 0) {
        fprintf(stderr, "%s: cannot write pool %s: %s\n", argv[0], poolname, strerror(-err));
        rados_ioctx_destroy(io);
        rados_shutdown(cluster);
        exit(1);
}
printf("written object %s, size is %d MBytes\n",argv[2],size_cur*number_writes);
//printf("UINT_MAX = %u =0x%x\n", UINT_MAX, UINT_MAX);
return 0;

}
