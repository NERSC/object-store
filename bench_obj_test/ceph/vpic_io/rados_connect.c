#include <stdio.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <rados/librados.h>
int main(int argc, char* argv[]){
int err;
rados_t cluster;
if(argc<4){
printf("run as ./rados_connect ceph_conf file_obj_name MBs\n");
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

err = rados_ioctx_create(cluster, poolname, &io);
if (err < 0) {
        fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], poolname, strerror(-err));
        rados_shutdown(cluster);
        exit(1);
}
printf("pool connected,sizeof char: %zu,sizeof uint:%zu\n",sizeof(char),sizeof(unsigned int));
int size_cur=atoi(argv[3]);
char *x=malloc((1024*1024*size_cur)*sizeof(char));
err = rados_write_full(io, argv[2],(const char *) x, size_cur*1024*1024);
if (err < 0) {
        fprintf(stderr, "%s: cannot write pool %s: %s\n", argv[0], poolname, strerror(-err));
        rados_ioctx_destroy(io);
        rados_shutdown(cluster);
        exit(1);
}
printf("written object %s, size is %d MBytes\n",argv[2],size_cur);
printf("UINT_MAX = %u = 0x%x\n", UINT_MAX, UINT_MAX);
return 0;

}
