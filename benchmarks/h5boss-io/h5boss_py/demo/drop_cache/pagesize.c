#include<unistd.h>
main(){
printf ("pagesize=%d\n",sysconf(_SC_PAGESIZE));
}
