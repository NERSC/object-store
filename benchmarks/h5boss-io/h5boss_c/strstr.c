#include<stdio.h>
#include<stdlib.h>
#include<libgen.h>
void main(){
char *path ="ab/cde/fg.out";
printf("orig:%s\n",path);
char * pp=strdup(path);
char * ff=strdup(path);
printf("path:%s\n",dirname(pp));
printf("bname:%s\n",basename(ff));
}
