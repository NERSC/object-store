#include "parse_node.h"
#include "compound_copy.h"
#include<stdio.h>
#include<stdlib.h>
#include <string.h>
#include <assert.h>
#include<time.h>
int main(int argc, char ** argv){
   if(argc!=4) {
     printf("usage: %s csvfile output 1:readwrite(0:readonly)\n",argv[0]);
     exit(EXIT_FAILURE);
   }
   int j;
   time_t begin,end,final;
   const char sep=':';
   begin = time(NULL);
   bool write=false;
   long x = strtol(argv[3], NULL, 10);
   if(x==1) write=true;
   struct Fiber * dl=dataset_list(argv[1],sep);
   end= time(NULL);
   for(j=0;j<dl->count;j++){
    printf("%d: %s, %s\n",j,dl->keys[j],dl->values[j]);
    compound_read(dl->values[j],argv[2], dl->keys[j], write);
   }
   final=time(NULL);
   printf("parse csv:%.2f, read/write:%.2f\n",difftime(end,begin),difftime(final,end));
   if(dl!=NULL) free(dl);
   return 0;  
}
