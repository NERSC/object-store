#include "parse_node.h"
#include<stdio.h>
#include<stdlib.h>
#include <string.h>
#include <assert.h>
#include<libgen.h>
//structure for maintaining key value pair
int bufi=0;
//split a string by delimiter, return a char **
char** str_split(char* a_str, const char a_delim)
{
    char** result    = NULL;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = NULL;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;
    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }
    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);
    /* Add space for terminating null string so caller
       knows where the list of returned strings ends. */
    count++;
    result = malloc(sizeof(char*) * count);
    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);
        while (token)
        {
            assert(idx < count);
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        assert(idx == count - 1);
        *(result + idx) = 0;
    }
    return result;
}
char ** path_split(char* path) {
    char **token=(char **)malloc(2*sizeof(char *));
    char * dict=strdup(path);
    char * basec=strdup(path);
    char * d=strdup(dirname(dict));
    char * b=strdup(basename(basec));
    token[0]=d;
    token[1]=b;
    if(dict!=NULL) free(dict);
    if(basec!=NULL) free(basec);
    return token;
}
char ** parse_nodes(char * file,int numline){
  char ** buf=NULL;
  buf=(char **)malloc(sizeof(char *)*(numline+1));
  if(buf==NULL){
   printf("buf allocate error\n");
   exit(0);
  }
  FILE * fp;
  char * line = (char *)malloc(sizeof(char)*200);
  if(line==NULL){
   printf("line allocate error\n");
   exit(0);
  }
  size_t len = 0;
  size_t read;
  fp = fopen(file,"r");
  if (fp == NULL)
     exit(EXIT_FAILURE);
  while ((read = getline(&line,&len,fp))!= -1 ){
    //printf("line of length %zu:\n",read);
    //printf("%s",line);
    if('\n'!=line[0]){
     buf[bufi] = (char *)malloc(strlen(line)+1);
     line[strcspn(line,"\n")]=0;
     strcpy(buf[bufi],line);
     bufi++;
    }
  }
  fclose(fp);
  if (line!=NULL)
    free(line);
  return buf;
}
struct Fiber * dataset_list (char * file,const char sep,int numline){
    struct Fiber * dl= malloc(sizeof(struct Fiber));

    char ** lines=NULL;
    lines=parse_nodes(file,numline);
    if(lines==NULL){
     printf("lines parsing error\n");
     exit(0);
    }
    char ** dl_keys=NULL;
    char ** dl_values=NULL;
    dl_keys=(char **)malloc(sizeof(char *)*numline);
    if (dl_keys==NULL)  {
     printf("dl_keys allocation error\n");
     exit(0);
    }
    dl_values=(char **)malloc(sizeof(char *)*numline);
    if (dl_values==NULL)  {
     printf("dl_values allocation error\n");
     exit(0);
    }
    dl->count=bufi;
    int i;
    for (i=0;i<bufi;i++){
      char ** tokens=NULL;
      tokens=str_split(lines[i],sep);
      if(tokens==NULL) {
       printf("tokens memory error\n");
       exit(0);
      }
      dl_keys[i]=(char *)malloc(strlen(tokens[0])+1);
      dl_values[i]=(char *)malloc(strlen(tokens[1])+1);
      if(dl_keys[i]==NULL || dl_values[i]==NULL){
       printf("dl keys element allocate error\n");
      }
      strcpy(dl_keys[i],tokens[0]);
      strcpy(dl_values[i],tokens[1]);
    }
    dl->keys=dl_keys;
    dl->values=dl_values;
    bufi=0;
    if(lines!=NULL) free(lines);
    return dl;
}
struct Catalog * catalog_list (char * file,const char sep,int numline){
    struct Catalog * dl= malloc(sizeof(struct Catalog));
    if(dl==NULL){
      printf("catalog dl mem err\n");
      exit(0);
    }
    char ** lines=parse_nodes(file,numline);
    if(lines==NULL){
     printf("lines parsing error\n");
     exit(0);
    }
    char ** dl_plate_mjd=NULL;
    hsize_t * dl_fiber_id=NULL;
    char ** dl_filepath=NULL;
    hsize_t * dl_fiber_offset=NULL;
    hsize_t * dl_fiber_local_length=NULL;
    dl_plate_mjd=(char **)malloc(sizeof(char *)*(numline+1));
    if (dl_plate_mjd==NULL)  {
     printf("dl_platemjd allocation error\n");
     exit(0);
    }
    dl_fiber_id=(hsize_t *)malloc(sizeof(hsize_t)*(numline+1));
    if (dl_fiber_id==NULL)  {
     printf("dl_fiber_id allocation error\n");
     exit(0);
    }
    dl_filepath=(char **)malloc(sizeof(char *)*(numline+1));
    if (dl_filepath==NULL)  {
     printf("dl_filepath allocation error\n");
     exit(0);
    }
    dl_fiber_offset=(hsize_t *)malloc(sizeof(hsize_t)*(numline+1));
    if (dl_fiber_offset==NULL)  {
     printf("dl_fiber_offset allocation error\n");
     exit(0);
    }
    dl_fiber_local_length=(hsize_t *)malloc(sizeof(hsize_t)*(numline+1));
    if (dl_fiber_local_length==NULL)  {
     printf("dl_fiber_local_length allocation error\n");
     exit(0);
    }
    dl->count=bufi;
    int i;
    for (i=0;i<bufi;i++){
      char ** tokens=str_split(lines[i],sep);
      dl_plate_mjd[i]=(char *)malloc(strlen(tokens[0])+1);
      if(dl_plate_mjd[i]==NULL){
        printf("dl_plate_mjd mem err\n");
        exit(0);
       }

     dl_filepath[i]=(char *)malloc(strlen(tokens[2])+1);
      if(dl_filepath[i]==NULL){
        printf("dl_filepath mem err\n");
        exit(0);
       }
     strcpy(dl_plate_mjd[i],tokens[0]);
     dl_fiber_id[i]=atoll(tokens[1]);
     strcpy(dl_filepath[i],tokens[2]);
     dl_fiber_offset[i]=atoll(tokens[3]);
     dl_fiber_local_length[i]=atoll(tokens[4]);
    }
    if(lines!=NULL) free(lines);
    dl->plate_mjd=dl_plate_mjd;
    dl->fiberid=dl_fiber_id;
    dl->filepath=dl_filepath;
    dl->fiber_gstart=dl_fiber_offset;
    dl->fiber_llength=dl_fiber_local_length;
    bufi=0;
    return dl;
}
/*
void main(int argc, char ** argv){
   if(argc!=2) {
     printf("usage: %s filename\n",argv[0]);
     exit(EXIT_FAILURE);
   }
   int j;
   const char sep=':';
   struct Fiber * dl=dataset_list(argv[1],sep);
   //parse_nodes(argv[1]);
   for(j=0;j<dl->count;j++){
    printf("parsed line %d: %s, %s",j,dl->keys[j],dl->values[j]);
   }
   exit(EXIT_SUCCESS);
}
*/
