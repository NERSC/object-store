/*
*HDF ETL Tooset
*This file is a HDF5 matcher 
*Date: Mar 14 2016
*Author:
*Jialin Liu, jalnliu@lbl.gov
*Input: a list of plate/mjd/fiber
*Output: One hdf5 file
*HDF5 version: cray-hdf5/1.8.16
*/

#include <stdio.h>
#include <string.h>
#include <hdf5.h>
#include <stdlib.h>
#include <sys/stat.h>
#include "getopt.h"
int file_exist (char *filename)
{
  struct stat   buffer;   
  return (stat (filename, &buffer) == 0);
}

//void  matching ( int * mt, char * ipmf, char * ihdf){
	
//}
#define NAME_MAX 255
#define BUF 20

int count(char * in){
  FILE * file=fopen(in,"r");
  int lines = 0;
  int c;
  int last = '\n';
  while (EOF != (c = fgetc(file))) {
    if (c == '\n' && last != '\n') {
      ++lines;
    }
    last = c;
  }
  rewind(file);
  fclose(file);
  return lines;
}
void toarray(char ** line, char * file){
     FILE * plist=NULL;
     int i=0;
     plist = fopen(file,"r");
     while(fgets(line[i],BUF, plist)){
     	line[i][strlen(line[i]-1)]='\0';
	i++;
     }
     rewind(plist);
     fclose(plist);
}

char ipmf[NAME_MAX];
char ihdf [NAME_MAX];
char ohdf [NAME_MAX];

int main(int argc, char ** argv)
{
    int c=0;
    strncpy(ipmf, "./pmf.txt",NAME_MAX);
    strncpy(ihdf,"./hdf.txt",NAME_MAX);
    strncpy(ohdf, "./o.h5", NAME_MAX);
    while ((c = getopt (argc, argv, "i:j:o:")) != -1)
    switch (c)
      {
      case 'i':
        strncpy(ipmf, optarg, NAME_MAX);
        break;
      case 'j':
        strncpy(ihdf, optarg,NAME_MAX);
        break;
      case 'o':
        strncpy(ohdf, optarg, NAME_MAX);
      default:
        break;
      }

    if(argc<3) {printf("input args\n"); return 0;}
    if(!file_exist(ipmf)||!file_exist(ihdf)) {printf("inputs not exist\n");return 0;}
    //toarray(ipmflist,ipmf);
    int line_ipmf=count(ipmf);
    printf("lines in %s is %d\n",ipmf,line_ipmf);
    char ** ipmflist=(char **)malloc(sizeof(char *)*line_ipmf);
    toarray(ipmflist,ipmf);

    int line_ihdf=count(ihdf);
    printf("lines in %s is %d\n",ihdf,line_ihdf);
    char ** ihdflist=(char **)malloc(sizeof(char *)*line_ihdf);

    int * mt=(int *) malloc(line_ipmf*sizeof(int));
    //match(mt, line_ipmf,line_ihdf);
    if(ipmflist) free(ipmflist);
    if(ihdflist) free(ihdflist);
    if(mt) free(mt);

    return 0;
}
