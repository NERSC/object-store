/*
*HDF ETL Tooset
*This file is a HDF5 subselecter
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

const char *pPath=NULL;
const char * ext=".h5";
const char *DATASET=NULL;
char * concat_filepath(int id){
      char fid_str[5];
      sprintf(fid_str,"%d",id);
      char * newfile;
      newfile=malloc(strlen(pPath)+strlen(ext)+strlen(fid_str)+1);
      strcpy(newfile,pPath);
      strcat(newfile,fid_str);
      strcat(newfile,ext);
 return newfile;
}
int file_exist (char *filename)
{
  struct stat   buffer;   
  return (stat (filename, &buffer) == 0);
}

int [] matching (char * ipmf, char * ihdf){


}
int count(char * in){
   File * input=fopen(in, "r");
   int ch, number_of_lines=0;
   do 
   {
    ch = fgetc(input);
    if(ch == '\n')
    	number_of_lines++;
   } while (ch != EOF);

   if(ch != '\n' && number_of_lines != 0) 
    number_of_lines++;
   return number_of_lines;
   input.close();
}

void toarray(char ** list, char * file){
     FILE * plist=NULL;
     int i=0;
     plist = fopen(file,"r");
     while(fgets(line[i],BUF, plist)){
     	line[i][strlen(line[i]-1)]='\0';
	i++;
     }
}
void print_usage(){

     printf("\n\nWarning:Arguments Incomplete\n\n********Arguments List********\n");
     printf("* ./subselect\n*");
     printf("* output\t(e.g., o.h5)\n");
     printf("* pmf-list\t(e.g., pmf.txt contains a list of plate/mjd/fiber)\n");
     printf("* hdf-list\t(e.g., hdf.txt contains a list of hdf file list)\n");
     printf("******************************\n");
     printf("Sample Command: ./subselect pmf.txt hdf.txt $SCRATCH/o.h5\n\n\n");
     return 1;
}

int main(int argc, char ** argv)
{
    int mpi_size,mpi_rank;
    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Info info;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);    

    if(argc<6){
      print_usage();
     return 1;
    }
    int c;
    char * plate,mjd,fiber;//we will query all files based on plate/mjd/fiber infor, and then get all spectrum in one file.
    char * ipmf = NULL;
    char * ihdf = NULL;
    char * ohdf = NULL;
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
    if(!file_exist(ipmf)||!file_exist(ihdf)) return 0;

    int line_ipmf=count(ipmf);
    char ** ipmflist=(char **)malloc(sizeof(char *)*line_ipmf)
    toarray(ipmflist,ipmf);

    int line_ihdf=count(ihdf);
    char ** ihdflist=(char **)malloc(sizeof(char *)*line_ihdf);

    int * mt=(int) malloc(line_ipmf*sizeof(int));
    match(mt, line_ipmf,line_ihdf);
    // using one rank to read the input: ipmf, ihdf
    if(mpi_rank==0){
      
    
    }
    
    hsize_t ndims,ncols,nrows,ex_nrows;
    int total=atoi(argv[1]), startid=atoi(argv[2]);
    char *saveFilePath=NULL;
    saveFilePath=argv[3];
    pPath=argv[4];
    DATASET=argv[5];
    printf("Files: Combining %d Files(Extracting One Dataset: %s) from %s to %s\n\n",total,DATASET,pPath,saveFilePath);
    ndims=2;
    //create datasets
     hid_t ex_file, ex_dataset,file_space,mem_space;
     hid_t plist = H5Pcreate(H5P_DATASET_CREATE);
     H5Pset_layout(plist, H5D_CHUNKED);
     hsize_t chunk_dims[2];
     chunk_dims[0]=1024;
     chunk_dims[1]=11;
     H5Pset_chunk(plist, ndims, chunk_dims);
     hsize_t dims[ndims];
     dims[0]=0;
     dims[1]=0;
     hsize_t      maxdims[2];
     maxdims[0] = H5S_UNLIMITED;
     maxdims[1] = H5S_UNLIMITED;
     file_space=H5Screate_simple(ndims,dims,maxdims);
     ex_file=H5Fcreate(saveFilePath, H5F_ACC_TRUNC,H5P_DEFAULT,H5P_DEFAULT); 
     ex_dataset=H5Dcreate(ex_file,DATASET, H5T_NATIVE_FLOAT,file_space,H5P_DEFAULT,plist,H5P_DEFAULT);   
     mem_space = H5Screate_simple(ndims,dims,NULL);
     hsize_t start[ndims],count[ndims];
     start[0]=dims[0];
     hid_t err_stack=0, nextFid,nextDid;   
     //float buffer[nrows][ncols];    
     herr_t status;
     int err_read=0,err_write=0,i=0,update_last=0;
     hsize_t acdims[ndims];
     acdims[0]=dims[0];
     acdims[1]=dims[1];
     // disable error info
     //H5Eset_auto(err_stack, NULL, NULL);
     //retrieve new data dims, ndims
     hsize_t new_ndims=0;
     int nonpath=0;
     int skip_read=0;
     hid_t new_dataspace;
     hsize_t dim_before=0;
     int firstread=0;
     for(i=0;i<total;i++){
      char * nextfile= concat_filepath(i+startid);
      if(!file_exist(nextfile)){
        nonpath++;
	continue;	
       }	
      nextFid = H5Fopen (nextfile, H5F_ACC_RDONLY, H5P_DEFAULT);
      nextDid = H5Dopen (nextFid, DATASET, H5P_DEFAULT);
      // check dims of new data
      new_dataspace=H5Dget_space(nextDid);
      new_ndims=H5Sget_simple_extent_ndims(new_dataspace);
      if(new_ndims<=0){
      	err_read++;
 	continue;
      }
      hsize_t new_dims[new_ndims];
      H5Sget_simple_extent_dims(new_dataspace,new_dims,NULL);
      
      //validate extendablity of new data
      if(new_ndims!=ndims||new_dims[0]<10) {
        skip_read++;
        continue;
      }      
      float buffer[new_dims[0]][new_dims[1]];
      //load new data
      status=H5Dread(nextDid,H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                buffer);
      if(status==-1){
      	err_read++;
	continue;
      }
      //Specify the memory space for new buffer 
      if(firstread==0) {
	start[0]=0;
	dim_before=new_dims[0];//update
      }
      else {
      	start[0] +=dim_before;
      }//finally, I found this error, new_dims[0] should use previous iteration for calculating new start offet
      H5Sset_extent_simple(mem_space, ndims, new_dims, NULL);
      //Extend dataset
      acdims[0]+=new_dims[0];
      acdims[1]=new_dims[1];
      status=H5Dset_extent(ex_dataset,acdims);
      file_space=H5Dget_space(ex_dataset);
      //Select hyperslab on file dataset
      start[1]  =0;
      count[0]  =new_dims[0];
      count[1]  =new_dims[1];
      status=H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start, NULL, count, NULL);
      //now the selected file_space has same num elements as the pre-specified mem space
      H5Fclose(nextFid);
      H5Dclose(nextDid);
      status=H5Dwrite(ex_dataset, H5T_NATIVE_FLOAT, mem_space, file_space, H5P_DEFAULT, buffer);
      //printf("acdims[0]%lld: newdims[0]%lld, start[0] %lld, sum %lld \n",acdims[0],new_dims[0],start[0],new_dims[0]+start[0]);
      if(status==-1) {
        printf("Fail Write File %s\n",nextfile);
        start[0]-=dim_before;
	acdims[0]-=new_dims[0];
	err_write++;
	continue;
      }
      update_last=i+startid;
      firstread=1;
      dim_before=new_dims[0];
    }
    printf("Summary\n");
    printf("Total Read \t%d\nRead Error \t%d\nRead ErrorRate \t%.2f\nFile NotExist \t%d\nSkip Read \t%d\nWrite Error \t%d\n",total,err_read, (float)err_read/total,nonpath,skip_read,err_write);
    printf("Write Done at\t %s\n",saveFilePath);
    printf("Combined File Range\t %d-%d\n",startid,update_last);
    H5Sclose(file_space);
    H5Sclose(mem_space);
    H5Dclose(ex_dataset);
    H5Fclose(ex_file);
}
