#include<stdio.h>
#include<stdlib.h>
#include<hdf5.h>
#define NAME_MAX 255
char filename[NAME_MAX];
char groupname[NAME_MAX];
int main(int argc, char ** argv){
  int      mpi_size, mpi_rank;
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info;
  MPI_Init(&argc, &argv);
  MPI_Comm_size(comm, &mpi_size);
  MPI_Comm_rank(comm, &mpi_rank); 
  strncpy(filename, "/global/cscratch1/sd/jialin/h5boss/6054-56089.hdf5", NAME_MAX);
  strncpy(groupname,"6054/56089/575/exposures/144631",NAME_MAX);
  hid_t ifile= H5Fopen(filename,H5F_ACC_RDONLY,H5P_DEFAULT);
  if(ifile<0) printf("file '%s' open error in rank %d\n",filename,mpi_rank);
  herr_t igroup= H5Gopen(ifile,groupname,H5P_DEFAULT);
  if(igroup<0) printf("group '%d' open error in rank %d\n",igroup,mpi_rank);
  return 0;
}

