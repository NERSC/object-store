/*Reads datasets from sources files, write to existing file, both in parallel
 *Jialin Liu, Aug 8 2016
 *Jalnliu@lbl.gov
 */
#include "stdlib.h"
#include "hdf5.h"
#include "getopt.h"
#include "parse_node.h"
#include "compound_copy.h"
#include<stdio.h>
#include <string.h>
#include <assert.h>
#include<time.h>
#define NAME_MAX 255
char filename[NAME_MAX];
char csvfile[NAME_MAX];
char catalog_csvfile[NAME_MAX];
int main(int argc, char **argv){
  int      mpi_size, mpi_rank;
  MPI_Comm comm = MPI_COMM_WORLD;
  MPI_Info info;
  MPI_Init(&argc, &argv);
  MPI_Comm_size(comm, &mpi_size);
  MPI_Comm_rank(comm, &mpi_rank);
  //printf("argc:%d\n",argc);
  //if (argc != 7){
  if (mpi_rank==0)  
    printf("usage: %s -f output -m csv -l number_lines -c catalog -k number_catalog\n",argv[0]);
   // printf("only got %d\n",argc);
  //  return 0;
  //}
  int c;
  int numline;
  int catalog_numline;
  opterr = 0;
  strncpy(filename, "fiber.h5", NAME_MAX);
  strncpy(csvfile, "fiberlist.txt",NAME_MAX);
  strncpy(catalog_csvfile, "cataloglist.txt",NAME_MAX);
  /***input arguments****/ 
  //f: output, m:csv   
  while ((c = getopt (argc, argv, "f:m:l:n:k:")) != -1)
    switch (c)
      {
      case 'f':
	strncpy(filename, optarg, NAME_MAX);
	break;
      case 'm':
	strncpy(csvfile, optarg, NAME_MAX);
        break;
      case 'l':
        strncpy(catalog_csvfile, optarg, NAME_MAX);
        break;
      case 'n':
        numline=strtol(optarg, NULL, 10); 
        break;
      case 'k':
        catalog_numline=strtol(optarg, NULL, 10);
        break;
      default:
	break;
      }
  MPI_Info_create(&info); 
  //Open file/dataset
  if (mpi_rank==0){
  printf("input:%s\n",filename);
  printf("number of fiber lines:%d\n",numline);
  
  printf("fiber csv:%s\n",csvfile); 
  printf("number of catalog lines:%d\n",catalog_numline);
  printf("catalog csv:%s\n",catalog_csvfile);
  }
  hid_t fapl,file;
  file=-1;
  fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_fapl_mpio(fapl, comm, info);
//  hbool_t is_collective=true;
//  H5Pset_all_coll_metadata_ops(fapl, is_collective);
  file= H5Fopen(filename, H5F_ACC_RDWR, fapl);
  //if(mpi_rank==0) printf("output file hanlde= %lld / %0llx\n", (long long)file, (unsigned long long)file);
  if(file<0) printf("filename '%s' open error in rank %d\n",filename,mpi_rank);
  //else printf("filename '%s' open correctly rank %d\n",filename, mpi_rank);
//H5Fclose(file);
//MPI_Finalize();
//exit(0);
  
  //file = H5Fopen(filename, H5F_ACC_RDWR, H5P_DEFAULT);
  
  H5Pclose(fapl);
  if(mpi_rank==0) {
	if(file<0){
	  printf("File %s open error\n",filename); 
	  return 0;
	}
	else {
	  printf("File %s open ok\n",filename);
	}
  }
  
  const char sep=':';
  struct Fiber * dl_fiber=NULL;
  struct Catalog *dl_catalog=NULL;
  dl_fiber=dataset_list(csvfile,sep,numline);
  if(dl_fiber==NULL) {
    printf("dl_fiber memorry allocation error\n");
    exit(0);

  }
  //dl_catalog=catalog_list(catalog_csvfile,sep,catalog_numline);
  /*if(dl_catalog==NULL) {
    printf("dl_catalog memorry allocation error, rank:%d\n",mpi_rank);
    exit(0);
  }
  */
  int total_nodes=dl_fiber->count;
  if(mpi_rank==0){
   printf("dl_fiber count:%d,pre-allocate:%d\n",dl_fiber->count,numline);
  // printf("dl_catalog count:%d,pre-allocate:%d\n",dl_catalog->count,catalog_numline);
  }
  int step = total_nodes /mpi_size +1;
  int rank_start=mpi_rank*step;
  int rank_end=rank_start+step;
  if(mpi_rank == mpi_size-1){
    rank_end=total_nodes;
    if(rank_start>total_nodes){
      rank_start=total_nodes;
    }
  }
  int i;
  double t0=MPI_Wtime();
  //read fiber
  for(i=rank_start;i<rank_end;i++){
   //para:input file, output file, dataset(group/dataset), write, mpi rank
   compound_read(dl_fiber->values[i],file, dl_fiber->keys[i],1,mpi_rank);
   //compound_read_fiber(dl_fiber,file,i,1,mpi_rank);//rewirte into this interface
  }
  

  //read catalog
 /* for(i=rank_start;i<rank_end;i++){
   //para:input file, output file, dataset(group/table_name), original offset, new offset,write, mpi rank
   compound_read_catalog(dl_catalog,file,i,1,mpi_rank);
  }
  */
  MPI_Barrier(comm);
  if(dl_fiber!=NULL) free(dl_fiber);
  //if(dl_catalog!=NULL) free(dl_catalog);
  if(mpi_rank==0||mpi_rank==mpi_size-1) printf("rank:%d:%d\n",mpi_rank,rank_end-rank_start);
  double t1 = MPI_Wtime();
  if(mpi_rank==0){ 
   printf("\nRank %d, read time %.2fs\n",mpi_rank,t1-t0);
  }
  H5Fclose(file);
  MPI_Barrier(comm);
  double t2=MPI_Wtime();
  if(mpi_rank==0){
  printf("\nRank %d, close time %.2fs\n",mpi_rank,t2-t1);
  }
  MPI_Finalize();
  return 0;
}
