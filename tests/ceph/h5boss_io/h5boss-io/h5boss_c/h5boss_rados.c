#include "h5rados_example.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#include <hdf5.h>
#include "timer.h"
#define NDSET 25304802
#define NAMELEN 48

struct timeval start_time[3];
float elapse[3];

void print_usage() {
    printf("Usage: srun -n n_proc ./h5bossio dset_list.txt output n_dsets(max:25304802) ceph_conf poolname\n");
}
//int main(int argc, const char *argv[])
int main(int argc, char* argv[])
{
    int size, rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    char *in_filename, *out_filename;
    char **all_dset_names = NULL, *all_dset_names_1d = NULL;
    char tmp_input[256];
    hsize_t dims[1];
    int *all_dset_sizes = NULL;
    int i, j, n_write = NDSET, my_n_write = 0, my_write_start;
    hid_t file_id, dset_id, filespace,fapl;
    hsize_t tsize=0;
    if(argc!=6) {
	print_usage();
	ERROR;
    }
    in_filename  = argv[1];
    out_filename = argv[2];
    n_write = atoi(argv[3]);
    //ceph_conf argv[4]
    //poolname argv[5]
//Rados VOL Related Code Change
	rados_t cluster;
	/*Rados VOL Related Code Change*/
	if(rados_create(&cluster, NULL) < 0)
		ERROR;
	if(rados_conf_read_file(cluster, argv[4]) < 0)
		ERROR;

	/* Initialize VOL */
	if(H5VLrados_init(cluster, argv[5]) < 0)
		ERROR;

	/* Set up FAPL */
	if((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0)
		ERROR;
	if(H5Pset_fapl_rados(fapl, MPI_COMM_WORLD, MPI_INFO_NULL) < 0)
		ERROR;
	if(H5Pset_all_coll_metadata_ops(fapl, true) < 0)
		ERROR;
//Rados VOL Related Code Change 

    if (rank == 0) 
        printf("Writing %d datasets.\n", n_write);
    
    all_dset_names    = (char**)calloc(n_write, sizeof(char*));
    all_dset_names_1d = (char*)calloc(n_write*NAMELEN, sizeof(char));
    all_dset_sizes = (int*)calloc(n_write, sizeof(int));
    for (i = 0; i < n_write; i++) 
        all_dset_names[i] = all_dset_names_1d + i*NAMELEN;

    // Rank 0 read from input file
    if (rank == 0) {
        printf("Reading from %s\n", in_filename);
        FILE *pm_file = fopen(in_filename, "r");
        if (NULL == pm_file) {
            fprintf(stderr, "Error opening file: %s\n", in_filename);
            return (-1);
        }

        // /3523/55144/1/coadd, 1D: 4619, 147808
        i = 0;
        char dset_ndim[4];
        int  dset_nelem;
        while (fgets(tmp_input, 255, pm_file) != NULL ) {
      	   // printf("Reading %d,%d line\n",i,n_write);
	    if (i >= n_write) 
                break;
            sscanf(tmp_input+1, "%[^,], %[^:]: %d, %d", 
                               all_dset_names[i], dset_ndim, &dset_nelem, &all_dset_sizes[i]);
            if (strcmp(dset_ndim, "1D") != 0) {
                printf("Dimension is %s!\n", dset_ndim);
                continue;
            }
            for (j = 0; j < strlen(all_dset_names[i]); j++) {
                if (all_dset_names[i][j] == '/') 
                    all_dset_names[i][j] = '-';
                
            }
            i++;
        }

        fclose(pm_file);
        n_write = i;
	
         for (i = 0; i < n_write; i++) {
             tsize+= all_dset_sizes[i]; 
         }
	 printf("Total size is %ld bytes\n",tsize);
    }

    //fflush(stdout);
    my_n_write  = n_write;
    MPI_Bcast( &n_write, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast( all_dset_names_1d, n_write*NAMELEN, MPI_CHAR, 0, MPI_COMM_WORLD);
    MPI_Bcast( all_dset_sizes, n_write, MPI_INT, 0, MPI_COMM_WORLD);
   // fflush(stdout);
    // Distribute work evenly
    // Last rank may have extra work
    my_n_write = n_write / size;
    my_write_start = rank * my_n_write;
    if (rank == size - 1) 
        my_n_write += n_write % size;
  //  printf("rank:%d,start:%d,length:%d\n",rank,my_write_start,my_n_write);
    file_id = H5Fcreate(out_filename, H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
    //fflush(stdout); 
    if (file_id < 0) {
        printf("Error creating a file [%s]\n", out_filename);
        ERROR;
    }
    else {
	if (rank==0)
		printf("file creation ok\n");	
    }
    /* H5Fclose(file_id); */

    MPI_Barrier(MPI_COMM_WORLD);
    timer_on(1);
    for (i = 0; i < n_write; i++) {
        dims[0] = all_dset_sizes[i];
        filespace = H5Screate_simple(1, dims, NULL); 
//	if (rank==0) printf("dset:%d,size:%d\n",i,all_dset_sizes[i]);
        dset_id = H5Dcreate(file_id, all_dset_names[i], H5T_NATIVE_CHAR, filespace, 
                            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Sclose(filespace);
        H5Dclose(dset_id);
	//printf("rank:%d, creating dset:%d\n",rank,i);
    }
    H5Fclose(file_id);
    MPI_Barrier (MPI_COMM_WORLD);
    timer_off(1);
    file_id=H5Fopen(out_filename, H5F_ACC_RDWR, fapl);
    if(file_id <0){
	if(rank==0)  printf("Error opening a file [%s]\n", out_filename);
        ERROR;
    }else{
    	if(rank==0)  printf("Ok opening existing file[%s]\n",out_filename);
    }

    fflush(stdout);
    hsize_t count = 0;
    //char *buf = NULL;
    int prev_buf_size;
    herr_t ret;
    MPI_Barrier (MPI_COMM_WORLD);
    timer_on (0);
    for (i = 0; i < my_n_write; i++) {
	
//	printf("Rank %d starting\n",rank);
        count = all_dset_sizes[i+my_write_start];
        dset_id = H5Dopen(file_id, all_dset_names[my_write_start+i], H5P_DEFAULT);
        if (dset_id < 0) {
            printf("Error opening the dataset [%s]\n", all_dset_names[my_write_start+i]);
            continue;
        }
/*        if (buf == NULL) {
            buf = (char*)calloc(sizeof(char), all_dset_sizes[i+my_write_start]);
            prev_buf_size = all_dset_sizes[i+my_write_start];
        }
        else {
            if (prev_buf_size < all_dset_sizes[i+my_write_start]) {
                buf = realloc(buf, all_dset_sizes[i+my_write_start]);
            }
        }
*/
	char * buf=(char*)malloc(sizeof(char)*all_dset_sizes[i+my_write_start]);
	memset(buf,'\0',sizeof(char)*all_dset_sizes[i+my_write_start]);
	//if(buf==NULL) printf("rank:%d,buf alocate error\n",rank);
        buf[0] = all_dset_names[my_write_start+i][1]; 
        buf[1] = all_dset_names[my_write_start+i][2]; 
        buf[2] = all_dset_names[my_write_start+i][3]; 
        buf[3] = all_dset_names[my_write_start+i][4]; 
	//printf("rank:%d,buf:%s\n",rank,buf);
//	if (i==0)printf("rank:%d,dset:%d,size:%d\n",rank,i+my_write_start,all_dset_sizes[i+my_write_start]);
        ret = H5Dwrite(dset_id, H5T_NATIVE_CHAR, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf);
        if (ret < 0) {
            printf("Error writing to the dataset [%s]\n", all_dset_names[my_write_start+i]);
            H5Dclose(dset_id);
            continue;
        }
	free (buf);
  //      if(rank==size-1) printf("Proc %d: written dset [%s]\n", rank, all_dset_names[my_write_start+i]);
        /* all_dset_names[my_write_start+i] */
        H5Dclose(dset_id);
    }
    //fflush(stdout);
    //printf("rank:%d, done with dset write\n",rank);
    MPI_Barrier (MPI_COMM_WORLD);
    timer_off (0);

    
    if (rank == 0)
    {
	timer_msg(1);//dset creation cost
	timer_msg(0);//dset write cost
	printf("%ld MB\n",tsize/1024/1024);

    }

error:
    H5E_BEGIN_TRY {
        H5Fclose(file_id);
        H5Pclose(fapl);
    } H5E_END_TRY;
        (void)MPI_Finalize();

    return 0;
}
