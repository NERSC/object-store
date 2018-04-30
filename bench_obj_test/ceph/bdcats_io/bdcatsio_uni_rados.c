/****** Copyright Notice ***
 *
 * PIOK - Parallel I/O Kernels - VPIC-IO, VORPAL-IO, and GCRM-IO, Copyright
 * (c) 2015, The Regents of the University of California, through Lawrence
 * Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy).  All rights reserved.
 *
 * If you have questions about your rights to use or distribute this
 * software, please contact Berkeley Lab's Innovation & Partnerships Office
 * at  IPO@lbl.gov.
 *
 * NOTICE.  This Software was developed under funding from the U.S.
 * Department of Energy and the U.S. Government consequently retains
 * certain rights. As such, the U.S. Government has been granted for itself
 * and others acting on its behalf a paid-up, nonexclusive, irrevocable,
 * worldwide license in the Software to reproduce, distribute copies to the
 * public, prepare derivative works, and perform publicly and display
 * publicly, and to permit other to do so.
 *
 ****************************/

/**
 *
 * Email questions to SByna@lbl.gov
 * Scientific Data Management Research Group
 * Lawrence Berkeley National Laboratory
 *
*/

// Description: This is a simple benchmark based on VPIC's I/O interface
//		Each process reads a specified number of particles into 
//		a hdf5 output file using only HDF5 calls
// Author:	Suren Byna <SByna@lbl.gov>
//		Lawrence Berkeley National Laboratory, Berkeley, CA
// Created:	in 2011
// Modified:	01/06/2014 --> Removed all H5Part calls and using HDF5 calls
// 


#include <math.h>
#include "h5rados_example.h"
// A simple timer based on gettimeofday
#include "timer.h"
struct timeval start_time[3];
float elapse[3];

// HDF5 specific declerations
herr_t ierr;
hid_t file_id, dset_id;
hid_t filespace, memspace;
hid_t fapl;

// Variables and dimensions
long numparticles = 8388608;	// 8  meg particles per process
long long total_particles, offset;

float *x, *y, *z;
float *px, *py, *pz;
int *id1, *id2;
int x_dim = 64;
int y_dim = 64; 
int z_dim = 64;

// Uniform random number
inline double uniform_random_number() 
{
	return (((double)rand())/((double)(RAND_MAX)));
}

void print_data(int n)
{
	int i;
	for (i = 0; i < n; i++)
		printf("%f %f %f %d %d %f %f %f\n", x[i], y[i], z[i], id1[i], id2[i], px[i], py[i], pz[i]);
}

// Create HDF5 file and read data
void read_h5_data(int rank)
{
	// Note: printf statements are inserted basically 
	// to check the progress. Other than that they can be removed
	dset_id = H5Dopen2(file_id, "x", H5P_DEFAULT);
        ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, x);
        H5Dclose(dset_id);
	if (rank == 0) printf ("Read variable 1 \n");

	dset_id = H5Dopen2(file_id, "y", H5P_DEFAULT);
        ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, y);
	//dset_id = H5Dcreate(file_id, "y", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
        //ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, fapl, y);
        H5Dclose(dset_id);
	if (rank == 0) printf ("Read variable 2 \n");

	dset_id = H5Dopen2(file_id, "z", H5P_DEFAULT);
        ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, z);
	//dset_id = H5Dcreate(file_id, "z", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
        //ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, fapl, z);
        H5Dclose(dset_id);
	if (rank == 0) printf ("Read variable 3 \n");

	dset_id = H5Dopen2(file_id, "id1", H5P_DEFAULT);
        ierr = H5Dread(dset_id, H5T_NATIVE_INT, memspace, filespace, H5P_DEFAULT, id1);
	//dset_id = H5Dcreate(file_id, "id1", H5T_NATIVE_INT, filespace, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
        //ierr = H5Dread(dset_id, H5T_NATIVE_INT, memspace, filespace, fapl, id1);
        H5Dclose(dset_id);
	if (rank == 0) printf ("Read variable 4 \n");

	dset_id = H5Dopen2(file_id, "id2", H5P_DEFAULT);
        ierr = H5Dread(dset_id, H5T_NATIVE_INT, memspace, filespace, H5P_DEFAULT, id2);
	//dset_id = H5Dcreate(file_id, "id2", H5T_NATIVE_INT, filespace, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
        //ierr = H5Dread(dset_id, H5T_NATIVE_INT, memspace, filespace, fapl, id2);
        H5Dclose(dset_id);
	if (rank == 0) printf ("Read variable 5 \n");

	dset_id = H5Dopen2(file_id, "px", H5P_DEFAULT);
        ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, px);
	//dset_id = H5Dcreate(file_id, "px", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
        //ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, fapl, px);
        H5Dclose(dset_id);
	if (rank == 0) printf ("Read variable 6 \n");

	dset_id = H5Dopen2(file_id, "py", H5P_DEFAULT);
        ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, py);
	//dset_id = H5Dcreate(file_id, "py", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
        //ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, fapl, py);
        H5Dclose(dset_id);
	if (rank == 0) printf ("Read variable 7 \n");

	dset_id = H5Dopen2(file_id, "pz", H5P_DEFAULT);
        ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, H5P_DEFAULT, pz);
	//dset_id = H5Dcreate(file_id, "pz", H5T_NATIVE_FLOAT, filespace, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
        //ierr = H5Dread(dset_id, H5T_NATIVE_FLOAT, memspace, filespace, fapl, pz);
        H5Dclose(dset_id);
	if (rank == 0) printf ("Read variable 8 \n");

	print_data(3);
}

int main (int argc, char* argv[]) 
{
	
	MPI_Init(&argc,&argv);
	if(argc != 5)
        	PRINTF_ERROR("argc != 5 file pool ceph.conf nparticles\n");
	int my_rank, num_procs;
	rados_t cluster;
	hid_t fapl=-1;
	MPI_Comm_rank (MPI_COMM_WORLD, &my_rank);
	MPI_Comm_size (MPI_COMM_WORLD, &num_procs);
	numparticles = (atoi (argv[4]))*1024*1024;	
        MPI_Info info  = MPI_INFO_NULL;
	//if (my_rank == 0) {printf ("Number of paritcles: %ld \n", numparticles);}
        if(my_rank==0&&numparticles<1024*1024*1024) printf("Number of particles is %d million\n",numparticles/1024/1024);
        else if(my_rank==0&&numparticles>=1024*1024*1024)printf("Number of particles is %d billion\n",numparticles/1024/1024/1024);
	x=(float*)malloc(numparticles*sizeof(double));
	y=(float*)malloc(numparticles*sizeof(double));
	z=(float*)malloc(numparticles*sizeof(double));

	px=(float*)malloc(numparticles*sizeof(double));
	py=(float*)malloc(numparticles*sizeof(double));
	pz=(float*)malloc(numparticles*sizeof(double));

	id1=(int*)malloc(numparticles*sizeof(int));
	id2=(int*)malloc(numparticles*sizeof(int));

	/*if (my_rank == 0)
	{
		printf ("Finished initializeing particles \n");
	}
	*/
	MPI_Barrier (MPI_COMM_WORLD);
	timer_on (0);

	MPI_Allreduce(&numparticles, &total_particles, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
        MPI_Scan(&numparticles, &offset, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);	
	offset -= numparticles;


	char *file_name = argv[1];

/*Rados VOL Related Code Change*/
        if(rados_create(&cluster, NULL) < 0)
                ERROR;
        if(rados_conf_read_file(cluster, argv[3]) < 0)
                ERROR;

        /* Initialize VOL */
        if(H5VLrados_init(cluster, argv[2]) < 0)
                ERROR;

        /* Set up FAPL */
        if((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0)
                ERROR;
        if(H5Pset_fapl_rados(fapl, MPI_COMM_WORLD, MPI_INFO_NULL) < 0)
                ERROR;
/*Rados VOL Related Code Change*/

	/* Create file */
	file_id = H5Fopen(file_name , H5F_ACC_RDONLY, fapl);
	if(file_id < 0) {
		printf("Error with creating file!\n");
		goto done;
	}

	if (my_rank == 0)
	{
		printf ("Opened HDF5 file... \n");
	}

	filespace = H5Screate_simple(1, (hsize_t *) &total_particles, NULL);
        memspace =  H5Screate_simple(1, (hsize_t *) &numparticles, NULL);

	//printf("total_particles: %lld\n", total_particles);
	//printf("my particles   : %ld\n", numparticles);

        //H5Pset_dxpl_mpio(fapl, H5FD_MPIO_COLLECTIVE);
        H5Sselect_hyperslab(filespace, H5S_SELECT_SET, (hsize_t *) &offset, NULL, (hsize_t *) &numparticles, NULL);

	MPI_Barrier (MPI_COMM_WORLD);
	timer_on (1);

	if (my_rank == 0) printf ("Before reading particles \n");
	read_h5_data(my_rank);

	MPI_Barrier (MPI_COMM_WORLD);
	timer_off (1);
	if (my_rank == 0) printf ("After reading particles \n");

	free(x); free(y); free(z);
	free(px); free(py); free(pz);
	free(id1);
	free(id2);

	MPI_Barrier (MPI_COMM_WORLD);

	timer_off (0);

	if (my_rank == 0)
	{
		printf ("\nTiming results\n");
		timer_msg (1, "just reading data");
		timer_msg (0, "opening, reading, closing file");
		printf ("\n");
	}

	H5Sclose(memspace);
        H5Sclose(filespace);
        H5Pclose(fapl);
        H5Fclose(file_id);
	if (my_rank == 0) printf ("After closing HDF5 file \n");

error:
    H5E_BEGIN_TRY {
        H5Fclose(file_id);
        H5Pclose(fapl);
    } H5E_END_TRY;

done:
	H5close();
	MPI_Finalize();

	return 0;
}
