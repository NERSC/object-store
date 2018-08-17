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
//		Each process writes a specified number of particles into 
//		a hdf5 output file using only HDF5 calls
// Author:	Suren Byna <SByna@lbl.gov>
//		Lawrence Berkeley National Laboratory, Berkeley, CA
// Created:	in 2011
// Modified:	01/06/2014 --> Removed all H5Part calls and using HDF5 calls
// 


#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <Python.h>
#include <numpy/arrayobject.h>
#include <string.h>
#include <assert.h>
#include "../src/python_vol.h"
// A simple timer based on gettimeofday
#include "timer.h"
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
struct timeval start_time[3];
float elapse[3];

// HDF5 specific declerations
herr_t ierr;
hid_t file_id, dset_id,dataspaceId,dset_id1,dset_id2,dset_id3,dset_id4,dset_id5,dset_id6,dset_id7;
//hid_t H5S_ALL=H5S_ALL;
//hid_t H5S_ALL=H5S_ALL;
hid_t plist_id=H5P_DEFAULT;

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

// Initialize particle data
void init_particles ()
{
	int i;
	for (i=0; i<numparticles; i++) 
	{
		id1[i] = i;
		id2[i] = i*2;
		x[i] = uniform_random_number()*x_dim;
		y[i] = uniform_random_number()*y_dim;
		z[i] = ((double)id1[i]/numparticles)*z_dim;    
		px[i] = uniform_random_number()*x_dim;
		py[i] = uniform_random_number()*y_dim;
		pz[i] = ((double)id2[i]/numparticles)*z_dim;    
	}
}

// Create HDF5 file and write data
void create_and_write_synthetic_h5_data(int rank)
{
	// Note: printf statements are inserted basically 
	// to check the progress. Other than that they can be removed
	dset_id = H5Dcreate2(file_id, "x", H5T_NATIVE_FLOAT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        ierr = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, plist_id, x);
        //H5Dclose(dset_id);
        //if (rank == 0) printf ("Written variable 1 \n");

        dset_id1 = H5Dcreate2(file_id, "y", H5T_NATIVE_FLOAT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        ierr = H5Dwrite(dset_id1, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, plist_id, y);
        //H5Dclose(dset_id);
        //if (rank == 0) printf ("Written variable 2 \n");

        dset_id2 = H5Dcreate2(file_id, "z", H5T_NATIVE_FLOAT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        ierr = H5Dwrite(dset_id2, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, plist_id, z);
        //H5Dclose(dset_id);
        //if (rank == 0) printf ("Written variable 3 \n");

        dset_id3 = H5Dcreate2(file_id, "id1", H5T_NATIVE_INT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        ierr = H5Dwrite(dset_id3, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, plist_id, id1);
        //H5Dclose(dset_id);
        //if (rank == 0) printf ("Written variable 4 \n");

        dset_id4 = H5Dcreate2(file_id, "id2", H5T_NATIVE_INT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        ierr = H5Dwrite(dset_id4, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, plist_id, id2);
        //H5Dclose(dset_id);
        //if (rank == 0) printf ("Written variable 5 \n");

        dset_id5 = H5Dcreate2(file_id, "px", H5T_NATIVE_FLOAT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        ierr = H5Dwrite(dset_id5, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, plist_id, px);
        //H5Dclose(dset_id);
        //if (rank == 0) printf ("Written variable 6 \n");

        dset_id6 = H5Dcreate2(file_id, "py", H5T_NATIVE_FLOAT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        ierr = H5Dwrite(dset_id6, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, plist_id, py);
        //H5Dclose(dset_id);
        //if (rank == 0) printf ("Written variable 7 \n");

        dset_id7 = H5Dcreate2(file_id, "pz", H5T_NATIVE_FLOAT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        ierr = H5Dwrite(dset_id7, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, plist_id, pz);
        //H5Dclose(dset_id);
        //if (rank == 0) printf ("Written variable 8 \n");
}

int main (int argc, char* argv[]) 
{
	char *file_name = argv[1];
	const char plugin_name[7]="python";	
	//MPI_Init(&argc,&argv);
	int my_rank, num_procs;
	hid_t acc_tpl, under_fapl, vol_id, vol_id2;
	if (argc == 3){
	  numparticles = (atoi (argv[2]))*1024*1024;
	}
	else{
	  numparticles = 8*1024*1024;
	}
	x=(float*)malloc(numparticles*sizeof(double));
	y=(float*)malloc(numparticles*sizeof(double));
	z=(float*)malloc(numparticles*sizeof(double));

	px=(float*)malloc(numparticles*sizeof(double));
	py=(float*)malloc(numparticles*sizeof(double));
	pz=(float*)malloc(numparticles*sizeof(double));

	id1=(int*)malloc(numparticles*sizeof(int));
	id2=(int*)malloc(numparticles*sizeof(int));

	init_particles ();
	/*
	if (my_rank == 0)
	{
		printf ("Finished initializeing particles \n");
	}
	*/
	// h5part_int64_t alignf = 8*1024*1024;

	//MPI_Barrier (comm);
	timer_on (0);

       //Initialize Python and Numpy Routine
        Py_Initialize();
        import_array();



        dataspaceId =  H5Screate_simple(1, (hsize_t *) &numparticles, NULL);
	timer_on (1);
	//timer create timer write timer write timer_true;  
	//if (my_rank == 0) printf ("Before writing particles \n");
	create_and_write_synthetic_h5_data(my_rank);

	//MPI_Barrier (comm);
	timer_off (1);
	H5Fclose(file_id);
	free(x); free(y); free(z);
	free(px); free(py); free(pz);
	free(id1);
	free(id2);

	//MPI_Barrier (comm);

	timer_off (0);

	if (my_rank == 0)
	{
		//printf ("\nTiming results\n");
		timer_msg (1);
		timer_msg (0);
		printf ("\n");
	}
	Py_Finalize();
	//MPI_Finalize();

	return 0;
}
