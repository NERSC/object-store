// Description: This is a simple benchmark based on VPIC's I/O interface
//		Each process writes a specified number of particles into 
//		a h5part output file
// Author:	Suren Byna <SByna@lbl.gov>
//		Lawrence Berkeley National Laboratory, Berkeley, CA
// 


#include "H5Part.h"
#include "H5Block.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

// A simple timer based on gettimeofday
#include "./timer.h"
struct timeval start_time[3];
float elapse[3];

H5PartFile* file;
int numparticles = 8388608;	// 8  meg particles per process
float *x, *y, *z;
float *px, *py, *pz;
h5part_int32_t *id1, *id2;
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

// Create H5Part file and write data
void create_and_write_synthetic_h5part_data(int rank) 
{
	// Note: printf statements are inserted basically 
	// to check the progress. Other than that they can be removed
	H5PartSetStep(file, 0); // only 1 timestep in this file
	if (rank == 0) printf ("Step is set \n");
	H5PartSetNumParticles(file, numparticles);
	if (rank == 0) printf ("Num particles is set \n");
	float var_data_size;

	timer_on (2);
	H5PartWriteDataFloat32(file,"x",x); 
	timer_off (2);
	var_data_size = numparticles * sizeof (h5part_float32_t)/(1024*1024);
#ifdef TIME_DEBUG
	printf ("Rank: %d, wrote %.2f MB, time: %f sec; rate: %f\n", rank, var_data_size, elapse[2], var_data_size/elapse[2]);
#endif
	timer_reset(2);
	if (rank == 0) printf ("Written variable 1 \n");

	timer_on (2);
	H5PartWriteDataFloat32(file,"y",y);
	if (rank == 0) printf ("Written variable 2 \n");
	timer_off (2);
	var_data_size = numparticles * sizeof (h5part_float32_t)/(1024*1024);
#ifdef TIME_DEBUG
	printf ("Rank: %d, wrote %.2f MB, time: %f sec; rate: %f\n", rank, var_data_size, elapse[2], var_data_size/elapse[2]);
#endif
	timer_reset(2);

	timer_on (2);
	H5PartWriteDataFloat32(file,"z",z);
	if (rank == 0) printf ("Written variable 3 \n");
	timer_off (2);
	var_data_size = numparticles * sizeof (h5part_float32_t)/(1024*1024);
#ifdef TIME_DEBUG
	printf ("Rank: %d, wrote %.2f MB, time: %f sec; rate: %f\n", rank, var_data_size, elapse[2], var_data_size/elapse[2]);
#endif
	timer_reset(2);

	timer_on (2);
	H5PartWriteDataInt32(file,"id1",id1);
	if (rank == 0) printf ("Written variable 4 \n");
	timer_off (2);
	var_data_size = numparticles * sizeof (h5part_int32_t)/(1024*1024);
#ifdef TIME_DEBUG
	printf ("Rank: %d, wrote %.2f MB, time: %f sec; rate: %f\n", rank, var_data_size, elapse[2], var_data_size/elapse[2]);
#endif
	timer_reset(2);

	timer_on (2);
	H5PartWriteDataInt32(file,"id2",id2);
	if (rank == 0) printf ("Written variable 5 \n");
	timer_off (2);
	var_data_size = numparticles * sizeof (h5part_int32_t)/(1024*1024);
#ifdef TIME_DEBUG
	printf ("Rank: %d, wrote %.2f MB, time: %f sec; rate: %f\n", rank, var_data_size, elapse[2], var_data_size/elapse[2]);
#endif
	timer_reset(2);

	timer_on (2);
	H5PartWriteDataFloat32(file,"px",px); 
	if (rank == 0) printf ("Written variable 6 \n");
	timer_off (2);
	var_data_size = numparticles * sizeof (h5part_float32_t)/(1024*1024);
#ifdef TIME_DEBUG
	printf ("Rank: %d, wrote %.2f MB, time: %f sec; rate: %f\n", rank, var_data_size, elapse[2], var_data_size/elapse[2]);
#endif
	timer_reset(2);

	timer_on (2);
	H5PartWriteDataFloat32(file,"py",py);
	if (rank == 0) printf ("Written variable 7 \n");
	timer_off (2);
	var_data_size = numparticles * sizeof (h5part_float32_t)/(1024*1024);
#ifdef TIME_DEBUG
	printf ("Rank: %d, wrote %.2f MB, time: %f sec; rate: %f\n", rank, var_data_size, elapse[2], var_data_size/elapse[2]);
#endif
	timer_reset(2);

	timer_on (2);
	H5PartWriteDataFloat32(file,"pz",pz);
	if (rank == 0) printf ("Written variable 8 \n");
	timer_off (2);
	var_data_size = numparticles * sizeof (h5part_float32_t)/(1024*1024);
#ifdef TIME_DEBUG
	printf ("Rank: %d, wrote %.2f MB, time: %f sec; rate: %f\n", rank, var_data_size, elapse[2], var_data_size/elapse[2]);
#endif
	timer_reset(2);
}

int main (int argc, char* argv[]) 
{
	char *file_name = argv[1];
	
	MPI_Init(&argc,&argv);
	int my_rank, num_procs;
	MPI_Comm_rank (MPI_COMM_WORLD, &my_rank);
	MPI_Comm_size (MPI_COMM_WORLD, &num_procs);

	if (argc == 3)
	{
		numparticles = (atoi (argv[2]))*1024*1024;
	}
	else
	{
		numparticles = 32*1024*1024;
	}

	if (my_rank == 0) {printf ("Number of paritcles: %ld \n", numparticles);}

	x=(float*)malloc(numparticles*sizeof(double));
	y=(float*)malloc(numparticles*sizeof(double));
	z=(float*)malloc(numparticles*sizeof(double));

	px=(float*)malloc(numparticles*sizeof(double));
	py=(float*)malloc(numparticles*sizeof(double));
	pz=(float*)malloc(numparticles*sizeof(double));

	id1=(h5part_int32_t*)malloc(numparticles*sizeof(h5part_int32_t));
	id2=(h5part_int32_t*)malloc(numparticles*sizeof(h5part_int32_t));

	init_particles ();

	if (my_rank == 0)
	{
		printf ("Finished initializing particles \n");
	}

	MPI_Barrier (MPI_COMM_WORLD);
	timer_on (0);
	//file = H5PartOpenFileParallelAlign(file_name, H5PART_WRITE, MPI_COMM_WORLD, alignf);
	// file = H5PartOpenFileParallel (file_name, H5PART_WRITE | H5PART_VFD_MPIPOSIX | H5PART_FS_LUSTRE, MPI_COMM_WORLD);
	file = H5PartOpenFileParallel (file_name, H5PART_WRITE | H5PART_FS_LUSTRE, MPI_COMM_WORLD);

	if (my_rank == 0)
	{
		printf ("Opened H5Part file... \n");
	}
	// Throttle and see performance
	// H5PartSetThrottle (file, 10);

	H5PartWriteFileAttribString(file, "Origin", "Tested by Suren");

	MPI_Barrier (MPI_COMM_WORLD);
	timer_on (1);

	if (my_rank == 0) printf ("Before writing particles \n");
	create_and_write_synthetic_h5part_data(my_rank);

	MPI_Barrier (MPI_COMM_WORLD);
	timer_off (1);
	if (my_rank == 0) printf ("After writing particles \n");

	H5PartCloseFile(file);
	if (my_rank == 0) printf ("After closing particles \n");

	free(x); free(y); free(z);
	free(px); free(py); free(pz);
	free(id1);
	free(id2);

	MPI_Barrier (MPI_COMM_WORLD);

	timer_off (0);

	if (my_rank == 0)
	{
		printf ("\nTiming results\n");
		timer_msg (1, "just writing data");
		timer_msg (0, "opening, writing, closing file");
		printf ("\n");
	}

	MPI_Finalize();

	return 0;
}
