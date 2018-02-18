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


/* 
 * File:        vorpalio.c
 * Author:      Mark Howison
 * Created:     2009-09-15
 * Description: Benchmark application that simulates I/O for TechX's VORPAL
 *              code. The key feature is non-uniform grid decomposition
 *              to accommodate VORPAL's load-balancing scheme.
 */

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <mpi.h>
#include <H5Part.h>
#include <H5Block.h>

#define ONE_MEGABYTE 1048576

#define ERROR(msg,my_rank) do{\
    fprintf (stderr, "rank %d: %s (%d): %s!\n",\
            my_rank, __FILE__, __LINE__, msg);\
    fflush (stderr);\
    exit (EXIT_FAILURE);\
}while(0);


typedef struct {
    int rank, nprocs;
    int align_mb, block[3], adjust[3], decomp[3];
    int nComps, nSteps, nIters;
    int verbosity, lustre, mpiposix, dryrun;
    char* output;
} Params;


typedef struct {
    int i,j,k;
    h5part_int64_t size;
    h5part_int64_t dims[3];
    h5part_int64_t layout[6];
} Block;


/*****************************************************************************/
/* Argument helper functions                                                 */
/*****************************************************************************/

void PrintUsage (int rank)
{
    if (rank == 0) {
        printf ("Usage: (-n and -b are required!)\n");
        printf (" -a        alignment in MB (default 1MB)\n");
        printf (" -b x y z  block dimensions\n");
        printf (" -c        number of components (default 3)\n");
        printf (" -d x y z  block +/- adjustments (default 0 0 0)\n");
        printf (" -i        iterations (default 1)\n");
        printf (" -n x y z  block decomposition (x*y*z = nprocs)\n");
        printf (" -o        output directory (default 'output')\n");
        printf (" -t        timesteps (default 1)\n");
        printf (" -v        verbosity level (default 3)\n");
        printf (" -lustre   enable lustre-specific tuning\n");
        printf (" -mpiposix use MPI-mpiposix VFD\n");
        printf (" -dryrun   do not perform write\n");
    }
    MPI_Finalize();
    exit (EXIT_SUCCESS);
}

void ParseArgs (int argc, char** argv, Params* params)
{
    int i;
    int check = 0;
    int nRequired = 2;

    // default values
    params->adjust[0]   = 0;
    params->adjust[1]   = 0;
    params->adjust[2]   = 0;
    params->align_mb    = 1;
    params->nComps      = 3;
    params->nSteps      = 1;
    params->nIters      = 1;
    params->verbosity   = 3;
    params->lustre      = 0;
    params->mpiposix    = 0;
    params->dryrun      = 0;
    params->output      = "output";
   
    i = 1;
    while (i < argc)
    {
        if (strcmp(argv[i],"-b") == 0)
        {
            i++;
            params->block[0] = atoi (argv[i]);
            i++;
            params->block[1] = atoi (argv[i]);
            i++;
            params->block[2] = atoi (argv[i]);
            check++;
        }
        else if (strcmp(argv[i],"-n") == 0)
        {
            i++;
            params->decomp[0] = atoi (argv[i]);
            i++;
            params->decomp[1] = atoi (argv[i]);
            i++;
            params->decomp[2] = atoi (argv[i]);
            check++;
        }
        else if (strcmp(argv[i],"-d") == 0)
        {
            i++;
            params->adjust[0] = atoi (argv[i]);
            i++;
            params->adjust[1] = atoi (argv[i]);
            i++;
            params->adjust[2] = atoi (argv[i]);
        }
        else if (strcmp(argv[i],"-a") == 0)
        {
            i++;
            params->align_mb = atoi (argv[i]);
        }
        else if (strcmp(argv[i],"-c") == 0)
        {
            i++;
            params->nComps = atoi (argv[i]);
        }
        else if (strcmp(argv[i],"-t") == 0)
        {
            i++;
            params->nSteps = atoi (argv[i]);
        }
        else if (strcmp(argv[i],"-i") == 0)
        {
            i++;
            params->nIters = atoi (argv[i]);
        }
        else if (strcmp(argv[i],"-v") == 0)
        {
            i++;
            params->verbosity = atoi (argv[i]);
        }
        else if (strcmp(argv[i],"-o") == 0)
        {
            i++;
            params->output = (char*) malloc (strlen (argv[i]) + 1);
            strcpy (params->output, argv[i]);
        }
        else if (strcmp(argv[i],"-lustre") == 0)
        {
            params->lustre = 1;
        }
        else if (strcmp(argv[i],"-mpiposix") == 0)
        {
            params->mpiposix = 1;
        }
        else if (strcmp(argv[i],"-dryrun") == 0)
        {
            params->dryrun = 1;
        }
        else if (strcmp(argv[i],"--help") == 0)
        {
            PrintUsage (params->rank);
        }
        else
        {
            if (params->rank == 0) {
                fprintf (stderr, "%s: unrecognized argument %s \n",
                        argv[0], argv[i]);
            }
            PrintUsage (params->rank);
        }
        i++;
    }

    if (check != nRequired) PrintUsage (params->rank);

    if (params->rank == 0) {
        printf ("Parameters:\n");
        printf ("\tblock dimensions = (%d,%d,%d)\n",
                params->block[0], params->block[1], params->block[2]);
        printf ("\tblock adjustments = +/- (%d,%d,%d)\n",
                params->adjust[0], params->adjust[1], params->adjust[2]);
        printf ("\tblock decomposition = (%d,%d,%d)\n",
                params->decomp[0], params->decomp[1], params->decomp[2]);
        printf ("\tcomponents = %d\n", params->nComps);
        printf ("\ttimesteps = %d\n", params->nSteps);
        printf ("\titerations = %d\n", params->nIters);
        printf ("\talignment = %d MB\n", params->align_mb);
        printf ("\toutput path = '%s'\n", params->output);
        printf ("\tverbosity level = %d\n", params->verbosity);
        printf ("\tmpiposix = %d\n", params->mpiposix);
        printf ("\tlustre tuning = %d\n", params->lustre);
        printf ("\tdryrun = %d\n", params->dryrun);
    }

    if (       params->nprocs
            != params->decomp[0] * params->decomp[1] * params->decomp[2])
    {
        ERROR("Decomposition does not contain the correct number of procs",
                params->rank);
    }
}


/*****************************************************************************/
/* Block / write functions                                                   */
/*****************************************************************************/

void WriteBlock (Params *params, Block *block, h5part_float64_t *data)
{
    int i;
    char flags;
    char* filename;
    double time;
    H5PartFile* file;
    h5part_int64_t status;
    
    filename = (char*) malloc (strlen(params->output) + 16);
    sprintf(filename, "%s/vorpalio.h5", params->output);

    // start timer
    //MPI_Barrier (MPI_COMM_WORLD);
    time = MPI_Wtime();
    
    flags = H5PART_WRITE;
    // if (params->lustre) flags |= H5PART_FS_LUSTRE;
    // if (params->mpiposix) flags |= H5PART_VFD_MPIPOSIX;

    file = H5PartOpenFileParallelAlign (
            filename,
            flags,
            MPI_COMM_WORLD,
            params->align_mb * ONE_MEGABYTE);

    if (!file) {
        ERROR("Could not open H5Part file", params->rank);
    }

    for (i=0; i<params->nSteps; i++)
    {
	if (params->rank == 0)
		printf ("Here 1 ... \n");
        status = H5PartSetStep (file, i);
        if (status != H5PART_SUCCESS) {
            ERROR("H5PartSetStep was unsuccessful", params->rank);
        }

	if (params->rank == 0)
		printf ("Here 2 ... \n");
        status = H5BlockDefine3DFieldLayout (file,
                block->layout[0], block->layout[1],
                block->layout[2], block->layout[3],
                block->layout[4], block->layout[5]);
        if (status != H5PART_SUCCESS) {
            ERROR("H5Block layout was unsuccessful", params->rank);
        }

        status = H5Block3dWriteScalarFieldFloat64 (file, "E", data);
        if (status != H5PART_SUCCESS) {
            ERROR("H5Block write was unsuccessful", params->rank);
        }
    }

    H5PartCloseFile (file);

    // stop timer
    //MPI_Barrier (MPI_COMM_WORLD);
    time = MPI_Wtime() - time;

    if (params->rank == 0) {
        float size;
        size = params->nprocs *
               params->nSteps *
               block->size *
               sizeof(h5part_float64_t);
        size /= ONE_MEGABYTE;
        printf("size = %g MB\n", size);
        printf("time = %g s\n", time);
        printf("bandwidth = %g MB/s\n", size / time);
    }
}

void GetBlockIndices (int rank, int *decomp, Block *block)
{
    block->i = rank % decomp[0];
    block->j = (rank / decomp[0]) % decomp[1];
    block->k = rank / (decomp[0] * decomp[1]);
}

void GetBlockLayout (Params *params, Block *block)
{
    int dx, dy, dz;

    dx = (block->i % 3) - 1;
    dy = (block->j % 3) - 1;
    dz = (block->k % 3) - 1;

    block->dims[0] = params->block[0] + dx*params->adjust[0];
    block->dims[1] = params->block[1] + dy*params->adjust[1];
    block->dims[2] = params->block[2] + dz*params->adjust[2];

    if (params->verbosity >= H5PART_VERB_DEBUG) {
        printf ("[%d] block dimensions (%ld,%ld,%ld)\n",
                params->rank,
                block->dims[0],
                block->dims[1],
                block->dims[2]);
    }

    block->size = params->nComps *
                  block->dims[0] *
                  block->dims[1] *
                  block->dims[2];

    // fold components into fastest moving direction
    block->layout[0] = params->nComps * block->i * params->block[0];
    if (dx >= 0) block->layout[0] -= params->nComps * params->adjust[0];
    block->layout[1] = block->layout[0] + params->nComps * block->dims[0] - 1;

    block->layout[2] = block->j * params->block[1];
    if (dy >= 0) block->layout[2] -= params->adjust[1];
    block->layout[3] = block->layout[2] + block->dims[1] - 1;

    block->layout[4] = block->k * params->block[2];
    if (dz >= 0) block->layout[4] -= params->adjust[2];
    block->layout[5] = block->layout[4] + block->dims[2] - 1;

    if (params->verbosity >= H5PART_VERB_DEBUG) {
        printf ("[%d] block layout (%ld:%ld,%ld:%ld,%ld:%ld)\n",
                params->rank,
                block->layout[0],
                block->layout[1],
                block->layout[2],
                block->layout[3],
                block->layout[4],
                block->layout[5]);
    }
}

/******************************************************************************/
/* Main procedure                                                             */
/******************************************************************************/

int main (int argc, char** argv)
{
    Params params;
    Block block;

    // initialize MPI
    MPI_Init (&argc, &argv);
    MPI_Comm_rank (MPI_COMM_WORLD, &params.rank);
    MPI_Comm_size (MPI_COMM_WORLD, &params.nprocs);

    if (params.rank == 0) {
        printf ("H5_VERS_INFO: %s\n", H5_VERS_INFO);
    }

    ParseArgs(argc, argv, &params);

    H5PartSetVerbosityLevel (params.verbosity);

    GetBlockIndices(params.rank, params.decomp, &block);
    GetBlockLayout(&params, &block);

    if (params.dryrun) {
        if (params.rank == 0) {
            printf ("Dry run... exiting without write!\n");
        }
    } else {
        int i;
        size_t datasize;
        h5part_float64_t *data;

        if (params.rank == 0) {
            printf ("Generating random data...\n");
        }

        datasize = params.nSteps * block.size;
        data = (h5part_float64_t*) malloc(datasize*sizeof(h5part_float64_t));
        if (!data) {
            ERROR("Could not malloc data", params.rank);
        }

        for (i=0; i<datasize; i++) {
            data[i] = (h5part_float64_t) random();
        }

        for (i=0; i<params.nIters; i++)
        {
            if (params.rank == 0) printf ("Write test %d...\n", i);
            WriteBlock (&params, &block, data);
        }
    }
    
    MPI_Finalize();
    return (EXIT_SUCCESS);
}

