#include "h5dsm_example.h"
#include "timer.h"
struct timeval start_time[6];
float elapse[6];

int main(int argc, char *argv[]) {
    uuid_t pool_uuid;
    char *pool_grp = NULL;
    hid_t file = -1, fapl = -1;
    int nfiles = 1, i;
    char fname[128];
    char msg[128];

    (void)MPI_Init(&argc, &argv);

    if(argc != 3)
        PRINTF_ERROR("argc != 3\n");

    /* Parse UUID */
    if(0 != uuid_parse(argv[1], pool_uuid))
        ERROR;

    nfiles = atoi(argv[2]);

    /* Initialize VOL */
    if(H5VLdaosm_init(MPI_COMM_WORLD, pool_uuid, pool_grp) < 0)
        ERROR;

    /* Set up FAPL */
    if((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        ERROR;
    if(H5Pset_fapl_daosm(fapl, MPI_COMM_WORLD, MPI_INFO_NULL) < 0)
        ERROR;
    if(H5Pset_all_coll_metadata_ops(fapl, true) < 0)
        ERROR;

    printf("Create %d files\n", nfiles);

    timer_on(0);
    timer_on(1);
    timer_on(2);
    timer_on(3);
    timer_on(4);

    /* Create file */
    for (i = 0; i < nfiles; i++) {
	sprintf(fname, "Files%d%d", nfiles, i);
	    if((file = H5Fcreate(fname, H5F_ACC_TRUNC, H5P_DEFAULT, fapl)) < 0)
		ERROR;

	/* Close */
	if(H5Fclose(file) < 0)
	    ERROR;

	if (i == 99) {
	    timer_off(0);
	    timer_msg (0, "100 file creation");
	}
	if (i == 999) {
	    timer_off(1);
	    timer_msg (1, "1000 file creation");
	}
	if (i == 9999) {
	    timer_off(2);
	    timer_msg (2, "10000 file creation");
	}
	if (i == 99999) {
	    timer_off(3);
	    timer_msg (3, "100000 file creation");
	}
	if (i == 999999) {
	    timer_off(4);
	    timer_msg (4, "1000000 file creation");
	}
    }

    if(H5Pclose(fapl) < 0)
        ERROR;

    printf("Success\n");

    (void)MPI_Finalize();
    return 0;

error:
    H5E_BEGIN_TRY {
        H5Fclose(file);
        H5Pclose(fapl);
    } H5E_END_TRY;

    (void)MPI_Finalize();
    return 1;
}

