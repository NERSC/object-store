#include<assert.h>
#include"compound_copy.h"
#include"parse_node.h"
#include<stdio.h>
#include<hdf5.h>
#include<stdlib.h>
#include<string.h>
#include<hdf5_hl.h>//this is needed for using the hdf5 table api
size_t COADD_REC_SIZE=sizeof(struct COADD);
size_t EXPOSURE_REC_SIZE=sizeof(struct EXPOSURE);
size_t CATALOG_REC_SIZE[]={sizeof(struct PLUGMAP),sizeof(struct ZBEST), sizeof(struct ZLINE),
                          sizeof(struct MATCH), sizeof(struct MATCHFLUX), sizeof(struct MATCHPOS)};
#define NFIELDS   (hsize_t) 8
#define NFIELDS_PLUGMAP (hsize_t) 35
#define NFIELDS_ZBEST (hsize_t) 73
#define NFIELDS_ZLINE (hsize_t) 19
#define NFIELDS_MATCH (hsize_t) 24
#define NFIELDS_MATCHFLUX (hsize_t) 140
#define NFIELDS_MATCHPOS  (hsize_t) 140
#define TABLE_COADD_NAME "coadd"
#define TABLE_EXPOSURE_B_NAME "b"
#define TABLE_EXPOSURE_R_NAME "r"
const char * FIBERID="FIBERID";
const char * catalog_array[] = {
  "plugmap",
  "zbest",
  "zline",
  "match",
  "matchflux",
  "matchpos"
};
#define n_catalog (sizeof(catalog_array)/sizeof(const char *))
const char photo[6]="/photo";
size_t coadd_sizes[NFIELDS] = {
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(int),
  sizeof(float),
  sizeof(float),
  sizeof(float)
};
size_t coadd_offsets[NFIELDS] = {
	HOFFSET( COADD, WAVE ),
	HOFFSET( COADD, FLUX ),
	HOFFSET( COADD, IVAR ),
	HOFFSET( COADD, AND_MASK ),
	HOFFSET( COADD, OR_MASK ),
	HOFFSET( COADD, WAVEDISP ),
	HOFFSET( COADD, SKY ),
	HOFFSET( COADD, MODEL ),
};
size_t exposure_offsets[NFIELDS] = {
  HOFFSET( EXPOSURE, WAVE ),
  HOFFSET( EXPOSURE, FLUX ),
  HOFFSET( EXPOSURE, IVAR ),
  HOFFSET( EXPOSURE, MASK ),
  HOFFSET( EXPOSURE, WAVEDISP ),
  HOFFSET( EXPOSURE, SKY ),
  HOFFSET( EXPOSURE, X),
  HOFFSET( EXPOSURE, CALIB ),
};
size_t exposure_sizes[NFIELDS] ={
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float)
};
size_t plugmap_sizes[NFIELDS_PLUGMAP] = {
  sizeof(int)*5,
  sizeof(char)*6,
  sizeof(double),
  sizeof(double),
  sizeof(float)*5,
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(char)*16,
  sizeof(double),
  sizeof(double),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(float),
  sizeof(char)*15,
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(int64_t),
  sizeof(int64_t),
  sizeof(int64_t),
  sizeof(int64_t),
  sizeof(int),
  sizeof(char)*4,
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(int)*5,
  sizeof(float)
};
size_t plugmap_offsets[NFIELDS_PLUGMAP] = {
  HOFFSET(PLUGMAP,OBJID),
  HOFFSET(PLUGMAP,HOLETYPE),
  HOFFSET(PLUGMAP,RA),
  HOFFSET(PLUGMAP,DEC),
  HOFFSET(PLUGMAP,MAG),
  HOFFSET(PLUGMAP,STARL),
  HOFFSET(PLUGMAP,EXPL),
  HOFFSET(PLUGMAP,DEVAUCL),
  HOFFSET(PLUGMAP,OBJTYPE),
  HOFFSET(PLUGMAP,XFOCAL),
  HOFFSET(PLUGMAP,YFOCAL),
  HOFFSET(PLUGMAP,SPECTROGRAPHID),
  HOFFSET(PLUGMAP,FIBERID),
  HOFFSET(PLUGMAP,THROUGHPUT),
  HOFFSET(PLUGMAP,PRIMTARGET),
  HOFFSET(PLUGMAP,SECTARGET),
  HOFFSET(PLUGMAP,OFFSETID),
  HOFFSET(PLUGMAP,SCI_EXPTIME),
  HOFFSET(PLUGMAP,SOURCETYPE),
  HOFFSET(PLUGMAP,LAMBDA_EFF),
  HOFFSET(PLUGMAP,ZOFFSET),
  HOFFSET(PLUGMAP,BLUEFIBER),
  HOFFSET(PLUGMAP,BOSS_TARGET1),
  HOFFSET(PLUGMAP,BOSS_TARGET2),
  HOFFSET(PLUGMAP,ANCILLARY_TARGET1),
  HOFFSET(PLUGMAP,ANCILLARY_TARGET2),
  HOFFSET(PLUGMAP,RUN),
  HOFFSET(PLUGMAP,RERUN),
  HOFFSET(PLUGMAP,CAMCOL),
  HOFFSET(PLUGMAP,FIELD),
  HOFFSET(PLUGMAP,ID),
  HOFFSET(PLUGMAP,CALIBFLUX),
  HOFFSET(PLUGMAP,CALIBFLUX_IVAR),
  HOFFSET(PLUGMAP,CALIB_STATUS),
  HOFFSET(PLUGMAP,SFD_EBV)
};
size_t zbest_sizes[NFIELDS_ZBEST] = {
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(char)*6,
  sizeof(char)*6,
  sizeof(int)*5,
  sizeof(char)*16,
  sizeof(double),
  sizeof(double),
  sizeof(char)*6,
  sizeof(char)*21,
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(float),
  sizeof(char)*24,
  sizeof(int)*10,
  sizeof(int),
  sizeof(float)*10,
  sizeof(float)*100,
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(float)*5,
  sizeof(float),
  sizeof(float),
  sizeof(float)*10,
  sizeof(float)*10,
  sizeof(float)*10,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(int),
  sizeof(int),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(char)*16,
  sizeof(char)*8,
  sizeof(char)*12,
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(int),
  sizeof(char)*6,
  sizeof(char)*19,
  sizeof(float),
  sizeof(float)*35,
  sizeof(int64_t)
};
size_t zbest_offsets[NFIELDS_ZBEST] = {
  HOFFSET ( ZBEST,PLATE ),
  HOFFSET ( ZBEST,TILE ),
  HOFFSET ( ZBEST,MJD ),
  HOFFSET ( ZBEST,FIBERID ),
  HOFFSET ( ZBEST,RUN2D ),
  HOFFSET ( ZBEST,RUN1D ),
  HOFFSET ( ZBEST,OBJID ),
  HOFFSET ( ZBEST,OBJTYPE ),
  HOFFSET ( ZBEST,PLUG_RA ),
  HOFFSET ( ZBEST,PLUG_DEC ),
  HOFFSET ( ZBEST,CLASS ),
  HOFFSET ( ZBEST,SUBCLASS ),
  HOFFSET ( ZBEST,Z ),
  HOFFSET ( ZBEST,Z_ERR ),
  HOFFSET ( ZBEST,RCHI2 ),
  HOFFSET ( ZBEST,DOF ),
  HOFFSET ( ZBEST,RCHI2DIFF ),
  HOFFSET ( ZBEST,TFILE ),
  HOFFSET ( ZBEST,TCOLUMN ),
  HOFFSET ( ZBEST,NPOLY ),
  HOFFSET ( ZBEST,THETA ),
  HOFFSET ( ZBEST,THETA_COVAR ),
  HOFFSET ( ZBEST,VDISP ),
  HOFFSET ( ZBEST,VDISP_ERR ),
  HOFFSET ( ZBEST,VDISPZ ),
  HOFFSET ( ZBEST,VDISPZ_ERR ),
  HOFFSET ( ZBEST,VDISPCHI2 ),
  HOFFSET ( ZBEST,VDISPNPIX ),
  HOFFSET ( ZBEST,VDISPDOF ),
  HOFFSET ( ZBEST,WAVEMIN ),
  HOFFSET ( ZBEST,WAVEMAX ),
  HOFFSET ( ZBEST,WCOVERAGE ),
  HOFFSET ( ZBEST,ZWARNING ),
  HOFFSET ( ZBEST,SN_MEDIAN ),
  HOFFSET ( ZBEST,SN_MEDIAN_ALL ),
  HOFFSET ( ZBEST,CHI68P ),
  HOFFSET ( ZBEST,FRACNSIGMA ),
  HOFFSET ( ZBEST,FRACNSIGHI ),
  HOFFSET ( ZBEST,FRACNSIGLO ),
  HOFFSET ( ZBEST,SPECTROFLUX ),
  HOFFSET ( ZBEST,SPECTROFLUX_IVAR ),
  HOFFSET ( ZBEST,SPECTROSYNFLUX ),
  HOFFSET ( ZBEST,SPECTROSYNFLUX_IVAR ),
  HOFFSET ( ZBEST,SPECTROSKYFLUX ),
  HOFFSET ( ZBEST,ANYANDMASK ),
  HOFFSET ( ZBEST,ANYORMASK ),
  HOFFSET ( ZBEST,SPEC1_G ),
  HOFFSET ( ZBEST,SPEC1_R ),
  HOFFSET ( ZBEST,SPEC1_I ),
  HOFFSET ( ZBEST,SPEC2_G ),
  HOFFSET ( ZBEST,SPEC2_R ),
  HOFFSET ( ZBEST,SPEC2_I ),
  HOFFSET ( ZBEST,ELODIE_FILENAME ),
  HOFFSET ( ZBEST,ELODIE_OBJECT ),
  HOFFSET ( ZBEST,ELODIE_SPTYPE ),
  HOFFSET ( ZBEST,ELODIE_BV ),
  HOFFSET ( ZBEST,ELODIE_TEFF ),
  HOFFSET ( ZBEST,ELODIE_LOGG ),
  HOFFSET ( ZBEST,ELODIE_FEH ),
  HOFFSET ( ZBEST,ELODIE_Z ),
  HOFFSET ( ZBEST,ELODIE_Z_ERR ),
  HOFFSET ( ZBEST,ELODIE_Z_MODELERR ),
  HOFFSET ( ZBEST,ELODIE_RCHI2 ),
  HOFFSET ( ZBEST,ELODIE_DOF ),
  HOFFSET ( ZBEST,Z_NOQSO ),
  HOFFSET ( ZBEST,Z_ERR_NOQSO ),
  HOFFSET ( ZBEST,ZNUM_NOQSO ),
  HOFFSET ( ZBEST,ZWARNING_NOQSO ),
  HOFFSET ( ZBEST,CLASS_NOQSO ),
  HOFFSET ( ZBEST,SUBCLASS_NOQSO ),
  HOFFSET ( ZBEST,RCHI2DIFF_NOQSO ),
  HOFFSET ( ZBEST,VDISP_LNL ),
  HOFFSET ( ZBEST,SPECOBJID )
};
size_t zline_sizes[NFIELDS_ZLINE] = {
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(char)*13,
  sizeof(double),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(int),
  sizeof(float),
  sizeof(float)
};
size_t zline_offsets[NFIELDS_ZLINE] ={
  HOFFSET ( ZLINE,PLATE ),
  HOFFSET ( ZLINE,MJD ),
  HOFFSET ( ZLINE,FIBERID ),
  HOFFSET ( ZLINE,LINENAME ),
  HOFFSET ( ZLINE,LINEWAVE ),
  HOFFSET ( ZLINE,LINEZ ),
  HOFFSET ( ZLINE,LINEZ_ERR ),
  HOFFSET ( ZLINE,LINESIGMA ),
  HOFFSET ( ZLINE,LINESIGMA_ERR ),
  HOFFSET ( ZLINE,LINEAREA ),
  HOFFSET ( ZLINE,LINEAREA_ERR ),
  HOFFSET ( ZLINE,LINEEW ),
  HOFFSET ( ZLINE,LINEEW_ERR ),
  HOFFSET ( ZLINE,LINECONTLEVEL ),
  HOFFSET ( ZLINE,LINECONTLEVEL_ERR ),
  HOFFSET ( ZLINE,LINENPIXLEFT ),
  HOFFSET ( ZLINE,LINENPIXRIGHT ),
  HOFFSET ( ZLINE,LINEDOF ),
  HOFFSET ( ZLINE,LINECHI2 )
};
size_t match_sizes[NFIELDS_MATCH] ={
  sizeof(char)*19,
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(char)*3,
  sizeof(int),
  sizeof(int),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(char)*19,
  sizeof(int),
  sizeof(double),
  sizeof(double),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(int),
  sizeof(int),
  sizeof(int16_t)
};
size_t match_offsets[NFIELDS_MATCH] = {
  HOFFSET ( MATCH,FLUX_OBJID ),
  HOFFSET ( MATCH,RUN ),
  HOFFSET ( MATCH,CAMCOL ),
  HOFFSET ( MATCH,FIELD ),
  HOFFSET ( MATCH,FLUX_ID ),
  HOFFSET ( MATCH,THING_ID ),
  HOFFSET ( MATCH,RERUN ),
  HOFFSET ( MATCH,NOBSERVE ),
  HOFFSET ( MATCH,NDETECT ),
  HOFFSET ( MATCH,FLUX_RA ),
  HOFFSET ( MATCH,FLUX_DEC ),
  HOFFSET ( MATCH,MATCH_RA ),
  HOFFSET ( MATCH,MATCH_DEC ),
  HOFFSET ( MATCH,ORIG_OBJID ),
  HOFFSET ( MATCH,ORIG_ID ),
  HOFFSET ( MATCH,ORIG_RA ),
  HOFFSET ( MATCH,ORIG_DEC ),
  HOFFSET ( MATCH,APERFLUX3_R ),
  HOFFSET ( MATCH,FIBERFLUX_R ),
  HOFFSET ( MATCH,APERFLUX3_R_TOTAL ),
  HOFFSET ( MATCH,APERFLUX3_R_MATCH ),
  HOFFSET ( MATCH,FLUXMATCH_PARENT ),
  HOFFSET ( MATCH,FLUXMATCH_STATUS ),
  HOFFSET ( MATCH,FIBERID )
};
size_t matchflux_sizes[NFIELDS_MATCHFLUX]={
  sizeof(char)*19,
  sizeof(char)*19,
  sizeof(char)*19,
  sizeof(uint8_t),
  sizeof(uint8_t),
  sizeof(uint8_t),
  sizeof(int16_t),
  sizeof(char)*3,
  sizeof(uint8_t),
  sizeof(int16_t),
  sizeof(int16_t),
  sizeof(int16_t),
  sizeof(int16_t),
  sizeof(int),
  sizeof(float),
  sizeof(int),
  sizeof(int),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(int)*5,
  sizeof(int)*5,
  sizeof(int)*5,
  sizeof(float)*5,
  sizeof(int)*5,
  sizeof(float)*75,
  sizeof(float)*75,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(int)*5,
  sizeof(float)*5,
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(int),
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*40,
  sizeof(float)*40,
  sizeof(int)*5,
  sizeof(int)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(double)*5,
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(float),
  sizeof(int16_t)
};
size_t matchflux_offsets[NFIELDS_MATCHFLUX]={
  HOFFSET ( MATCHFLUX,OBJID ),
  HOFFSET ( MATCHFLUX,PARENTID ),
  HOFFSET ( MATCHFLUX,FIELDID ),
  HOFFSET ( MATCHFLUX,SKYVERSION ),
  HOFFSET ( MATCHFLUX,MODE ),
  HOFFSET ( MATCHFLUX,CLEAN ),
  HOFFSET ( MATCHFLUX,RUN ),
  HOFFSET ( MATCHFLUX,RERUN ),
  HOFFSET ( MATCHFLUX,CAMCOL ),
  HOFFSET ( MATCHFLUX,FIELD ),
  HOFFSET ( MATCHFLUX,ID ),
  HOFFSET ( MATCHFLUX,PARENT ),
  HOFFSET ( MATCHFLUX,NCHILD ),
  HOFFSET ( MATCHFLUX,OBJC_TYPE ),
  HOFFSET ( MATCHFLUX,OBJC_PROB_PSF ),
  HOFFSET ( MATCHFLUX,OBJC_FLAGS ),
  HOFFSET ( MATCHFLUX,OBJC_FLAGS2 ),
  HOFFSET ( MATCHFLUX,OBJC_ROWC ),
  HOFFSET ( MATCHFLUX,OBJC_ROWCERR ),
  HOFFSET ( MATCHFLUX,OBJC_COLC ),
  HOFFSET ( MATCHFLUX,OBJC_COLCERR ),
  HOFFSET ( MATCHFLUX,ROWVDEG ),
  HOFFSET ( MATCHFLUX,ROWVDEGERR ),
  HOFFSET ( MATCHFLUX,COLVDEG ),
  HOFFSET ( MATCHFLUX,COLVDEGERR ),
  HOFFSET ( MATCHFLUX,ROWC ),
  HOFFSET ( MATCHFLUX,ROWCERR ),
  HOFFSET ( MATCHFLUX,COLC ),
  HOFFSET ( MATCHFLUX,COLCERR ),
  HOFFSET ( MATCHFLUX,PETROTHETA ),
  HOFFSET ( MATCHFLUX,PETROTHETAERR ),
  HOFFSET ( MATCHFLUX,PETROTH50 ),
  HOFFSET ( MATCHFLUX,PETROTH50ERR ),
  HOFFSET ( MATCHFLUX,PETROTH90 ),
  HOFFSET ( MATCHFLUX,PETROTH90ERR ),
  HOFFSET ( MATCHFLUX,Q ),
  HOFFSET ( MATCHFLUX,QERR ),
  HOFFSET ( MATCHFLUX,U ),
  HOFFSET ( MATCHFLUX,UERR ),
  HOFFSET ( MATCHFLUX,M_E1 ),
  HOFFSET ( MATCHFLUX,M_E2 ),
  HOFFSET ( MATCHFLUX,M_E1E1ERR ),
  HOFFSET ( MATCHFLUX,M_E1E2ERR ),
  HOFFSET ( MATCHFLUX,M_E2E2ERR ),
  HOFFSET ( MATCHFLUX,M_RR_CC ),
  HOFFSET ( MATCHFLUX,M_RR_CCERR ),
  HOFFSET ( MATCHFLUX,M_CR4 ),
  HOFFSET ( MATCHFLUX,M_E1_PSF ),
  HOFFSET ( MATCHFLUX,M_E2_PSF ),
  HOFFSET ( MATCHFLUX,M_RR_CC_PSF ),
  HOFFSET ( MATCHFLUX,M_CR4_PSF ),
  HOFFSET ( MATCHFLUX,THETA_DEV ),
  HOFFSET ( MATCHFLUX,THETA_DEVERR ),
  HOFFSET ( MATCHFLUX,AB_DEV ),
  HOFFSET ( MATCHFLUX,AB_DEVERR ),
  HOFFSET ( MATCHFLUX,THETA_EXP ),
  HOFFSET ( MATCHFLUX,THETA_EXPERR ),
  HOFFSET ( MATCHFLUX,AB_EXP ),
  HOFFSET ( MATCHFLUX,AB_EXPERR ),
  HOFFSET ( MATCHFLUX,FRACDEV ),
  HOFFSET ( MATCHFLUX,FLAGS ),
  HOFFSET ( MATCHFLUX,FLAGS2 ),
  HOFFSET ( MATCHFLUX,TYPE ),
  HOFFSET ( MATCHFLUX,PROB_PSF ),
  HOFFSET ( MATCHFLUX,NPROF ),
  HOFFSET ( MATCHFLUX,PROFMEAN_NMGY ),
  HOFFSET ( MATCHFLUX,PROFERR_NMGY ),
  HOFFSET ( MATCHFLUX,STAR_LNL ),
  HOFFSET ( MATCHFLUX,EXP_LNL ),
  HOFFSET ( MATCHFLUX,DEV_LNL ),
  HOFFSET ( MATCHFLUX,PSP_STATUS ),
  HOFFSET ( MATCHFLUX,PIXSCALE ),
  HOFFSET ( MATCHFLUX,RA ),
  HOFFSET ( MATCHFLUX,DEC ),
  HOFFSET ( MATCHFLUX,CX ),
  HOFFSET ( MATCHFLUX,CY ),
  HOFFSET ( MATCHFLUX,CZ ),
  HOFFSET ( MATCHFLUX,RAERR ),
  HOFFSET ( MATCHFLUX,DECERR ),
  HOFFSET ( MATCHFLUX,L ),
  HOFFSET ( MATCHFLUX,B ),
  HOFFSET ( MATCHFLUX,OFFSETRA ),
  HOFFSET ( MATCHFLUX,OFFSETDEC ),
  HOFFSET ( MATCHFLUX,PSF_FWHM ),
  HOFFSET ( MATCHFLUX,MJD ),
  HOFFSET ( MATCHFLUX,AIRMASS ),
  HOFFSET ( MATCHFLUX,PHI_OFFSET ),
  HOFFSET ( MATCHFLUX,PHI_DEV_DEG ),
  HOFFSET ( MATCHFLUX,PHI_EXP_DEG ),
  HOFFSET ( MATCHFLUX,EXTINCTION ),
  HOFFSET ( MATCHFLUX,SKYFLUX ),
  HOFFSET ( MATCHFLUX,SKYFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,PSFFLUX ),
  HOFFSET ( MATCHFLUX,PSFFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,PSFMAG ),
  HOFFSET ( MATCHFLUX,PSFMAGERR ),
  HOFFSET ( MATCHFLUX,FIBERFLUX ),
  HOFFSET ( MATCHFLUX,FIBERFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,FIBERMAG ),
  HOFFSET ( MATCHFLUX,FIBERMAGERR ),
  HOFFSET ( MATCHFLUX,FIBER2FLUX ),
  HOFFSET ( MATCHFLUX,FIBER2FLUX_IVAR ),
  HOFFSET ( MATCHFLUX,FIBER2MAG ),
  HOFFSET ( MATCHFLUX,FIBER2MAGERR ),
  HOFFSET ( MATCHFLUX,CMODELFLUX ),
  HOFFSET ( MATCHFLUX,CMODELFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,CMODELMAG ),
  HOFFSET ( MATCHFLUX,CMODELMAGERR ),
  HOFFSET ( MATCHFLUX,MODELFLUX ),
  HOFFSET ( MATCHFLUX,MODELFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,MODELMAG ),
  HOFFSET ( MATCHFLUX,MODELMAGERR ),
  HOFFSET ( MATCHFLUX,PETROFLUX ),
  HOFFSET ( MATCHFLUX,PETROFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,PETROMAG ),
  HOFFSET ( MATCHFLUX,PETROMAGERR ),
  HOFFSET ( MATCHFLUX,DEVFLUX ),
  HOFFSET ( MATCHFLUX,DEVFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,DEVMAG ),
  HOFFSET ( MATCHFLUX,DEVMAGERR ),
  HOFFSET ( MATCHFLUX,EXPFLUX ),
  HOFFSET ( MATCHFLUX,EXPFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,EXPMAG ),
  HOFFSET ( MATCHFLUX,EXPMAGERR ),
  HOFFSET ( MATCHFLUX,APERFLUX ),
  HOFFSET ( MATCHFLUX,APERFLUX_IVAR ),
  HOFFSET ( MATCHFLUX,CLOUDCAM ),
  HOFFSET ( MATCHFLUX,CALIB_STATUS ),
  HOFFSET ( MATCHFLUX,NMGYPERCOUNT ),
  HOFFSET ( MATCHFLUX,NMGYPERCOUNT_IVAR ),
  HOFFSET ( MATCHFLUX,TAI ),
  HOFFSET ( MATCHFLUX,RESOLVE_STATUS ),
  HOFFSET ( MATCHFLUX,THING_ID ),
  HOFFSET ( MATCHFLUX,IFIELD ),
  HOFFSET ( MATCHFLUX,BALKAN_ID ),
  HOFFSET ( MATCHFLUX,NOBSERVE ),
  HOFFSET ( MATCHFLUX,NDETECT ),
  HOFFSET ( MATCHFLUX,NEDGE ),
  HOFFSET ( MATCHFLUX,SCORE ),
  HOFFSET ( MATCHFLUX,FIBERID )
};
size_t matchpos_sizes[NFIELDS_MATCHPOS] ={
  sizeof(char)*19,
  sizeof(char)*19,
  sizeof(char)*19,
  sizeof(uint8_t),
  sizeof(uint8_t),
  sizeof(uint8_t),
  sizeof(int16_t),
  sizeof(char)*3,
  sizeof(uint8_t),
  sizeof(int16_t),
  sizeof(int16_t),
  sizeof(int16_t),
  sizeof(int16_t),
  sizeof(int),
  sizeof(float),
  sizeof(int),
  sizeof(int),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float),
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(int)*5,
  sizeof(int)*5,
  sizeof(int)*5,
  sizeof(float)*5,
  sizeof(int)*5,
  sizeof(float)*75,
  sizeof(float)*75,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(int)*5,
  sizeof(float)*5,
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(double),
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(int),
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(float)*40,
  sizeof(float)*40,
  sizeof(int)*5,
  sizeof(int)*5,
  sizeof(float)*5,
  sizeof(float)*5,
  sizeof(double)*5,
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(int),
  sizeof(float),
  sizeof(int16_t)
};
size_t matchpos_offsets[NFIELDS_MATCHPOS] ={
  HOFFSET ( MATCHPOS,OBJID ),
  HOFFSET ( MATCHPOS,PARENTID ),
  HOFFSET ( MATCHPOS,FIELDID ),
  HOFFSET ( MATCHPOS,SKYVERSION ),
  HOFFSET ( MATCHPOS,MODE ),
  HOFFSET ( MATCHPOS,CLEAN ),
  HOFFSET ( MATCHPOS,RUN ),
  HOFFSET ( MATCHPOS,RERUN ),
  HOFFSET ( MATCHPOS,CAMCOL ),
  HOFFSET ( MATCHPOS,FIELD ),
  HOFFSET ( MATCHPOS,ID ),
  HOFFSET ( MATCHPOS,PARENT ),
  HOFFSET ( MATCHPOS,NCHILD ),
  HOFFSET ( MATCHPOS,OBJC_TYPE ),
  HOFFSET ( MATCHPOS,OBJC_PROB_PSF ),
  HOFFSET ( MATCHPOS,OBJC_FLAGS ),
  HOFFSET ( MATCHPOS,OBJC_FLAGS2 ),
  HOFFSET ( MATCHPOS,OBJC_ROWC ),
  HOFFSET ( MATCHPOS,OBJC_ROWCERR ),
  HOFFSET ( MATCHPOS,OBJC_COLC ),
  HOFFSET ( MATCHPOS,OBJC_COLCERR ),
  HOFFSET ( MATCHPOS,ROWVDEG ),
  HOFFSET ( MATCHPOS,ROWVDEGERR ),
  HOFFSET ( MATCHPOS,COLVDEG ),
  HOFFSET ( MATCHPOS,COLVDEGERR ),
  HOFFSET ( MATCHPOS,ROWC ),
  HOFFSET ( MATCHPOS,ROWCERR ),
  HOFFSET ( MATCHPOS,COLC ),
  HOFFSET ( MATCHPOS,COLCERR ),
  HOFFSET ( MATCHPOS,PETROTHETA ),
  HOFFSET ( MATCHPOS,PETROTHETAERR ),
  HOFFSET ( MATCHPOS,PETROTH50 ),
  HOFFSET ( MATCHPOS,PETROTH50ERR ),
  HOFFSET ( MATCHPOS,PETROTH90 ),
  HOFFSET ( MATCHPOS,PETROTH90ERR ),
  HOFFSET ( MATCHPOS,Q ),
  HOFFSET ( MATCHPOS,QERR ),
  HOFFSET ( MATCHPOS,U ),
  HOFFSET ( MATCHPOS,UERR ),
  HOFFSET ( MATCHPOS,M_E1 ),
  HOFFSET ( MATCHPOS,M_E2 ),
  HOFFSET ( MATCHPOS,M_E1E1ERR ),
  HOFFSET ( MATCHPOS,M_E1E2ERR ),
  HOFFSET ( MATCHPOS,M_E2E2ERR ),
  HOFFSET ( MATCHPOS,M_RR_CC ),
  HOFFSET ( MATCHPOS,M_RR_CCERR ),
  HOFFSET ( MATCHPOS,M_CR4 ),
  HOFFSET ( MATCHPOS,M_E1_PSF ),
  HOFFSET ( MATCHPOS,M_E2_PSF ),
  HOFFSET ( MATCHPOS,M_RR_CC_PSF ),
  HOFFSET ( MATCHPOS,M_CR4_PSF ),
  HOFFSET ( MATCHPOS,THETA_DEV ),
  HOFFSET ( MATCHPOS,THETA_DEVERR ),
  HOFFSET ( MATCHPOS,AB_DEV ),
  HOFFSET ( MATCHPOS,AB_DEVERR ),
  HOFFSET ( MATCHPOS,THETA_EXP ),
  HOFFSET ( MATCHPOS,THETA_EXPERR ),
  HOFFSET ( MATCHPOS,AB_EXP ),
  HOFFSET ( MATCHPOS,AB_EXPERR ),
  HOFFSET ( MATCHPOS,FRACDEV ),
  HOFFSET ( MATCHPOS,FLAGS ),
  HOFFSET ( MATCHPOS,FLAGS2 ),
  HOFFSET ( MATCHPOS,TYPE ),
  HOFFSET ( MATCHPOS,PROB_PSF ),
  HOFFSET ( MATCHPOS,NPROF ),
  HOFFSET ( MATCHPOS,PROFMEAN_NMGY ),
  HOFFSET ( MATCHPOS,PROFERR_NMGY ),
  HOFFSET ( MATCHPOS,STAR_LNL ),
  HOFFSET ( MATCHPOS,EXP_LNL ),
  HOFFSET ( MATCHPOS,DEV_LNL ),
  HOFFSET ( MATCHPOS,PSP_STATUS ),
  HOFFSET ( MATCHPOS,PIXSCALE ),
  HOFFSET ( MATCHPOS,RA ),
  HOFFSET ( MATCHPOS,DEC ),
  HOFFSET ( MATCHPOS,CX ),
  HOFFSET ( MATCHPOS,CY ),
  HOFFSET ( MATCHPOS,CZ ),
  HOFFSET ( MATCHPOS,RAERR ),
  HOFFSET ( MATCHPOS,DECERR ),
  HOFFSET ( MATCHPOS,L ),
  HOFFSET ( MATCHPOS,B ),
  HOFFSET ( MATCHPOS,OFFSETRA ),
  HOFFSET ( MATCHPOS,OFFSETDEC ),
  HOFFSET ( MATCHPOS,PSF_FWHM ),
  HOFFSET ( MATCHPOS,MJD ),
  HOFFSET ( MATCHPOS,AIRMASS ),
  HOFFSET ( MATCHPOS,PHI_OFFSET ),
  HOFFSET ( MATCHPOS,PHI_DEV_DEG ),
  HOFFSET ( MATCHPOS,PHI_EXP_DEG ),
  HOFFSET ( MATCHPOS,EXTINCTION ),
  HOFFSET ( MATCHPOS,SKYFLUX ),
  HOFFSET ( MATCHPOS,SKYFLUX_IVAR ),
  HOFFSET ( MATCHPOS,PSFFLUX ),
  HOFFSET ( MATCHPOS,PSFFLUX_IVAR ),
  HOFFSET ( MATCHPOS,PSFMAG ),
  HOFFSET ( MATCHPOS,PSFMAGERR ),
  HOFFSET ( MATCHPOS,FIBERFLUX ),
  HOFFSET ( MATCHPOS,FIBERFLUX_IVAR ),
  HOFFSET ( MATCHPOS,FIBERMAG ),
  HOFFSET ( MATCHPOS,FIBERMAGERR ),
  HOFFSET ( MATCHPOS,FIBER2FLUX ),
  HOFFSET ( MATCHPOS,FIBER2FLUX_IVAR ),
  HOFFSET ( MATCHPOS,FIBER2MAG ),
  HOFFSET ( MATCHPOS,FIBER2MAGERR ),
  HOFFSET ( MATCHPOS,CMODELFLUX ),
  HOFFSET ( MATCHPOS,CMODELFLUX_IVAR ),
  HOFFSET ( MATCHPOS,CMODELMAG ),
  HOFFSET ( MATCHPOS,CMODELMAGERR ),
  HOFFSET ( MATCHPOS,MODELFLUX ),
  HOFFSET ( MATCHPOS,MODELFLUX_IVAR ),
  HOFFSET ( MATCHPOS,MODELMAG ),
  HOFFSET ( MATCHPOS,MODELMAGERR ),
  HOFFSET ( MATCHPOS,PETROFLUX ),
  HOFFSET ( MATCHPOS,PETROFLUX_IVAR ),
  HOFFSET ( MATCHPOS,PETROMAG ),
  HOFFSET ( MATCHPOS,PETROMAGERR ),
  HOFFSET ( MATCHPOS,DEVFLUX ),
  HOFFSET ( MATCHPOS,DEVFLUX_IVAR ),
  HOFFSET ( MATCHPOS,DEVMAG ),
  HOFFSET ( MATCHPOS,DEVMAGERR ),
  HOFFSET ( MATCHPOS,EXPFLUX ),
  HOFFSET ( MATCHPOS,EXPFLUX_IVAR ),
  HOFFSET ( MATCHPOS,EXPMAG ),
  HOFFSET ( MATCHPOS,EXPMAGERR ),
  HOFFSET ( MATCHPOS,APERFLUX ),
  HOFFSET ( MATCHPOS,APERFLUX_IVAR ),
  HOFFSET ( MATCHPOS,CLOUDCAM ),
  HOFFSET ( MATCHPOS,CALIB_STATUS ),
  HOFFSET ( MATCHPOS,NMGYPERCOUNT ),
  HOFFSET ( MATCHPOS,NMGYPERCOUNT_IVAR ),
  HOFFSET ( MATCHPOS,TAI ),
  HOFFSET ( MATCHPOS,RESOLVE_STATUS ),
  HOFFSET ( MATCHPOS,THING_ID ),
  HOFFSET ( MATCHPOS,IFIELD ),
  HOFFSET ( MATCHPOS,BALKAN_ID ),
  HOFFSET ( MATCHPOS,NOBSERVE ),
  HOFFSET ( MATCHPOS,NDETECT ),
  HOFFSET ( MATCHPOS,NEDGE ),
  HOFFSET ( MATCHPOS,SCORE ),
  HOFFSET ( MATCHPOS,FIBERID )
};
size_t * catalog_field_sizes[]={
  plugmap_sizes,
  zbest_sizes,
  zline_sizes,
  match_sizes,
  matchflux_sizes,
  matchpos_sizes
};
size_t * catalog_field_offsets []={
  plugmap_offsets,
  zbest_offsets,
  zline_offsets,
  match_offsets,
  matchflux_offsets,
  matchpos_offsets
};
void print_record_cad(hsize_t nrecords,COADD * data){
  hsize_t i;
  for (i=0;i<nrecords;i++){
   printf ("%lld:%f %f %f %d %d %f %f %f\n",i,
    data[i].WAVE,
    data[i].FLUX,
    data[i].IVAR,
    data[i].AND_MASK,
    data[i].OR_MASK,
    data[i].WAVEDISP,
    data[i].SKY,
    data[i].MODEL
   );
  }
}
void print_record_exp(hsize_t nrecords,EXPOSURE * data){
  hsize_t i;
  for (i=0;i<nrecords;i++){
   printf ("%lld:%f %f %f %d %f %f %f %f\n", i,
    data[i].WAVE,
    data[i].FLUX,
    data[i].IVAR,
    data[i].MASK,
    data[i].WAVEDISP,
    data[i].SKY,
    data[i].X,
    data[i].CALIB
   );
  }
}
//void compound_read(char * src_file,char * dst_file, char * path_table, bool write){
void compound_read(char * src_file, hid_t dst_file, char * path_table, int write,int rank){
  hid_t ifile=-1;
  hid_t igroup=-1;
  hid_t fapl_id=-1;
  herr_t ierr=-1;
  hsize_t  nfields, nrecords;
  size_t * field_sizes;
  size_t * field_offsets;
  size_t record_size=0;
  //hbool_t is_collective=false;
  //fapl_id = H5Pcopy(H5P_FILE_ACCESS);
  //H5Pset_all_coll_metadata_ops(fapl_id,is_collective);
  //ifile= H5Fopen(src_file,H5F_ACC_RDONLY,fapl_id);
  ifile= H5Fopen(src_file,H5F_ACC_RDONLY,H5P_DEFAULT);
  //H5Pclose(fapl_id);
  //printf("ifile = %d / %0x\n", (int)ifile, (unsigned)ifile);
  if(ifile<0) printf("srcfile '%s' open error in rank %d\n",src_file,rank);
  //else printf("srcfile '%s' open correctly rank %d\n",src_file, rank);

  //split keys into group/dataset_table
  char **gt=path_split(path_table);
  igroup= H5Gopen(ifile,gt[0],H5P_DEFAULT);
  if(igroup<0) printf("group '%s' open error rank: %d\n",gt[0],rank);
  //ierr=H5TBget_field_info (igroup, gt[1], field_names,field_sizes, field_offsets, type_size);
  ierr=H5TBget_table_info (igroup, gt[1], &nfields, &nrecords);
  if(ierr<0) printf("ierr of get table info:%d in rank %d\n",ierr,rank);
  //allocate the buffer and read the table in memory
  char tag1[10];
  char tag2[10];
  char tag3[10];
  strcpy(tag1,"coadd");
  strcpy(tag2,"b");
  strcpy(tag3,"r");
  if(strcmp(gt[1],tag1)==0){
   record_size=COADD_REC_SIZE;
   field_sizes=coadd_sizes;
   field_offsets=coadd_offsets;
   COADD * data = malloc(nrecords * record_size);
   ierr=H5TBread_table(igroup, gt[1], record_size, field_offsets, field_sizes, data);
   if(ierr<0) printf("ierr of read table info:%d\n",ierr);
   //print_record_cad(nrecords,data);
   //printf("number of records:%ld\n",nrecords);
   if(write) {
    compound_write(dst_file,gt[0],gt[1], nrecords, record_size, field_offsets, field_sizes,data);
   }
   if(data!=NULL) free(data);
  }
  else if(strcmp(gt[1],tag3)==0||strcmp(tag2,gt[1])==0){
   record_size=EXPOSURE_REC_SIZE;
   field_sizes=exposure_sizes;
   field_offsets=exposure_offsets;
   EXPOSURE * data = malloc(nrecords * record_size);
   ierr=H5TBread_table(igroup, gt[1], record_size, field_offsets, field_sizes, data);
   //print_record_exp(nrecords,data);

   if(write) {
    compound_write(dst_file,gt[0],gt[1], nrecords, record_size,field_offsets, field_sizes,data);
   }
   if(data!=NULL) free(data);
  }
  H5Gclose(igroup);
  H5Fclose(ifile);
}

//void compound_write(char * dst_file,char * grp, const char * table_name, hsize_t nrecords,size_t type_size, const size_t * field_offsets, const size_t * field_sizes,void * data){
void compound_write(hid_t dst_file,char * grp, const char * table_name, hsize_t nrecords,size_t type_size, const size_t * field_offsets, const size_t * field_sizes,void * data){
  herr_t ierr=-1;
  hid_t igroup=-1;
  hid_t ifile=-1;
  //ifile= H5Fopen(dst_file,H5F_ACC_RDWR,H5P_DEFAULT);
  //if(ifile<0) printf("file open error:%s",dst_file);
  ifile=dst_file;
  igroup= H5Gopen(ifile,grp,H5P_DEFAULT);
  ierr=H5TBwrite_records ( igroup, table_name, 0, nrecords, type_size, field_offsets, field_sizes, data);
  if(ierr<0) printf("write records error\n");
  if(igroup>=0) H5Gclose(igroup);
  //if(ifile>=0)  H5Fclose(ifile);
}

void compound_read_catalog(struct Catalog * cl, hid_t dst_file, int it, int write,int rank){
  hid_t ifile=-1;
  hid_t igroup=-1;
  hid_t fapl_id=-1;
  herr_t ierr=-1;
  hsize_t  nfields, nrecords;
  size_t * field_sizes;
  size_t * field_offsets;
  size_t record_size=0;
  char photo_gp[20];
  strncpy(photo_gp,cl->plate_mjd[it],sizeof(cl->plate_mjd[it])-1);
  photo_gp[sizeof(cl->plate_mjd[it]-1)]='\0';
  strcat(photo_gp,photo);
  //open the source file by rank
  ifile= H5Fopen(cl->filepath[it],H5F_ACC_RDONLY,H5P_DEFAULT);
  if(ifile<0) printf("srcfile '%s' open error in rank %d\n",cl->filepath[it],rank);
  /* Groups           Table_Name  Index in catalog_array
     plate/mjd        plugmap     0
     plate/mjd        zbest       1
     plate/mjd        zline       2
     plate/mjd/photo  match       3
     plate/mjd/photo  matchflux   4
     plate/mjd/photo  matchpos    5
  */

  int icat;
  void * data_catalog=NULL; // buffer for storing the catalog records
  hsize_t * data_field=NULL;   // buffer for storing the catalog fiberid field

  //loop through the 6 catalog tables
  for (icat=0;icat<n_catalog;icat++){
    record_size=CATALOG_REC_SIZE[icat];
    field_offsets=catalog_field_offsets[icat];
    field_sizes=catalog_field_sizes[icat];
    if(icat<3){   //tables: plugmpa, zbest, zline
      igroup = H5Gopen(ifile,cl->plate_mjd[it],H5P_DEFAULT);
    }
    else{         //tables: match, matchflux,matchpos
      igroup = H5Gopen(ifile,photo_gp,H5P_DEFAULT);
    }
    if(igroup<0) printf("group '%s' or '%s' open error rank: %d\n",cl->plate_mjd[it], photo_gp,rank);

    herr_t ierr=H5TBget_table_info (igroup, catalog_array[icat], &nfields, &nrecords);
    if(ierr<0) printf("ierr of get table info:%d in rank %d\n",ierr,rank);
    data_field=malloc(sizeof(hsize_t)*nrecords);
    herr_t ifield_err=H5TBread_fields_name( igroup, catalog_array[icat], FIBERID, 0,
      nrecords, record_size,  field_offsets, field_sizes, data_field);
      //search the data_field to get the local offsets list
    int len_offset=0;
    hsize_t * offsetlist=malloc(sizeof(hsize_t )* nrecords);//nrecords will be larger or equal than len_offset;
    get_catalog_offset(offsetlist, &len_offset, cl->fiberid[it], nrecords, data_field);
    //compare len_offset with (cl->fiber_llength[it])
    assert (len_offset==(cl->fiber_llength[it]));
    int ifd;
    data_catalog=malloc(record_size*len_offset);
    for(ifd=0;ifd<len_offset;ifd++){      //read by start: offsetlist[ifd], length: 1
      H5TBread_records (igroup, catalog_array[icat], offsetlist[ifd], 1, record_size,
        field_offsets, field_sizes, &data_catalog[ifd]);
    }
    if(write){
      compound_write_catalog(dst_file,cl->plate_mjd[it],catalog_array[icat],
          len_offset,cl->fiber_gstart[it],record_size, field_offsets, field_sizes,data_catalog);
    }
  }

  if(data_catalog!=NULL) free(data_catalog);
  if(data_field!=NULL) free(data_field);
  H5Gclose(igroup);
  H5Fclose(ifile);
}

//void compound_write(char * dst_file,char * grp, const char * table_name, hsize_t nrecords,size_t type_size, const size_t * field_offsets, const size_t * field_sizes,void * data){
void compound_write_catalog(hid_t dst_file,char * grp, const char * table_name, hsize_t nrecords,
      hsize_t start, size_t type_size, const size_t * field_offsets, const size_t * field_sizes,void * data){
  herr_t ierr=-1;
  hid_t igroup=-1;
  hid_t ifile=-1;
  //ifile= H5Fopen(dst_file,H5F_ACC_RDWR,H5P_DEFAULT);
  //if(ifile<0) printf("file open error:%s",dst_file);
  ifile=dst_file;
  igroup= H5Gopen(ifile,grp,H5P_DEFAULT);
  ierr=H5TBwrite_records ( igroup, table_name, start, nrecords, type_size, field_offsets, field_sizes, data);
  if(ierr<0) printf("write records error\n");
  if(igroup>=0) H5Gclose(igroup);
  //if(ifile>=0)  H5Fclose(ifile);
}
void get_catalog_offset(hsize_t * offsetlist, hsize_t * len_offset, hsize_t fiberid, hsize_t nrecords, hsize_t * data_field){
     int id=0;
     int jd=0;
     len_offset=0;
     if(offsetlist==NULL || data_field==NULL) {
       printf("offsetlist or data_field is NULL\n");
       exit(EXIT_FAILURE);
     }
     for (id=0;id<nrecords;id++){
       if(fiberid==data_field[id]){
         len_offset++;
         offsetlist[jd]=id;
         jd++;
       }
     }
}
