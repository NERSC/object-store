//#ifndef H5_VOL_H
//#define H5_VOL_H
#include <Python.h>
#include <numpy/arrayobject.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "hdf5.h"
#include "H5private.h"          /* Generic Functions                    */

#include "H5Dprivate.h"         /* Datasets                             */
//#include "H5Eprivate.h"         /* Error handling                       */
#include "H5Fprivate.h"         /* Files                                */
#include "H5FDprivate.h"        /* File drivers                         */
//#include "H5FFprivate.h"        /* Fast Forward                         */
#include "H5Iprivate.h"         /* IDs                                  */
//#include "H5Mprivate.h"         /* Maps                                 */
//#include "H5MMprivate.h"        /* Memory management                    */
//#include "H5Opkg.h"             /* Objects                              */
#include "H5Pprivate.h"         /* Property lists                       */
#include "H5Sprivate.h"         /* Dataspaces                           */
//#include "H5TRprivate.h"        /* Transactions                         */
#include "H5VLprivate.h"        /* VOL plugins                          */

#include "python_vol.h"
#include "inttypes.h"
#define PYTHON 502
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
//static herr_t H5VL_python_init(hid_t vipl_id);
//static herr_t H5VL_python_term(hid_t vtpl_id);
/* Datatype callbacks */
static void *H5VL_python_datatype_commit(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req);
static void *H5VL_python_datatype_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t tapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_python_datatype_get(void *dt, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_python_datatype_close(void *dt, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void *H5VL_python_dataset_create(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_python_dataset_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_python_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                    hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_python_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                     hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_python_dataset_close(void *dset, hid_t dxpl_id, void **req);
//static herr_t H5VL_python_dataset_get(void * obj, H5VL_dataset_get_t get_type, hid_t dxpl_id, void H5_ATTR_UNUSED **req, va_list arguments);
/* File callbacks */
static void *H5VL_python_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *H5VL_python_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_python_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_python_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void *H5VL_python_group_create(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_python_group_close(void *grp, hid_t dxpl_id, void **req);

/* Link callbacks */

/* Object callbacks */
static void *H5VL_python_object_open(void *obj, H5VL_loc_params_t loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req);
static herr_t H5VL_python_object_specific(void *obj, H5VL_loc_params_t loc_params, H5VL_object_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);

hid_t native_plugin_id = -1;

const H5VL_class_t H5VL_python_g = {
    0,							/* version */
    PYTHON,						/* value */
    "python",                                      	/* name */
    NULL, //    H5VL_python_init,                        /* initialize */
    NULL, //    H5VL_python_term,                        /* terminate */
    sizeof(hid_t),
    NULL,
    NULL,
    {                                           /* attribute_cls */
        NULL, //H5VL_python_attr_create,                /* create */
        NULL, //H5VL_python_attr_open,                  /* open */
        NULL, //H5VL_python_attr_read,                  /* read */
        NULL, //H5VL_python_attr_write,                 /* write */
        NULL, //H5VL_python_attr_get,                   /* get */
        NULL, //H5VL_python_attr_specific,              /* specific */
        NULL, //H5VL_python_attr_optional,              /* optional */
        NULL  //H5VL_python_attr_close                  /* close */
    },
    {                                           /* dataset_cls */
        H5VL_python_dataset_create,                    /* create */
        H5VL_python_dataset_open,                      /* open */
        H5VL_python_dataset_read,                      /* read */
        H5VL_python_dataset_write,                     /* write */
        NULL, //H5VL_python_dataset_get,               /* get */
        NULL, //H5VL_python_dataset_specific,          /* specific */
        NULL, //H5VL_python_dataset_optional,          /* optional */
        H5VL_python_dataset_close                      /* close */
    },
    {                                               /* datatype_cls */
        H5VL_python_datatype_commit,                   /* commit */
        H5VL_python_datatype_open,                     /* open */
        H5VL_python_datatype_get,                      /* get_size */
        NULL, //H5VL_python_datatype_specific,         /* specific */
        NULL, //H5VL_python_datatype_optional,         /* optional */
        H5VL_python_datatype_close                     /* close */
    },
    {                                           /* file_cls */
        H5VL_python_file_create,                      /* create */
        H5VL_python_file_open,                        /* open */
        H5VL_python_file_get,                         /* get */
        NULL, //H5VL_python_file_specific,            /* specific */
        NULL, //H5VL_python_file_optional,            /* optional */
        H5VL_python_file_close                        /* close */
    },
    {                                           /* group_cls */
        H5VL_python_group_create,                     /* create */
        NULL, //H5VL_python_group_open,               /* open */
        NULL, //H5VL_python_group_get,                /* get */
        NULL, //H5VL_python_group_specific,           /* specific */
        NULL, //H5VL_python_group_optional,           /* optional */
        H5VL_python_group_close                       /* close */
    },
    {                                           /* link_cls */
        NULL, //H5VL_python_link_create,                /* create */
        NULL, //H5VL_python_link_copy,                  /* copy */
        NULL, //H5VL_python_link_move,                  /* move */
        NULL, //H5VL_python_link_get,                   /* get */
        NULL, //H5VL_python_link_specific,              /* specific */
        NULL, //H5VL_python_link_optional,              /* optional */
    },
    {                                           /* object_cls */
        H5VL_python_object_open,                        /* open */
        NULL, //H5VL_python_object_copy,                /* copy */
        NULL, //H5VL_python_object_get,                 /* get */
        H5VL_python_object_specific,                    /* specific */
        NULL, //H5VL_python_object_optional,            /* optional */
    },
    {
        NULL,
        NULL,
        NULL
    },
    NULL
};

typedef struct H5VL_python_t {
    void  *under_object;
    hid_t space_id;
    hid_t type_id;
} H5VL_python_t;

typedef struct H5VL_DT {
    int  ndims;	  // Acquired with H5Sget_simple_extent_ndims
    hsize_t * dims;      // Acquired with H5Sget_simple_extent_dims 
    hsize_t * maxdims;   // Acquired with H5Sget_simple_extent_dims
    int  py_type;        // Acquired with H5T_get_class, Support H5T_INTEGER, 16/32, FLOAT 32/64 for now. 
} H5VL_DT;


PyObject * pInstance=NULL;

/* File callbacks Implementation*/
static void *
H5VL_python_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    //hid_t under_fapl;
    H5VL_python_t *file;
    file = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));

    //under_fapl = *((hid_t *)H5Pget_vol_info(fapl_id));
    //printf("under_fapl:%ld\n",under_fapl);
    int ipvol=0; //default is using swift for python vol
    char pvol_name[3]="py"; 
    if( H5Pexist(fapl_id, pvol_name)>0){
      H5Pget(fapl_id, pvol_name, &ipvol);
    }
   
    PyObject *pModule, *pClass;
    PyObject *pValue=NULL; 
    const char module_name[ ] = "python_vol";
    char class_name[] = "swift";
    char method_name[]= "H5VL_python_file_create";
    pModule = PyImport_ImportModule(module_name); 
    // Instantiate an object
    PyErr_Print();    
    
    if(pModule != NULL){
       //if(pInstance!=NULL){
       //   printf("Supporting one file operation only, please close existing files before opening/creating new file\n");
          //call file close and free file instance
        //  Py_DECREF(pInstance);
        //  return NULL;
       //}
       pInstance = PyObject_CallMethod(pModule, class_name,NULL,NULL);
       PyErr_Print();
    }
    else{
       printf("Failed to get non-null file class\n");
    }
    if(pInstance == NULL)
       printf("New File instance failed\n");
    else{
       pValue = PyObject_CallMethod(pInstance, method_name, "sllllll", name, flags, fcpl_id, fapl_id, dxpl_id, 0, 0);
       PyErr_Print();
       if (pValue != NULL) {
	     //printf("------- Result of H5Fopen from python: %ld\n", PyLong_AsLong(pValue));
             PyObject * rt=PyLong_AsVoidPtr(pValue);
             void * rt_py = rt;
	     if (rt_py==NULL) fprintf(stderr, "File create, returned pointer from python is NULL\n");
             file->under_object = rt_py;
 	     return (void *) file;
        }
        else {
             fprintf(stderr,"Call failed in H5VL File Create\n");
             return NULL;
        }	
    }
    //printf("------- PYTHON H5Fcreate\n");
    return NULL;
}

static void *
H5VL_python_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    //printf("in H5VL file open\n");
    hid_t under_fapl;
    H5VL_python_t *file;
    file = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));
    //under_fapl = *((hid_t *)H5Pget_vol_info(fapl_id));
    PyObject *pModule=NULL, *pClass=NULL;
    PyObject *pValue=NULL;
    //printf("Testing H5VL file open\n");
    //const char class_name[ ] = "H5PVol";
    int ipvol=0; //default is using swift for python vol
    char pvol_name[3]="py";
    if( H5Pexist(fapl_id, pvol_name)>0){
      H5Pget(fapl_id, pvol_name, &ipvol);
    }


    char class_name [ ] = "swift";
    const char module_name[ ] = "python_vol";
    pModule = PyImport_ImportModule(module_name);
    //PyErr_Print();
    //Instantiate an object
  
    if(pModule != NULL){
      //if(pInstance!=NULL){
	//     printf("Supporting one file only, please close existing files before opening/creating new file\n");	
	     //call file close and free file instance
	  //   Py_DECREF(pInstance);
	     //return NULL; 
       //}
       //printf("New file object\n");
       pInstance = PyObject_CallMethod(pModule, class_name,NULL,NULL);
    }
    else{
       printf("Failed to get non-null file class\n");
    }
     
    //file->under_object = H5VLfile_open(name, flags, under_fapl, dxpl_id, req);
    char method_name[]= "H5VL_python_file_open";
    if(pInstance == NULL)
       fprintf(stderr, "New File instance failed\n");
    else{
       pValue = PyObject_CallMethod(pInstance, method_name, "slllll", name, flags, fapl_id, dxpl_id, 0, 0);
       //PyErr_Print();
       if (pValue != NULL) {
             //printf("------- Result of H5Fopen from python: %ld\n", PyLong_AsLong(pValue));
             PyObject * rt=PyLong_AsVoidPtr(pValue);
             void * rt_py = rt;
             if (rt_py==NULL) fprintf(stderr, "File open, returned pointer from python is NULL\n");
             file->under_object = rt_py;
             return (void *) file;
        }
        else {
             fprintf(stderr,"Call failed in H5VL File Open\n");
             return NULL;
        }
    }
    //printf("------- PYTHON H5Fopen\n");
    return (void *)file;	
}

static herr_t 
H5VL_python_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_python_t *f = (H5VL_python_t *)file;

    H5VLfile_get(f->under_object, native_plugin_id, get_type, dxpl_id, req, arguments);

    //printf("------- PYTHON H5Fget %d\n", get_type);
    return 1;
}
static herr_t 
H5VL_python_file_close(void *file, hid_t dxpl_id, void **req)
{
    H5VL_python_t *f = (H5VL_python_t *)file;
    if(f==NULL || f->under_object==NULL) {
	return 1;
    }
    PyObject * plong_under = PyLong_FromVoidPtr(f->under_object);
    PyObject *pValue=NULL;
    char method_name[]= "H5VL_python_file_close";
    if(pInstance==NULL){
      printf("pInstance is NULL in file close\n");
      return -1;
    }else{
      pValue = PyObject_CallMethod(pInstance, method_name, "lll", PyLong_AsLong(plong_under), dxpl_id, 0);
      PyErr_Print();
      if(pValue !=NULL){
        //printf("------- Result of H5Fclose from python: %ld\n", PyLong_AsLong(pValue));
        free(f);
	Py_DECREF(pInstance);
	//printf("cleaning pInstance\n");
        return 1;
      }
    }
    return -1;
}
/* Group callbacks Implementation*/
static void *
H5VL_python_group_create(void *obj, H5VL_loc_params_t loc_params, const char *name, 
                      hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_python_t *group;
    H5VL_python_t *o = (H5VL_python_t *)obj;
    PyObject * plong = PyLong_FromVoidPtr(o); // get the id of py object, and pass into python layer to form a py obj
    PyObject * plong_under = PyLong_FromVoidPtr(o->under_object);
    group = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));
    PyObject *pArgs, *pValue=NULL;
    char method_name[]= "H5VL_python_group_create";
    if(pInstance==NULL){
      printf("pInstance is NULL in group create\n");
      return NULL; 
    }else{
      pValue = PyObject_CallMethod(pInstance, method_name, "llsllll", PyLong_AsLong(plong_under), 0, name, gcpl_id, gapl_id, dxpl_id, 0);
      if(pValue !=NULL){
	//printf("------- Result of H5Fcreate from python: %ld\n", PyLong_AsLong(pValue));	
	void * rt_py = PyLong_AsVoidPtr(pValue);
        if (rt_py==NULL) fprintf(stderr, "File create, returned pointer from python is NULL\n");
	group->under_object = rt_py;
	return (void *) group;
      }	      
    } 
    //printf ("------- PYTHON H5Gcreate\n");
    return (void *) group;
}

static herr_t 
H5VL_python_group_close(void *grp, hid_t dxpl_id, void **req)
{
    H5VL_python_t *g = (H5VL_python_t *)grp;
    PyObject * plong_under = PyLong_FromVoidPtr(g->under_object);
    PyObject *pValue=NULL;
    char method_name[]= "H5VL_python_group_close";
    if(pInstance==NULL){
      fprintf(stderr, "pInstance is NULL in group close\n");
      return 1; 
    }else{
      pValue = PyObject_CallMethod(pInstance, method_name, "lll", PyLong_AsLong(plong_under), dxpl_id, 0);
      PyErr_Print();
      if(pValue !=NULL){
        //printf("------- Result of H5Gclose from python: %ld\n", PyLong_AsLong(pValue));
        free(g);
        return 1;
      }
    }
   return -1;
}
/* Datatypes callbacks Implementation*/
static void *
H5VL_python_datatype_commit(void *obj, H5VL_loc_params_t loc_params, const char *name, 
                         hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_python_t *dt;
    H5VL_python_t *o = (H5VL_python_t *)obj;

    dt = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));
    
    dt->under_object = H5VLdatatype_commit(o->under_object, loc_params, native_plugin_id, name, 
                                           type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req);

    //printf("------- PYTHON H5Tcommit\n");
    return dt;
}
static void *
H5VL_python_datatype_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_python_t *dt;
    H5VL_python_t *o = (H5VL_python_t *)obj;  

    dt = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));

    dt->under_object = H5VLdatatype_open(o->under_object, loc_params, native_plugin_id, name, tapl_id, dxpl_id, req);

    //printf("------- PYTHON H5Topen\n");
    return (void *)dt;
}
static herr_t 
H5VL_python_datatype_get(void *dt, H5VL_datatype_get_t get_type, hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_python_t *o = (H5VL_python_t *)dt;
    herr_t ret_value;

    ret_value = H5VLdatatype_get(o->under_object, native_plugin_id, get_type, dxpl_id, req, arguments);

    //printf("------- PYTHON datatype get\n");
    return ret_value;
}

static herr_t 
H5VL_python_datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    H5VL_python_t *type = (H5VL_python_t *)dt;

    assert(type->under_object);

    H5VLdatatype_close(type->under_object, native_plugin_id, dxpl_id, req);
    free(type);

    //printf("------- PYTHON H5Tclose\n");
    return 1;
}
/* Object callbacks Implementation*/
static void *
H5VL_python_object_open(void *obj, H5VL_loc_params_t loc_params, H5I_type_t *opened_type, hid_t dxpl_id, void **req)
{
    H5VL_python_t *new_obj;
    H5VL_python_t *o = (H5VL_python_t *)obj;

    new_obj = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));
    
    new_obj->under_object = H5VLobject_open(o->under_object, loc_params, native_plugin_id, opened_type, dxpl_id, req);

    //printf("------- PYTHON H5Oopen\n");
    return (void *)new_obj;
}

static herr_t 
H5VL_python_object_specific(void *obj, H5VL_loc_params_t loc_params, H5VL_object_specific_t specific_type, 
                         hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_python_t *o = (H5VL_python_t *)obj;

    H5VLobject_specific(o->under_object, loc_params, native_plugin_id, specific_type, dxpl_id, req, arguments);

    //printf("------- PYTHON Object specific\n");
    return 1;
}
/* Dataset callbacks Implementation*/


void helper_dt (hid_t dcpl_id, H5VL_DT * dt){
     hid_t space_id, type_id;
     H5Pget (dcpl_id, "dataset_space_id", &space_id); 
     H5Pget (dcpl_id, "dataset_type_id", &type_id);

     dt->ndims = H5Sget_simple_extent_ndims (space_id);
     //printf("Checking dt ndims:%d\n",dt->ndims);
     dt->dims = malloc(sizeof(unsigned long long int )* dt->ndims);
     dt->maxdims = malloc(sizeof(unsigned long long int)* dt->ndims);
     H5Sget_simple_extent_dims(space_id,dt->dims, dt->maxdims);
     hid_t atomic_type = H5Tget_class(type_id); // return type, see H5Tpublic.h
     size_t type_size = H5Tget_size(type_id); // return number of bytes, e.g., if int, it could be 4, 8, 16, 32, etc
     int py_type=0; // default is int16 

     if(atomic_type==0||atomic_type==1){
       int nbits = type_size*8; 
       if(nbits==16&&(int)atomic_type==0)      py_type=0; // int16
       else if(nbits==32&&(int)atomic_type==0) py_type=1; // int32
       else if(nbits==32&&(int)atomic_type==1) py_type=2; // float32
       else if(nbits==64&&(int)atomic_type==1) py_type=3; // float64
       else{fprintf(stderr, "type is not supported, only support int16, int32, float32, double\n");}
     }
     else fprintf(stderr, "type is not supported, only support int and float\n");
     dt->py_type=py_type;
}

static void *
H5VL_python_dataset_create(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req) 
{
    H5VL_python_t *dset;
    H5VL_python_t *o = (H5VL_python_t *)obj;
    PyObject * plong_under = PyLong_FromVoidPtr(o->under_object);
    dset = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));
    import_array();
    H5VL_DT * dt= malloc(sizeof(H5VL_DT)); //get the dataset size, type
    helper_dt (dcpl_id, dt);
    //printf("Testing H5VL_DT,%d\n",dt->py_type); 
    npy_intp dtm=dt->ndims; 
    PyObject * py_dims = PyArray_SimpleNewFromData(1, &dtm, NPY_INTP,dt->dims);//TODO: 64 bits, vs 32 bit 
    PyObject * py_maxdims = PyArray_SimpleNewFromData(1, &dtm, NPY_INTP,dt->maxdims );  
    PyObject *pValue=NULL;
    char method_name[] = "H5VL_python_dataset_create";
    size_t type_size = 0;
    size_t space_size = 0;
    hid_t type_id, space_id;
    void *type_buf = NULL;
    void *space_buf = NULL; 
    H5P_genplist_t *plist = NULL;      /* Property list pointer */ 
    /* Get the dcpl plist structure */
    plist = (H5P_genplist_t *)H5I_object(dcpl_id);
    /*
    if(NULL == (plist = (H5P_genplist_t *)H5I_object(dcpl_id)))
        HGOTO_ERROR(H5E_ATOM, H5E_BADATOM, NULL, "can't find object for ID")
    */
    /* get creation properties */
    H5P_get(plist, H5VL_PROP_DSET_TYPE_ID, &type_id);
    H5P_get(plist, H5VL_PROP_DSET_SPACE_ID, &space_id);
    /*
    if(H5P_get(plist, H5VL_PROP_DSET_TYPE_ID, &type_id) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for datatype id")
    if(H5P_get(plist, H5VL_PROP_DSET_SPACE_ID, &space_id) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get property value for space id")
    */
    if(pInstance==NULL){
      fprintf(stderr, "pInstance is NULL in group create\n");
      return NULL; 
    }else{
	pValue = PyObject_CallMethod(pInstance, method_name, "llsllllilOO", PyLong_AsLong(plong_under), 0, name, dcpl_id, dapl_id, dxpl_id, 0,dt->ndims,dt->py_type,py_dims, py_maxdims);
     if(pValue !=NULL){
        //printf("------- Result of H5Dcreate from python: %ld\n", PyLong_AsLong(pValue));
        void * rt_py = PyLong_AsVoidPtr(pValue);
        if (rt_py==NULL) fprintf(stderr, "Dataset create, returned pointer from python is NULL\n");
        dset->under_object = rt_py;
	//when you create a dataset, you also want to serialize the space and type info and adds as metadata into the dataset
	//you can just store it in memory, but later when you want to re-open this dataset, you still need to retrieve it from storage
	//let me first try the in memory part. 
	//The following encode part will be used in disk verision
        /* Encode dataspace */
        /*
        if(H5Sencode(space_id, NULL, &space_size) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of dataaspace")
        if(NULL == (space_buf = H5MM_malloc(space_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized dataaspace")
        if(H5Sencode(space_id, space_buf, &space_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize dataaspace")	
        */
	//type_id
	/* Encode datatype */
        /*
        if(H5Tencode(type_id, NULL, &type_size) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "can't determine serialized length of datatype")
        if(NULL == (type_buf = H5MM_malloc(type_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't allocate buffer for serialized datatype")
        if(H5Tencode(type_id, type_buf, &type_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, NULL, "can't serialize datatype")
        */
        dset->space_id=space_id;
	dset->type_id=type_id;
	return (void *) dset;
      }      
    } 
    return NULL;

}
/*
static void *                                                                                                                               
H5VL_python_dataset_get(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)               
{   
    H5VL_python_t *dset;
    H5VL_python_t *o = (H5VL_python_t *)obj;
    PyObject * plong_under = PyLong_FromVoidPtr(o->under_object);                                                                           
    dset = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));
    import_array();
    H5VL_DT * dt= malloc(sizeof(H5VL_DT)); //get the dataset size, type                                                                     
    helper_dt (dcpl_id, dt);                                                                                                                
    //printf("Testing H5VL_DT,%d\n",dt->py_type); 
    npy_intp dtm=dt->ndims;  
    PyObject * py_dims = PyArray_SimpleNewFromData(1, &dtm, NPY_INTP,dt->dims);//TODO: 64 bits, vs 32 bit                                   
    PyObject * py_maxdims = PyArray_SimpleNewFromData(1, &dtm, NPY_INTP,dt->maxdims );                                                      
    PyObject *pValue=NULL;                                                                                                                  
    char method_name[] = "H5VL_python_dataset_create";
    if(pInstance==NULL){                                                                                                                    
      fprintf(stderr, "pInstance is NULL in group create\n");                                                                               
      return NULL; 
    }else{
      pValue = PyObject_CallMethod(pInstance, method_name, "llsllllllOO", PyLong_AsLong(plong_under), 0, name, dcpl_id, dapl_id, dxpl_id, 0,dt->ndims,dt->py_type,py_dims, py_maxdims);
      if(pValue !=NULL){
        //printf("------- Result of H5Dcreate from python: %ld\n", PyLong_AsLong(pValue));                                                  
        void * rt_py = PyLong_AsVoidPtr(pValue);
        if (rt_py==NULL) fprintf(stderr, "Dataset create, returned pointer from python is NULL\n");                                         
        dset->under_object = rt_py;                                                                                                         
        return (void *) dset;                                                                                                               
      }                                                                                                                                     
    } 
    return NULL;                                                                                                                            

}
*/
static void *
H5VL_python_dataset_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_python_t *dset;
    H5VL_python_t *o = (H5VL_python_t *)obj;
    PyObject * plong_under = PyLong_FromVoidPtr(o->under_object);
    dset = (H5VL_python_t *)calloc(1, sizeof(H5VL_python_t));
    PyObject *pValue=NULL;
    char method_name[] = "H5VL_python_dataset_open";
    if(pInstance==NULL){
      fprintf(stderr, "pInstance is NULL in dataset open\n");
      return NULL;
    }else{
      pValue = PyObject_CallMethod(pInstance, method_name, "llslll", PyLong_AsLong(plong_under), 0, name, dapl_id, dxpl_id,0 );
      if(pValue !=NULL){
        //printf("------- Result of H5Dopen from python: %ld\n", PyLong_AsLong(pValue));
        void * rt_py = PyLong_AsVoidPtr(pValue);
        if (rt_py==NULL) fprintf(stderr, "Dataset open, returned pointer from python is NULL\n");
        dset->under_object = rt_py;
        return (void *) dset;
      }
    }
    return NULL;
}
/*
hsize_t H5Count(long dsetId, hsize_t *nelem, int * type_size)
{
    char dt_name[] = "H5VL_python_dt_info";
    npy_intp ndims=0,dtype=0;
    npy_intp *dims=NULL;
    //if((h5_ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
    //    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")
    //if(ndims != H5Sget_simple_extent_dims(dset->space_id, dim, NULL))
    //    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions")
    
    import_array();
    //retrieve the dataset information based dataset id in python vol layer. 
    if(pInstance!=NULL){
       PyObject * dt_obj= PyObject_CallMethod(pInstance, dt_name, "l",dsetId);
       if(dt_obj==NULL){
        fprintf(stderr, "dt_Obj is null\n");
        return -1;
       }
       PyArrayObject * dt_arr=(PyArrayObject *)dt_obj;
       //convert back to c array
       if(dt_arr->descr->type_num>=0){
         npy_intp * dt_if =(npy_intp *) dt_arr->data;
         ndims=dt_if[0];
         dtype=dt_if[1];
         dims=dt_if+2; //pointer starts from 3rd element
       }else{
        printf("dt_arr.type_num:%d is not PyArray_LONG\n",dt_arr->descr->type_num);
       }

    }
    else{
       fprintf(stderr, "pInstance is NULL\n");
       return -1;
    }
   npy_intp i=0;
   npy_intp x=1;
   for(i=0;i<ndims;i++) x*=dims[i];
   *x1=(hsize_t)x;
   *type_size=(int)dtype
   return (hsize_t) x;
}
*/
PyObject * Data_CPY(long dsetId, void * buf,hsize_t * nelem, int * type_size)
{
    char dt_name[] = "H5VL_python_dt_info";
    npy_intp ndims=0,dtype=0;
    npy_intp *dims=NULL;
    /*if((h5_ndims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")
    if(ndims != H5Sget_simple_extent_dims(dset->space_id, dim, NULL))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions")
    */
    import_array();
    //retrieve the dataset information based dataset id in python vol layer. 
    if(pInstance!=NULL){
       PyObject * dt_obj= PyObject_CallMethod(pInstance, dt_name, "l",dsetId);
       if(dt_obj==NULL){
        fprintf(stderr, "dt_Obj is null\n");
        return NULL; 
       }
       PyArrayObject * dt_arr=(PyArrayObject *)dt_obj;
       //convert back to c array
       if(dt_arr->descr->type_num>=0){
         npy_intp * dt_if =(npy_intp *) dt_arr->data;
         ndims=dt_if[0];
         dtype=dt_if[1];
         dims=dt_if+2; //pointer starts from 3rd element
       }else{
        printf("dt_arr.type_num:%d is not PyArray_LONG\n",dt_arr->descr->type_num);
       }

    }
    else{
       fprintf(stderr, "pInstance is NULL\n");
       return NULL; 
    }
    //Create pyobject reference to c buffer
    PyObject * pydata;
    if (dtype == 0){//int16 
      pydata = PyArray_SimpleNewFromData(ndims, dims, NPY_INT16, buf );
      *type_size=(int)sizeof(short int);
    }
    else if (dtype == 1){//int32
      pydata = PyArray_SimpleNewFromData(ndims, dims, NPY_INT32, buf );
      *type_size=(int)sizeof(int);
    }
    else if (dtype == 2) {//float32
      pydata = PyArray_SimpleNewFromData(ndims, dims, NPY_FLOAT, buf );
      *type_size=(int)sizeof(float);
    }
    else if (dtype == 3) {//float64
      pydata = PyArray_SimpleNewFromData(ndims, dims, NPY_DOUBLE, buf );
      *type_size=(int)sizeof(double);
    }
    else {
      fprintf(stderr, "Type is not supported for now Jan 31 2018\n");
      return NULL; 
    }
    //convert to C-contiguous array
    PyObject * pydata_c = PyArray_FROM_OF(pydata, NPY_ARRAY_C_CONTIGUOUS);
    npy_intp x=1;
    int i;
    for(i=0;i<ndims;i++) x*=dims[i];
    *nelem = (hsize_t)x;
    return pydata_c;
}
PyObject * Data_CPY2(long dsetId, void * buf, H5VL_python_t * dset)
{
    char dt_name[] = "H5VL_python_dt_info";
    npy_intp ndims=0,dtype=0;
    npy_intp *dims=NULL;
    hsize_t h5_dims[H5S_MAX_RANK];
    //int h5_ndims; 
    //h5_ndims = H5Sget_simple_extent_ndims(dset->space_id);
    //H5Sget_simple_extent_dims(dset->space_id, h5_dims, NULL);
    /*if((h5_dims = H5Sget_simple_extent_ndims(dset->space_id)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of dimensions")
    if(h5_ndims != H5Sget_simple_extent_dims(dset->space_id, dim, NULL))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dimensions")
    */
    /*int x=0;
    printf("In Data_CPY2:\n");
    for(x=0;x<h5_ndims;x++){
	printf("%d-th dims is:%lu\n",x,(unsigned long)h5_dims[x]);	
    }
    */
    import_array();
    //retrieve the dataset information based dataset id in python vol layer. 
    if(pInstance!=NULL){
       PyObject * dt_obj= PyObject_CallMethod(pInstance, dt_name, "l",dsetId);
       if(dt_obj==NULL){
        fprintf(stderr, "dt_Obj is null\n");
        return NULL;
       }
       PyArrayObject * dt_arr=(PyArrayObject *)dt_obj;
       //convert back to c array
       if(dt_arr->descr->type_num>=0){
         npy_intp * dt_if =(npy_intp *) dt_arr->data;
         ndims=dt_if[0];
         dtype=dt_if[1];
         dims=dt_if+2; //pointer starts from 3rd element
       }else{
        printf("dt_arr.type_num:%d is not PyArray_LONG\n",dt_arr->descr->type_num);
       }
    }
    else{
       fprintf(stderr, "pInstance is NULL\n");
       return NULL;
    }
    //Create pyobject reference to c buffer

    PyObject * pydata;
    if (dtype == 0){//int16 
      pydata = PyArray_SimpleNewFromData(ndims, dims, NPY_INT16, buf );
    }
    else if (dtype == 1){//int32
      pydata = PyArray_SimpleNewFromData(ndims, dims, NPY_INT32, buf );
    }
    else if (dtype == 2) {//float32
      pydata = PyArray_SimpleNewFromData(ndims, dims, NPY_FLOAT, buf );
    }
    else if (dtype == 3) {//float64
      pydata = PyArray_SimpleNewFromData(ndims, dims, NPY_DOUBLE, buf );
    }
    else {
      fprintf(stderr, "Type is not supported for now Jan 31 2018\n");
      return NULL;
    }
    //convert to C-contiguous array
    PyObject * pydata_c = PyArray_FROM_OF(pydata, NPY_ARRAY_C_CONTIGUOUS);
    return pydata_c;
}

static herr_t 
H5VL_python_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                      hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    H5VL_python_t *o = (H5VL_python_t *)dset;
    PyObject * plong_under = PyLong_FromVoidPtr(o->under_object);
    //create py reference to c buffer
    //PyObject * pydata=PyObject_Malloc(1);//
    //printf("BEFORE DATA_CPY"); 
    hsize_t * count_size=malloc(sizeof(hsize_t));
    int * type_size=malloc(sizeof(int));
    PyObject * pydata= Data_CPY(PyLong_AsLong(plong_under), buf,count_size,type_size);
    PyObject *pValue=NULL;
    char method_name[] = "H5VL_python_dataset_read";
    if(pInstance==NULL){
      printf("pInstance is NULL in dataset read\n");
      return -1; 
    }else{
      pValue = PyObject_CallMethod(pInstance, method_name, "lllllOl", PyLong_AsLong(plong_under),  mem_type_id, mem_space_id, file_space_id,plist_id, pydata,0);
      PyErr_Print();
      if(pValue !=NULL){
	//printf("------- Result of H5Dread from python is not NULL\n");
	//if(pydata != NULL){
	 //PyArrayObject * dt_arr=(PyArrayObject *) pydata; 
	 //PyArrayObject * dt_arr=(PyArrayObject *) pValue; 
	 //void * buf1=(void *) (((PyArrayObject *) dt_arr)->data);
	 void * buf1=(void *) (((PyArrayObject *) pValue)->data);
	 buf = (int *) buf;
	 buf1 = (int *) buf1;
	 //hsize_t count_size=H5Count(PyLong_AsLong(plong_under));
	 //int type_size = H5Type_size(PyLong_AsLong(plong_under));
	 //printf("In vol, number of elements is:%lu\n",(unsigned long)count_size);
	 if(count_size<0) {printf("number of elements is zero\n");return -1;}
	 memcpy(buf,buf1,*type_size**count_size);
	 //if(buf && buf1) {printf("done memcpy\n"); free(buf1);}
	 PyErr_Print();
	 //int k=0;
	 //for(k=0;k<60000;k++) printf("%d %d ",buf1[k],((int *) buf)[k]);
	 //free(buf1);
         return 1;
	//}
	//else{
        // printf("pydata returned from python is NULL\n");
        //}
      }
    }
   //printf ("------- PYTHON H5Dread\n");
    return 1;     
}
static herr_t 
H5VL_python_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                       hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    H5VL_python_t *o = (H5VL_python_t *)dset;
    PyObject * plong_under = PyLong_FromVoidPtr(o->under_object);
    //PyObject * pydata= Data_CPY(PyLong_AsLong(plong_under), (void *)buf);
    PyObject * pydata = Data_CPY2(PyLong_AsLong(plong_under), (void *)buf, o);
    PyErr_Print();
    PyObject *pValue=NULL;
    char method_name[] = "H5VL_python_dataset_write";
    //Call dataset write method
    if(pInstance==NULL){
      printf("pInstance is NULL in dataset write\n");
      return -1;
    }else{
	//printf("Calling in dataset_write in C\n");
      pValue = PyObject_CallMethod(pInstance, method_name, "lllllOl", PyLong_AsLong(plong_under),  mem_type_id, mem_space_id, file_space_id,plist_id, pydata, 0);
      if(pValue !=NULL){
        //printf("------- Result of H5Dwrite from python: %ld\n", PyLong_AsLong(pValue));
        return 1;
      }
    }   
    //printf ("-------! PYTHON H5Dwrite\n");
    return -1;     
}
static herr_t 
H5VL_python_dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    H5VL_python_t *d = (H5VL_python_t *)dset;
    PyObject * plong_under = PyLong_FromVoidPtr(d->under_object);    
    PyObject *pValue=NULL;
    char method_name [] ="H5VL_python_dataset_close";
    if(pInstance==NULL){
      printf("pInstance is NULL in dataset close\n");
      return 1;
    }else{
      //printf("in C, dataset id is %ld\n",PyLong_AsLong(plong_under));
      pValue = PyObject_CallMethod(pInstance, method_name, "lll", PyLong_AsLong(plong_under), dxpl_id, 0);
      PyErr_Print(); 
      if(pValue !=NULL){
        //printf("------- Result of H5Dclose from python: %ld\n", PyLong_AsLong(pValue));
        return 1;
      }
    }
    //printf ("------- PYTHON H5Dclose\n");
    free(d);
    return 1;
}
#if 0
static void *H5VL_python_attr_create(void *obj, H5VL_loc_params_t loc_params, const char *attr_name, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req){    
static herr_t H5VL_python_attr_close(void *attr, hid_t dxpl_id, void **req){

/* Datatype callbacks */


/* Dataset callbacks */
static void *H5VL_python_dataset_create(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req){
static herr_t H5VL_python_dataset_close(void *dset, hid_t dxpl_id, void **req){

/* File callbacks */

    
static void *H5VL_python_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req){


/* Group callbacks */

static void *H5VL_python_group_open(void *obj, H5VL_loc_params_t loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req){
static herr_t H5VL_python_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments){


/* Link callbacks */

/* Object callbacks */


#endif

//#endif
