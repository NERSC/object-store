from __future__ import division, print_function
import h5py
import optparse
import argparse
import time

tt=0

def copy_value(input,srcdset,output,desdset):
  s=time.time()
  fx=h5py.File(input,'r')
  dx=fx[srcdset].value
  hx=h5py.File(output,'a')
  subx=hx[desdset]
  subx=dx
  fx.close()
  hx.close()
  global tt
  tnow=time.time()-s
  print ("copy_value %.3f seconds"%(tnow))
  tt=tt+tnow

def copy_bracket(input,srcdset,output,desdset):
  s=time.time()
  fx=h5py.File(input,'r')
  dx=fx[srcdset][()]
  hx=h5py.File(output,'a')
  subx=hx[desdset]
  subx=dx
  fx.close()
  hx.close()
  global tt
  tnow=time.time()-s
  print ("copy_bracket %.3f seconds"%(tnow))
  tt=tt+tnow

def copy_dirbracket(input, srcdset,output,desdset):
  s=time.time()
  fx=h5py.File(input,'r')
  hx=h5py.File(output,'a')
  hx[desdset]=fx[srcdset][()]
  fx.close()
  hx.close()
  global tt
  tnow=time.time()-s
  print ("copy_dirbracket %.3f seconds"%(tnow))
  tt=tt+tnow

def copy_copy(input, srcdset,output,desdset):
  global tt
  s=time.time()
  fx=h5py.File(input,'r')
  hx=h5py.File(output,'a')
  try:
   fx.copy(srcdset,hx[desdset])
  except Exception as e:
   print("source:%s, destination:%s"%(srcdset,desdset))
  fx.close()
  hx.close()
  tnow=time.time()-s
  print ("copy_copy %.3f seconds"%(tnow))
  tt=tt+tnow

def test_copy_copy(num,srcd,des,src):
    for i in range(1,int(num)):
      srcdi=srcd+str(i)+'/coadd'
      #print (srcdi)
      hx=h5py.File(des,'a')
      hx.create_group(srcd+str(i))
      hx.close()
      copy_copy(src,srcdi,des,srcd+str(i))
    global tt      
    print ("Total %.3f , Average %.3f"%(tt,tt/int(num)))

def test_copy_value(num,srcd,des,src):
    #must used previously created file by copy_copy
    for i in range(1,int(num)):
      srcdi=srcd+str(i)+'/coadd'
      #print (srcdi)
      copy_value(src,srcdi,des,srcdi)
    global tt
    print ("Total %.3f , Average %.3f"%(tt,tt/int(num)))


def test_copy_bracket(num,srcd,des,src):
    for i in range(1,int(num)):
      srcdi=srcd+str(i)+'/coadd'
      #print (srcdi)
      copy_bracket(src,srcdi,des,srcdi)
    global tt
    print ("Total %.3f , Average %.3f"%(tt,tt/int(num)))


def test_copy_dirbracket(num,srcd,des,src):
    for i in range(1,int(num)):
      srcdi=srcd+str(i)+'/coadd'
      #print (srcdi)
      copy_dirbracket(src,srcdi,des,srcdi)
    global tt
    print ("Total %.3f , Average %.3f"%(tt,tt/int(num)))
if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='h5bread')
    parser.add_argument("src",    help="src file")
    parser.add_argument("srcd",    help="parent id of src dset")
    parser.add_argument("num",    help="num of src dset")
    parser.add_argument("des",    help="des file")
    parser.add_argument("test",    help="which test")
    print ("copy_copy:1\ncopy_value:2\ncopy_bracket:3\ncopy_direct_bracket:4")
    opts=parser.parse_args()
    src = opts.src
    srcd = opts.srcd
    des = opts.des
    num = opts.num
    test = int(opts.test)
    print ("test:%d"%test)
    if test==1:
      print ("test:%d"%test)
      test_copy_copy(num,srcd,des,src)
    elif test==2:
      print ("test:%d"%test)
      test_copy_value(num,srcd,des,src)
    elif test==3:
      print ("test:%d"%test)
      test_copy_bracket(num,srcd,des,src)
    elif test==4:
      print ("test:%d"%test)
      test_copy_dirbracket(num,srcd,des,src)
    else:
      print("wront test options")
