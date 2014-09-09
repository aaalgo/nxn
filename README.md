NxN: a framework for out-of-core computation on a cluster.

Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All rights reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.


To build:

1. Edit config.h to reflect your cluster environment and job requirements.
2. Edit Makefile according to your compiling environment -- not needed if GCC is used.
3. Type make.

TO use the library:

1. Carefully read the file nxn.h.
2. #include <nxn.h> in your C/C++ source code and link against libnxn.a.

