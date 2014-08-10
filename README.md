# sorted file seeker
=============================

### precondition
the file or certain column  must be sorted

### search total line

    search -k 'match total line'  file

### search the second column 

the columns must be splited with space

    search -i 2 -k 'column' file

### search within multiple files

    search -k 'name' file1 file2
