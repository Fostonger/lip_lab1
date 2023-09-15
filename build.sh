#! /bin/bash
mkdir cmakeFiles
cd cmakeFiles
cmake ..
cp ../writing_script.py ../build/
make
