# python-fuse-sample

This repo contains a very simple FUSE filesystem example in Python. It's the
code from a post I wrote a while back:

https://www.stavros.io/posts/python-fuse-filesystem/

If you see anything needing improvement or have any feedback, please open an
issue.


# Notes
- Install dependencies by running
```
pip install -r REQUIREMENT.txt
```

- Usage
```
python passthrough_hpcc.py ip mountpoint
```

- Navigate to the mountpoint to see files on the HPCC cluster

- Currently the program can read both .csv files as well as thor files. Thor files are stored as a json while .csv files
 are stored in their actual format. 
# Details
- open() - The program downloads 20 rows from a dataset and saves it to the ./TEMP folder.
- read() - Reads from the ./TEMP directory
- During initialization the clean up method is called which clears the ./TEMP folder.
