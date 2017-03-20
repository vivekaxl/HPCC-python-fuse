filename = '/home/osboxes/GIT/mount_pnt2/vivek/c_ecolids.csv'
count = 1
with open(filename, 'r') as f:
    for line in f:
        print count, line
        count += 1
        # if count % 299 == 0:
        #     raw_input()