from sklearn.linear_model import SGDRegressor


def process_line(line):
    t_line = line.strip()[1:-1]
    # Remove file position
    t_line = t_line.split(', \"__fileposition__\":')[0]
    tt_line = [t_l.split(':')[-1].strip().replace('"', '') for t_l in t_line.split(',')]
    values = map(float, tt_line)
    return values


def iter_minibatches(filename, chunksize):
    def split(lines):
        x = []
        y = []
        for line in lines:
            values = process_line(line)
            x.append(values[:-1])
            y.append(values[-1])
        return [x, y]

    count = 1
    rows = []
    # Provide chunks one by one
    with open(filename, 'r') as f:
        for line in f:
            rows.append(line)
            if count % chunksize == 0:
                yield split(rows)
                rows = []
            count += 1


def get_test_data(filename):
    x = []
    y = []
    count = 1
    with open(filename, 'r') as f:
        for line in f:
            if len(line) == 1 and ord(line[0]) == 10:  # EOF
                continue
            temp_values = process_line(line)
            x.append(temp_values[:-1])
            y.append(temp_values[-1])
    return x, y


def main():
    no_rows = 1000
    filename = "/home/osboxes/GIT/mount_pnt2/vn/casp"
    batcherator = iter_minibatches(filename, chunksize=no_rows)
    model = SGDRegressor()

    count = 1
    # Train model
    for item in batcherator:
        X_chunk = item[0]
        y_chunk = item[1]
        print "--" * 10, count, len(y_chunk)
        assert(len(X_chunk) == len(y_chunk)), "Something is wrong"
        assert(len(y_chunk) == no_rows), "Something is wrong"
        model.partial_fit(X_chunk, y_chunk)
        count += 1
    # Now make predictions with trained model
    x_test, y_test = get_test_data("/home/osboxes/GIT/mount_pnt2/vn/casp-test")
    y_predicted = model.predict(x_test)
    assert(len(y_test) == len(y_predicted)), "Something is wrong"
    abs_diff = 0
    for x,y in zip(y_test, y_predicted):
        abs_diff += abs(x-y)
    print "Absolute Diff: ", abs_diff



main()