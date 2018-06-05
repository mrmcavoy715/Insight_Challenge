
with open('./insight_testsuite/tests/test_1/input/log.csv') as f:

    first_line_pointer = f.tell()
    first_line = f.readline()
    print("first_line\t", first_line)

    f.seek(first_line_pointer)

    for line in f:
        print("next line\t", line)
