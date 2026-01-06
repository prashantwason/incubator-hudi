#!/usr/bin/env python3
import re
import csv
import sys
from os import listdir

BUILDING_MODULE_REGEX = re.compile(r'\[(INFO|WARNING|ERROR)\] Building (hudi-[^\s\\]+)\s')
TESTS_RUN_REGEX = re.compile(r'\[(INFO|WARNING|ERROR)\] Tests run:.+ Time elapsed: (.+) - in (.+)\s')

UNIT_TEST_REGEX = re.compile(r'^unittest.*\.txt$')
FUNCTIONAL_TEST_REGEX = re.compile(r'^functional_test.*\.txt$')

# use format: python3 module_top_tests.py <bool> <bool> <int> <int>
def main():
    write_to_csv = sys.argv[1] == 'True' or sys.argv[1] == 'true'
    ignore_empty = sys.argv[2] == 'True' or sys.argv[2] == 'true'
    limit = int(sys.argv[3])
    test_time_window = int(sys.argv[4])

    unit_test_modules = read_file(UNIT_TEST_REGEX, {})
    functional_test_modules = read_file(FUNCTIONAL_TEST_REGEX, {})

    if write_to_csv == True:
        file_to_write = 'modules_top_tests.csv'
        with open(file_to_write, 'w', newline='') as csvfile:
            fieldnames = ['Module', 'Test', 'Time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            write_to_csv_file(unit_test_modules, writer, limit, test_time_window, ignore_empty)
            writer.writerow({}) # add new line to separate unit and functional tests
            write_to_csv_file(functional_test_modules, writer, limit, test_time_window, ignore_empty)
            print(f"written to file: {file_to_write}")
    else:
        print('Unit Tests')
        print_modules(unit_test_modules, limit, test_time_window, ignore_empty)
        print('Functional Tests')
        print_modules(functional_test_modules, limit, test_time_window, ignore_empty)

def read_file(log_regex, modules):
    cur_module = None
    files = [f for f in listdir() if log_regex.match(f)]
    for file in files:
        f = open(file, 'r')
        lines = f.readlines()
        for l in lines:
            if BUILDING_MODULE_REGEX.match(l):
                cur_module = BUILDING_MODULE_REGEX.search(l).group(2)
                modules[cur_module] = (0.0, [])
            elif TESTS_RUN_REGEX.match(l):
                if cur_module is None:
                    raise TypeError("cur_module is None")
                regex = TESTS_RUN_REGEX.search(l)
                tmp_time = convert_to_time(regex.group(2))
                tmp_tuple = (regex.group(3), tmp_time)
                modules[cur_module][1].append(tmp_tuple)
                modules[cur_module] = (modules[cur_module][0] + tmp_time, modules[cur_module][1])
    for module in modules:
        modules[module] = (modules[module][0], sorted(modules[module][1], key= lambda x: x[1], reverse=True))
    return modules

def write_to_csv_file(test_modules, csv_writer, limit, time_window, ignore_empty):
    for i in sorted(test_modules, key= lambda x: test_modules[x][0], reverse=True):
        if len(test_modules[i][1]) == 0 and ignore_empty:
            continue
        total_time = str(round(test_modules[i][0] / 60, 4)) + 'm' if test_modules[i][0] > 60 else str(round(test_modules[i][0], 4)) + 's'
        csv_writer.writerow({'Module': i, 'Test': '', 'Time': total_time})
        count = 0
        for j in test_modules[i][1]:
            if count >= limit and j[1] < time_window * 60:
                break
            time = str(round(j[1] / 60, 4)) + 'm' if j[1] > 60 else str(round(j[1], 4)) + 's'
            csv_writer.writerow({'Module': '', 'Test': j[0], 'Time': time})
            count += 1

def print_modules(test_modules, limit, time_window, ignore_empty):
    for i in sorted(test_modules, key= lambda x: test_modules[x][0], reverse=True):
        if len(test_modules[i][1]) == 0 and ignore_empty:
            continue
        total_time = str(round(test_modules[i][0] / 60, 4)) + 'm' if test_modules[i][0] > 60 else str(round(test_modules[i][0], 4)) + 's'
        print(i, total_time)
        count = 0
        for j in test_modules[i][1]:
            if count >= limit and j[1] < time_window * 60:
                break
            time = str(round(j[1] / 60, 4)) + 'm' if j[1] > 60 else str(round(j[1], 4)) + 's'
            print('   ',j[0], time)
            count += 1
    print()

def convert_to_time(time_str):
    split_time = time_str.split()
    time, unit = float(split_time[0].replace(',', '')), split_time[1]
    if unit == 'm':
        return time * 60
    if unit == 'h':
        return time * 60 * 60
    return time

if __name__ == "__main__":
    main()