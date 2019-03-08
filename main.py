import os
import json
import numpy as np
from time import time
from functools import wraps
from heapq import merge
from itertools import count, islice
from contextlib import ExitStack


def func_time(func):
    """
    Декоратор, который сообщает время выполнения.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        print('Время выполнения "{}": {}\n'.format(func.__name__, end - start))
        return result

    return wrapper


def task1(array, n):
    try:
        print('n = {}\nОригинальный массив: \n{}'.format(n, array))
        if n <= 0:
            raise ValueError('n должно быть положительным ')

        if array.dtype == int or float:
            if len(array.shape) > 1:
                if array.shape[0] < n and array.shape[1] < n:
                    raise ValueError('Число строк и число столбцов меньше n')
                array = array[::n, ::n]
            else:
                if len(array) < n:
                    raise ValueError('Длина массива меньше n')
                array = array[::n]
        else:
            print('Некорректные данные')

        print('Обработанный массив:\n{}\n'.format(array))
    except ValueError as e:
        print('Ошибка:', e, '\n')

    return array


@func_time
def test_task1():
    print('-------------task1_begin-----------------')
    print('-----------Одномерный массив-------------')
    # одномерный массив
    array = np.arange(11)
    for n in [3, 5, 10, -1, 0]:
        task1(array, n)

    print('-----------Двумерный массив---------------')
    # двумерный массив
    sizes = [3, 5, 11]
    # создать одномерные массивы
    arrays = [np.arange(x ** 2) for x in sizes]
    # преобразовать в двумерные массивы
    arrays = [array.reshape(size, size) for array, size in zip(arrays, sizes)]

    for array in arrays:
        for n in [2, 4, 10]:
            task1(array, n)
    print('-------------task1_end-------------------')
    return


@func_time
def test_task2():
    print('-------------task2_begin-----------------')
    path_to_json_files = [mydir for mydir in os.listdir(working_directory)
                          if mydir.startswith('task2_jsons') and os.path.isdir(mydir)][0]
    path_to_json_files = [os.path.join(path_to_json_files, file) for file in os.listdir(path_to_json_files)
                          if file.endswith('.json')]
    json_files = []
    try:
        for json_file in path_to_json_files:
            with open(json_file, 'r', encoding='utf-8') as file:
                json_files.append(json.load(file))
    except json.JSONDecodeError as e:
        print(e, 'Ошибка при чтении JSON')

    for i in range(0, len(json_files), 2):
        difference = task2(json_files[i], json_files[i + 1])
        if difference:
            for key, value in difference.items():
                print('Различия между файлами JSON:')
                print("'{}': {}\n'{}': {}".format(key, value, key, json_files[i + 1][key]))
            print()
        else:
            print('Нет разницы')
    print('-------------task2_end-------------------')
    return


def task2(first_json, second_json):
    difference = {key: first_json[key] for key in first_json if key in second_json
                  and first_json[key] != second_json[key]}
    return difference


@func_time
def test_task3():
    print('-------------task3_begin-----------------')
    path_to_txt = [mydir for mydir in os.listdir(working_directory)
                   if mydir.startswith('task3_txt') and os.path.isdir(mydir)][0]
    path_to_txt = [os.path.join(path_to_txt, file) for file in os.listdir(path_to_txt)
                   if file.endswith('.txt')]
    task3(*path_to_txt)
    print('-------------task3_end-------------------')
    return


def task3(first_file, second_file):
    try:
        os.mkdir('chunks')
    except OSError as e:
        print(e, 'Директория существует!\n')

    path_to_chunks = [mydir for mydir in os.listdir(working_directory)
                      if mydir.startswith('chunks') and os.path.isdir(mydir)][0]
    chunk_names = []

    try:
        for idx, filename in enumerate([first_file, second_file]):
            with open(filename, encoding='utf-8') as input_file:
                for chunk_number in count(1):
                    sorted_chunk = sorted(islice(input_file, 50000))
                    if not sorted_chunk:
                        break
                    chunk_name = 'chunk_{}_{}.txt'.format(idx, chunk_number)
                    chunk_names.append(chunk_name)
                    with open(path_to_chunks + '\\' + chunk_name, 'w', encoding='utf-8') as chunk_file:
                        chunk_file.writelines(sorted_chunk)
        with ExitStack() as stack, open('output.txt', 'w', encoding='utf-8') as output_file:
            files = [stack.enter_context(open(path_to_chunks + '\\' + chunk, encoding='utf-8')) for chunk in
                     chunk_names]
            output_file.writelines(merge(*files))
    except Exception as e:
        print(e, 'Ошибка записи в кусочный файл/полный файл')
    finally:
        for chunk in chunk_names:
            try:
                os.remove(path_to_chunks + '\\' + chunk)
            except Exception as e:
                print(e, 'Ошибка удаления файлов')
        try:
            os.rmdir(path_to_chunks)
        except OSError as e:
            print(e, 'Ошибка удаления директории!')
        print('Файл успешно создан и отсортирован!')
    return


if __name__ == '__main__':
    working_directory = os.getcwd()
    test_task1()
    test_task2()
    test_task3()
