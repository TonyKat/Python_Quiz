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


def is_square(m):
    return all(len(row) == len(m) for row in m)


def task1(array, n):
    try:
        print('n = {}\nОригинальный массив: \n{}'.format(n, array))
        if n <= 0 or type(n) != int:
            raise ValueError('n должно быть положительным и целочисленным')

        if type(array[0]) == list or type(array[0]) == np.ndarray:
            if not is_square(array):
                raise ValueError('Двумерный список не квадратный!')

        array = np.array(array)
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
    array = np.arange(11)
    for n in [3, 5, 10, -1, 0]:
        task1(array, n)

    print('-----------Двумерный массив---------------')
    sizes = [3, 5, 11]
    arrays = [np.arange(x ** 2) for x in sizes]
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
    for i in range(0, len(path_to_json_files), 2):
        task2(path_to_json_files[i], path_to_json_files[i + 1])
    print('-------------task2_end-------------------')
    return


def task2(first_json, second_json):
    json_files = []
    try:
        for json_file in [first_json, second_json]:
            with open(json_file, 'r', encoding='utf-8') as file:
                json_files.append(json.load(file))
    except json.JSONDecodeError as e:
        print(e, 'Ошибка при чтении JSON')
        return
    except FileNotFoundError as e:
        print(e, 'Не найден файл')
        return

    difference = {key: json_files[0][key] for key in json_files[0] if key in json_files[1]
                  and json_files[0][key] != json_files[1][key]}

    if difference:
        for key, value in difference.items():
            print('Различия между файлами JSON:')
            print("'{}': {}\n'{}': {}".format(key, value, key, json_files[1][key]))
    else:
        print('Нет различий или одинаковых ключей')
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
        for chunk in chunk_names:
            try:
                os.remove(path_to_chunks + '\\' + chunk)
            except OSError as e:
                print(e, 'Ошибка удаления файлов')
        try:
            os.rmdir(path_to_chunks)
        except OSError as e:
            print(e, 'Ошибка удаления директории!')
        print('Файл успешно создан и отсортирован!')
    except FileNotFoundError as e:
        print(e, 'Не найден файл')
        return
    except Exception as e:
        print(e, 'Ошибка записи в кусочный файл/полный файл')
    return


if __name__ == '__main__':
    working_directory = os.getcwd()
    test_task1()
    test_task2()
    test_task3()
