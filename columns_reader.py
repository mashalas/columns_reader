#!/usr/bin/python3

import csv
from collections import Counter
from statistics import mean
from pprint import pprint
import datetime
import math

ON_UNDEFINED__KEEP = 'keep'
ON_UNDEFINED__SKIP_ROW = 'skip_row'
ON_UNDEFINED__BREAK = 'break'
ON_UNDEFINED__EXCEPTION = 'exception'

ON_ERROR__SET_NONE = 'set_none'
ON_ERROR__DELETE_ROW = 'delete_row'
ON_ERROR__EXCEPTION = 'exception'
ON_ERROR__DISCARD_CHANGES = 'discard_changes'

TRANSFORMATION_KIND__LOWER = 'lower'
TRANSFORMATION_KIND__UPPER = 'lower'
TRANSFORMATION_KIND__CAPITALIZE = 'capitalize'
TRANSFORMATION_KIND__STRIP = 'strip'
TRANSFORMATION_KIND__LSTRIP = 'lstrip'
TRANSFORMATION_KIND__RSTRIP = 'rstrip'
TRANSFORMATION_KIND__SET_LENGTH = 'set_length'
TRANSFORMATION_KIND__REPLACE = 'replace'
TRANSFORMATION_KIND__LOWER_LIMIT = 'lower_limit'
TRANSFORMATION_KIND__UPPER_LIMIT = 'upper_limit'
TRANSFORMATION_KIND__MULTIPLY = 'multiply'
TRANSFORMATION_KIND__DIVIDE = 'divide'
TRANSFORMATION_KIND__ADD = 'add'
TRANSFORMATION_KIND__SUBSTRACT = 'substract'
TRANSFORMATION_KIND__NORMALIZE = 'normalize'
TRANSFORMATION_KIND__STANDARDIZE = 'standardize'
TRANSFORMATION_KIND__ABS = 'abs'
TRANSFORMATION_KIND__SQRT = 'sqrt'
TRANSFORMATION_KIND__POWER = 'power'

TRANSFORMATION_KIND__TO_FLOAT = 'to_float'
TRANSFORMATION_KIND__TO_INTEGER = 'to_integer'
TRANSFORMATION_KIND__TO_STRING = 'to_string'
TRANSFORMATION_KIND__TO_BOOLEAN = 'to_boolean'
TRANSFORMATION_KIND__TO_DATE_TIME = 'to_date_time'

DIRECTION__VER = 'v'
DIRECTION__HOR = 'h'

DATA_TYPE__UNKNOWN = 'unknown'
DATA_TYPE__STRING = 'string'
DATA_TYPE__INTEGER = 'integer'
DATA_TYPE__FLOAT = 'float'
DATA_TYPE__DATETIME = 'date_time'
DATA_TYPE__BOOLEAN = 'boolean'

TRUE_VALUES  = ['1', '+', 'yes', 'on']
FALSE_VALUES = ['0', '-', 'no',  'off']

DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


def dump_vector(
        filename,
        data,
        caption = None,
        mode = 'wt',
        direction = DIRECTION__VER,
        column_number_of_matrix = None,
        since:int = 0,
        until = -1,
        sep = ','
):
    if until < 0:
        until = len(data)
    with open(filename, mode) as f:
        if not caption is None:
            f.write(caption)
            if direction == DIRECTION__VER:
                f.write('\n')
            elif direction == DIRECTION__HOR:
                f.write(sep)
        for i in range(since, until):
            if column_number_of_matrix is None:
                # передан вектор, а не 2-мерная матрица
                f.write(str(data[i]))
            else:
                # передана 2-мерная матрица, выгрузить определённый столбец
                f.write(str(data[i][column_number_of_matrix]))
            if direction == DIRECTION__VER:
                f.write('\n')
            elif direction == DIRECTION__HOR:
                if i < until-1:
                    f.write(sep)
                else:
                    f.write('\n')
            

def dump_matrix(
        filename,
        data,
        header = [],
        mode = 'wt',
        since:int = 0,
        until = -1,
        sep = ','
):
    if until < 0:
        until = len(data)
    with open(filename, mode) as f:
        if not header is None and len(header) > 0:
            for j in range(len(header)):
                f.write(header[j])
                if j < len(header)-1:
                    f.write(sep)
            f.write('\n')
        for i in range(since, until):
            for j in range(len(data[i])):
                f.write(str(data[i][j]))
                if j < len(header)-1:
                    f.write(sep)
            f.write('\n')


# ----- Нормализация к диапазону -----
def normalize(x, x_min, x_max, target_min, target_max):
    return target_min + (x - x_min) / (x_max - x_min) * (target_max - target_min)


# ----- Является ли параметр массивом (список, кортёж) -----
def is_array(x) -> bool:
    if type(x) == type([]):
        return True
    if type(x) == type(()):
        return True
    return False

# ----- Является ли параметр скаляром (не список, не кортёж) -----
def is_scalar(x) -> bool:
    if type(x) == type([]):
        return False
    if type(x) == type(()):
        return False
    return True


# ------- Попытаться преобразовать строку в дату/время; вернуть дату или None, если конвертирование не удалось -------
def str_to_date_time(s):
    #                     123456
    # dd.mm.yyyy hh:mm:ss.micros
    # yyyy-mm-dd hh:mm:ss.micros
    # 01234567890123456789012345
    if len(s) < 10:
        # слишком мало символов, чтобы быть датой
        return None
    year = None
    month = None
    day = None
    if (
        s[0].isdigit() and
        s[1].isdigit() and
        not(s[2].isdigit()) and
        s[3].isdigit() and
        s[4].isdigit() and
        not(s[5].isdigit()) and
        s[6].isdigit() and
        s[7].isdigit() and
        s[8].isdigit() and
        s[9].isdigit() and
        1 == 1
    ) :
        # dd.mm.yyyy hh:mm:ss
        # 0123456789012345678
        try:
            day = int(s[0:2])
            month = int(s[3:5])
            year = int(s[6:10])
        except:
            return None
    elif (
        s[0].isdigit() and
        s[1].isdigit() and
        s[2].isdigit() and
        s[3].isdigit() and
        not(s[4].isdigit()) and
        s[5].isdigit() and
        s[6].isdigit() and
        not(s[7].isdigit()) and
        s[8].isdigit() and
        s[9].isdigit() and
        1 == 1
    ) :
        # yyyy-mm-dd hh:mm:ss
        # 0123456789012345678
        try:
            year = int(s[0:4])
            month = int(s[5:7])
            day = int(s[8:10])
        except:
            return None
    else:
        return None
    hour = 0
    minute = 0
    second = 0
    micros = 0
    # dd.mm.yyyy hh:mm:ss
    # 0123456789012345678
    if len(s) >= 13:
        try:
            hour = int(s[11:13])
        except:
            return None
    if len(s) >= 16:
        try:
            minute = int(s[14:16])
        except:
            return None
    if len(s) >= 19:
        try:
            second = int(s[17:19])
        except:
            return None
        
    if len(s) >= 21 and not s[19].isdigit() :
        # микросекунды
        i = 20
        micros_str = ""
        while i < len(s) and i < 26:
            if s[i].isdigit():
                micros_str += s[i]
                i += 1

        n1 = len(micros_str)
        #print("micros_str:", micros_str, "  n1:", n1)
        # удалить ведущие нули
        leading_zeros_count = 0
        while len(micros_str) > 0 and micros_str[0] == "0":
            leading_zeros_count += 1
            micros_str = micros_str[1:len(micros_str)]
                
        try:
            micros = int(micros_str)
            #micros = float("0." + micros_str)
            n2 = 6 - n1
            for i in range(n2):
                micros *= 10
            #print("micros:", micros)
        except:
            return None

    #print(year, month, day, hour, minute, second)
    try:
        result = datetime.datetime(year, month, day, hour, minute, second, micros)
    except:
        return None
    return result

#print(str_to_date_time('2026-09_27T12:15:59.123')); exit(0)

class ColumnsReader():

    def __init__(self) -> None:
        self.last_file_header = [] # названия всех столбцов из последнего прочитанного файла
        self.header = [] # названия столбцов, которые читались из файла
        self.data = [] # данные столбцов, которые читались из файла
        self.skipped_count = 0 # сколько строк файла не удалось прочитать
        self.last_error = None


    def get_skipped_count(self):
        return self.skipped_count
    

    def get_column_index(self, column, header):
        column_index = None
        if type(column) == type(123):
            # указан номер столбца
            column_index = column
            if column_index < 0:
                # указан номер столбца с конца
                column_index = len(header) + column_index
        elif type(column) == type("abc"):
            # искать по имени столбца с учётом регистра
            for j in range(len(header)):
                if column == header[j]:
                    column_index = j
                    break
            if column_index is None:
                # искать по имени столбца без учёта регистра (т.к. с учётом регистра не нашли)
                for j in range(len(header)):
                    column__lower = column.lower()
                    for j in range(len(header)):
                        if column__lower == header[j].lower():
                            column_index = j
                            break
        return column_index


    def get_columns_indexes(self, columns, header):
        #if type(columns) != type([]) and type(columns) != type(()):
        if is_scalar(columns):
            columns = [columns] # если передан скаляр - преобразовать его в массив
        if len(columns) == 0:
            # если столбцы не укзаны - подразумеваются все столбцы
            columns_indexes = [j for j in range(len(header))]
        else:
            columns_indexes = []
            for c in columns:
                ci = self.get_column_index(c, header)
                columns_indexes.append(ci)
        return columns_indexes


    def read(
            self,
            filename,
            columns = [],
            clear_data:bool = False,
            csv__delimiter:str = ',',
            csv__newline:str = '',
            csv__quotechar:str = '"',
            csv__escapechar:str = None,
            csv__encoding:str = 'utf-8',
            rows_max_count__total:int = 0,
            rows_max_count__file:int = 0,
            read_header_only:bool = False,  # прочитать и вернуть только заголовок файла
            undefined_values = [''], # эти значения заменяются на None, если on_undefined_action == ON_UNDEFINED__KEEP
            on_undefined_action = ON_UNDEFINED__KEEP
    ):
        with open(
            filename,
            newline=csv__newline,
            encoding=csv__encoding
        ) as f:
            reader = csv.reader(
                f,
                delimiter=csv__delimiter,
                quotechar=csv__quotechar,
                escapechar=csv__escapechar
            ) # есть ещё параметры в csv.reader
            self.header.clear()
            self.last_file_header.clear()
            if clear_data:
                self.data.clear()
                self.skipped_count = 0
            file_row_number = 0
            appended_rows_count = 0
            for row in reader:
                #print(row)
                file_row_number += 1
                if len(row) == 0:
                    # пустая строка
                    continue
                if len(self.last_file_header) == 0:
                    self.last_file_header = row.copy()
                    if read_header_only:
                        return self.last_file_header
                    columns_indexes = self.get_columns_indexes(columns, self.last_file_header)
                    for ci in range(len(columns_indexes)):
                        self.header.append( self.last_file_header[ci] )
                    continue
                if len(row) < len(self.last_file_header):
                    # количество столбцов в строке данных меньше количества столбцов в заголовке
                    continue
                data_row = []
                for ci in columns_indexes:
                    x = row[ci] 
                    if x == '' or x in undefined_values:
                        # значение не определено
                        if on_undefined_action == ON_UNDEFINED__EXCEPTION:
                            msg = 'At line #{} column "{}" contains undefined value "{}"' . format(file_row_number, self.header[ci], x)
                            raise Exception(msg)
                        elif on_undefined_action == ON_UNDEFINED__BREAK:
                            self.last_error = 'At line #{} column "{}" contains undefined value "{}"' . format(file_row_number, self.header[ci], x)
                            return False
                        elif on_undefined_action == ON_UNDEFINED__SKIP_ROW:
                            data_row.clear()
                            self.skipped_count += 1
                            break
                        elif on_undefined_action == ON_UNDEFINED__KEEP:
                            x = None
                    data_row.append(x)
                if len(data_row) == 0:
                    # при игнорировании строк с неопределёнными значениями очищается строка данных
                    continue
                self.data.append(data_row)
                appended_rows_count += 1
                if rows_max_count__total > 0 and len(self.data) >= rows_max_count__total:
                    break
                if rows_max_count__file > 0 and appended_rows_count >= rows_max_count__file:
                    break
        return True


    # ----- Удалить столбец (можно указать несколько столбцов) -----
    def drop_column(self, column):
        if is_array(column):
            for c in column:
                self.drop_column(c)
            return
        ci = self.get_column_index(column, self.header)
        for i in range(len(self.data)):
            del self.data[i][ci]
        del self.header[ci]


    # ----- Добавить новый столбец данных, значения переданы в items[], их количество должно совпадать с количеством уже имеющихся строк с данными -----
    def add_column(self, name, items):
        if name in self.header:
            msg = 'Column "{}" already exists!' . format(name)
            raise Exception(msg)
        items_count = len(items)
        data_rows_count = len(self.data)
        if items_count != data_rows_count:
            msg = 'Data contains {} rows but new column "{}" contains {} rows' . format(data_rows_count, name, items_count)
            raise Exception(msg)
        self.header.append(name)
        #j = len(self.header) - 1
        for i in range(len(self.data)):
            self.data[i].append(items[i])


    # ----- Удалить строку с указанным номером (отсчёт начинается с 0) -----
    # (нельзя указать несколько номеров строк, т.к. после удаления одной строки, для других изменится нумерация)
    def delete_one_row(self, row_number):
        del self.data[row_number]


    # ----- Удалить несколько строк с указанными номерами (отсчёт начинается с 0) -----
    def delete_several_rows(self, rows_numbers):
        tmp = []
        for i in range(len(self.data)):
            if i not in rows_numbers:
                tmp.append(self.data[i])
        self.data = tmp


    # ----- Обновить значение в таблице данных для указанного столбца и номера строки (можно указать несколько номеров строк) -----
    def update(self, column, row_number, new_data):
        if is_array(row_number):
            for rn in row_number:
                self.update(column, rn, new_data)
            return
        column_index = self.get_column_index(column, self.header)
        self.data[row_number][column_index] = new_data


    # ----- Количество строк с данными -----
    def get_rows_count(self):
        return len(self.data)
        

    def delete_rows_with_empty(self, empty_values = []):
        removing_rows = []
        columns_count = len(self.data[0])
        for i in range(len(self.data)):
            for j in range(columns_count):
                if self.data[i][j] is None:
                    removing_rows.append(i)
                    break
                if empty_values is None or len(empty_values) == 0:
                    # не указан список "пустых" значений - проверять с пустой строкой
                    if self.data[i][j] == '':
                        removing_rows.append(i)
                        break
                else:
                    # указан список "пустых" значений - сверять на пустоту только с ним
                    if self.data[i][j] in empty_values:
                        removing_rows.append(i)
                        break
        if len(removing_rows) > 0:
            self.delete_several_rows(removing_rows)


    # ----- Получить в виде вектора значения указанного по имени или номеру столбца ----
    # ----- когда keep_undefined - добавлять в список значение None, если false - не добавлять их -----
    def get_column(self, column, keep_undefined = True):
        column_index = self.get_column_index(column, self.header)
        items = []
        for i in range(len(self.data)):
            if keep_undefined or self.data[i][column_index] is not None:
                items.append(self.data[i][column_index])
        return items
    

    def _transform_column(
            self,
            transformation_kind,
            column_index,
            on_error,
            instead_of_undefined,
            transformation_param1,
            transformation_param2,
            transformation_param3,
            transformation_param4
    ):
        self.last_error = None
        j = column_index
        ci = column_index
        backup = []
        for i in range(len(self.data)):
            x = self.data[i][j]
            backup.append(x)
            if x is None:
                # если текущее значение не определено и указано на что замениять неопределённые значения
                if instead_of_undefined is not None:
                    x = instead_of_undefined
            elif transformation_kind == TRANSFORMATION_KIND__UPPER:
                # в верхний регистр
                x = x.upper()
            elif transformation_kind == TRANSFORMATION_KIND__LOWER:
                # в нижний регистр
                x = x.lower()
            elif transformation_kind == TRANSFORMATION_KIND__CAPITALIZE:
                # первая буква - заглавная
                x = x.capitalize()
            elif transformation_kind == TRANSFORMATION_KIND__STRIP:
                # удалить пробелы в начале и конце
                x = x.strip()
            elif transformation_kind == TRANSFORMATION_KIND__LSTRIP:
                # удалить пробелы в начале
                x = x.strip()
            elif transformation_kind == TRANSFORMATION_KIND__RSTRIP:
                # удалить пробелы в конце
                x = x.strip()
            elif transformation_kind == TRANSFORMATION_KIND__SET_LENGTH:
                # если строка длиннее transformation_param1 - укоротить до transformation_param1
                # если строка короче transformation_param1 - дополнить до transformation_param1
                # при дополнении до заданной длины transformation_param2 добавляется в начале , transformation_param3 добавляется в конце
                x = x[0:transformation_param1]
                while x < transformation_param1:
                    if transformation_param2 is not None and transformation_param2 != '':
                        x = transformation_param2 + x
                    elif transformation_param3 is not None and transformation_param3 != '':
                        x += transformation_param3
                    else:
                        break
            elif transformation_kind == TRANSFORMATION_KIND__REPLACE:
                # замена подстроки
                if transformation_param3 is None:
                    # выполнить замену для всех совпадений
                    x = x.replace(transformation_param1, transformation_param2)
                else:
                    # указано максимальное количество замен
                    x = x.replace(transformation_param1, transformation_param2, transformation_param3)
            elif transformation_kind == TRANSFORMATION_KIND__LOWER_LIMIT:
                # если числовое значение меньше указанного предела в transformation_param1 - заменить на transformation_param1
                if x < transformation_param1:
                    x = transformation_param1
            elif transformation_kind == TRANSFORMATION_KIND__UPPER_LIMIT:
                # если числовое значение больше указанного предела в transformation_param1 - заменить на transformation_param1
                if x > transformation_param1:
                    x = transformation_param1
            elif transformation_kind == TRANSFORMATION_KIND__MULTIPLY:
                # умножить на transformation_param1
                x = x * transformation_param1
            elif transformation_kind == TRANSFORMATION_KIND__DIVIDE:
                # разделить на transformation_param1
                x = x / transformation_param1
            elif transformation_kind == TRANSFORMATION_KIND__ADD:
                # сложить с transformation_param1
                x = x + transformation_param1
            elif transformation_kind == TRANSFORMATION_KIND__SUBSTRACT:
                # вычесть transformation_param1
                x = x - transformation_param1
            elif transformation_kind == TRANSFORMATION_KIND__NORMALIZE:
                # нормализация к диапазону
                x = normalize(x, transformation_param1, transformation_param2, transformation_param3, transformation_param4)
            elif transformation_kind == TRANSFORMATION_KIND__STANDARDIZE:
                # нормализация стандартным отклонением (xi - x_avg) / stddev
                x = (x - transformation_param1) / transformation_param2
            elif transformation_kind == TRANSFORMATION_KIND__ABS:
                # модуль числа
                if x < 0:
                    x = -x
            elif transformation_kind == TRANSFORMATION_KIND__SQRT:
                # квадратный корень
                x = math.sqrt(x)
            elif transformation_kind == TRANSFORMATION_KIND__POWER:
                # возведение в степень
                x = x ** transformation_param1                
            elif transformation_kind == TRANSFORMATION_KIND__TO_FLOAT:
                try:
                    x_flt = float(x)
                    x = x_flt
                except:
                    if on_error == ON_ERROR__SET_NONE:
                        x = None
                    elif on_error == ON_ERROR__EXCEPTION:
                        raise Exception('Cannot convert "{}" to float on line #{}' . format(x, i))
                    elif on_error == ON_ERROR__SET_NONE:
                        x = None
                    elif on_error == ON_ERROR__DISCARD_CHANGES:
                        for i in range(len(backup)):
                            self.data[i][ci] = backup[i]
                        self.last_error = 'Cannot convert "{}" to float on line #{}' . format(x, i)
                        return False
            elif transformation_kind == TRANSFORMATION_KIND__TO_INTEGER:
                try:
                    x_int = int(x)
                    x = x_int
                except:
                    if on_error == ON_ERROR__SET_NONE:
                        x = None
                    elif on_error == ON_ERROR__EXCEPTION:
                        raise Exception('Cannot convert "{}" to integer on line #{}' . format(x, i))
                    elif on_error == ON_ERROR__SET_NONE:
                        x = None
                    elif on_error == ON_ERROR__DISCARD_CHANGES:
                        for i in range(len(backup)):
                            self.data[i][ci] = backup[i]
                        self.last_error = 'Cannot convert "{}" to integer on line #{}' . format(x, i)
                        return False
            elif transformation_kind == TRANSFORMATION_KIND__TO_STRING:
                x = str(x)
            elif transformation_kind == TRANSFORMATION_KIND__TO_BOOLEAN:
                x = x.lower()
                if x in TRUE_VALUES:
                    x = True
                elif x in FALSE_VALUES:
                    x = False
                else:
                    x = None
            elif transformation_kind == TRANSFORMATION_KIND__TO_DATE_TIME:
                try:
                    if transformation_param1 is None:
                        # формат даты не задан, попытаться сконвертировать автоматически
                        x_dt = str_to_date_time(x)
                        if x_dt is not None:
                            x = x_dt
                        else:
                            raise Exception('Cannot convert "{}" to date/time' . format(x))
                    else:
                        # явно заданный формат даты
                        x_dt = datetime.datetime.strptime(x, transformation_param1)
                        x = x_dt
                except:
                    if on_error == ON_ERROR__SET_NONE:
                        x = None
                    elif on_error == ON_ERROR__EXCEPTION:
                        raise Exception('Cannot convert "{}" to date/time on line #{}' . format(x, i))
                    elif on_error == ON_ERROR__SET_NONE:
                        x = None
                    elif on_error == ON_ERROR__DISCARD_CHANGES:
                        for i in range(len(backup)):
                            self.data[i][ci] = backup[i]
                        self.last_error = 'Cannot convert "{}" to date/time on line #{}' . format(x, i)
                        return False
            if x is None:
                if instead_of_undefined is not None:
                    x = instead_of_undefined
            self.data[i][ci] = x
        return True

    def transform(
            self,
            transformation_kind,
            columns = [],
            transformation_param1 = None,
            transformation_param2 = None,
            transformation_param3 = None,
            transformation_param4 = None,
            on_error = ON_ERROR__EXCEPTION,
            instead_of_undefined = None,
        ):
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for ci in columns_indexes:
            ok = self._transform_column(
                transformation_kind,
                ci,
                on_error,
                instead_of_undefined,
                transformation_param1,
                transformation_param2,
                transformation_param3,
                transformation_param4
            )
            if not ok:
                return False
        return True


    def undefined_to_something(self, something, columns = []):
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for ci in columns_indexes:
            for i in range(len(self.data)):
                if self.data[i][ci] is None:
                    self.data[i][ci] = something


    def undefined_to_median(self, columns = []):
        columns_indexes = self.get_columns_indexes(columns, self.header)
        items = []
        for ci in columns_indexes:
            undefined_count = 0
            items.clear()
            for i in range(len(self.data)):
                if self.data[i][ci] is None:
                    undefined_count += 1
                else:
                    items.append(self.data[i][ci])
            if undefined_count > 0:
                # имеются неопределённые значения
                items.sort()
                p = int(len(items) / 2)
                for i in range(len(self.data)):
                    if self.data[i][ci] is None:
                        self.data[i][ci] = items[p]


    def one_hot_encoding(
            self,
            column, # имя или номер столбца на основе которого сформировать OneHotEncoding-столбцы
            classes = {},   # словарь, в котором ключ - имя нового ohe-столбца, а значение - список значений таблицы данных
                            # соответствующих этому столбцу
                            # если не задан - автоматически определить ohe-столбцы
            when_matched = 1.0,
            when_missed = 0.0,
            keep_src_column:bool = False    # сохранять ли исходный столбец, на основе которого были получены новые ohe-столбцы
        ):

        ci = self.get_column_index(column, self.header)

        if len(classes.keys()) == 0:
            # классы не заданы - автоматически определить классы на основе имеющихся в столбце значений
            src_column_name = self.header[ci]
            items_uniq = self.get_unique_values(ci)
            items_uniq.sort()
            for x in items_uniq:
                class_name = src_column_name + '=' + x
                classes[class_name] = [x]
            #pprint(classes); exit(0)

        # если не для всех классов значения заданы в виде списка - сконвертировать скаляр в список
        #for j in range(len(classes)):
        for key in classes.keys():
            values = classes[key]
            if is_scalar(values):
            #if type(values) != type([]) and type(values) != type(()):
                classes[key] = [values]

        # составить список имён новых классов
        classes_list = list(classes.keys())
        classes_list.sort()

        for j in range(len(classes_list)):
            class_name = classes_list[j]
            self.header.append(class_name)
            for i in range(len(self.data)):
                x = self.data[i][ci]
                if x in classes[class_name]:
                    self.data[i].append(when_matched)
                else:
                    self.data[i].append(when_missed)

        if not keep_src_column:
            self.drop_column(ci)


    def get_stat(self, columns = [], unique_max_count = 10):
        stat = {}
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for ci in columns_indexes:
            stat[self.header[ci]] = {}
            items = self.get_column(ci)
            #if type(self.data[0][ci]) == type(123.456):
            if type(items[0]) == type(123.456):
                # дробные числа
                stat[self.header[ci]] = {}
                stat[self.header[ci]]["min"] = min(items)
                stat[self.header[ci]]["max"] = max(items)
                stat[self.header[ci]]["mean"] = mean(items)
            elif type(items[0]) == type("abc"):
                # строки
                items_uniq = list(set(items))
                items_uniq.sort()
                if unique_max_count > 0:
                    items_uniq = items_uniq[0:unique_max_count]
                stat[self.header[ci]]["uniq"] = items_uniq
                #for x in items_uniq:
                stat[self.header[ci]]["counting"] = Counter(items)
        return stat

    
    def get_header(self):
        return self.header
    

    def get_data(self):
        return self.data
    

    def copy(self, columns = [], since_row = 0, until_row = -1):
        csr = ColumnsReader()
        columns_indexes = self.get_columns_indexes(columns, self.header)
        if until_row < 0:
            until_row = len(self.data)
        for ci in columns_indexes:
            csr.header.append(self.header[ci])
        for i in range(since_row, until_row):
            data_row = []
            for ci in columns_indexes:
                data_row.append(self.data[i][ci])
            csr.data.append(data_row)
        return csr


    def copy_header(self, columns = []):
        header = []
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for ci in columns_indexes:
            header.append(self.header[ci])
        return header


    def copy_data(self, columns = [], since_row = 0, until_row = -1, keep_columns_if_vector:bool = False):
        data = []
        columns_indexes = self.get_columns_indexes(columns, self.header)
        copy_as_vector = len(columns) == 1 and keep_columns_if_vector == False
        if until_row < 0:
            until_row = len(self.data)
        for i in range(since_row, until_row):
            data_row = []
            for ci in columns_indexes:
                data_row.append(self.data[i][ci])
            if copy_as_vector:
                # вектор из одного столбца
                data.append(data_row[0])
            else:
                # двумерная матрица, даже если из одного столбца
                data.append(data_row)
        return data


    def print(self, columns = [], since_row = 0, until_row = -1):
        columns_indexes = self.get_columns_indexes(columns, self.header)
        if until_row < 0:
            until_row = len(self.data)

        # --- header ---
        print('RowNum', end='')
        for ci in columns_indexes:
            print('\t' + self.header[ci], end='')
        print('')

        # --- data ---
        for i in range(since_row, until_row):
            print(str(i) + ')', end='')
            for ci in columns_indexes:
                print('\t' + str(self.data[i][ci]), end='')
            print('')

    # ----- Получить список уникальных значений столбца -----
    def get_unique_values(self, column, ignore_case:bool = False):
        j = self.get_column_index(column, self.header)
        values_set = set()
        for i in range(len(self.data)):
            x = self.data[i][j]
            if ignore_case:
                x = x.lower()
            if x not in values_set:
                values_set.add(x)
        values_list = list(values_set)
        values_list.sort()
        return values_list
    
    # ----- Получить словарь, в котором ключ - уникальное значение столбца, а значение - сколько раз это значание повторяется в таблице -----
    def count_unique_values(self, column, ignore_case:bool = False):
        j = self.get_column_index(column, self.header)
        if j is None:
            # не удалось определить номер столбца
            return None
        values = {}
        for i in range(len(self.data)):
            x = self.data[i][j]
            if ignore_case:
                x = x.lower()
            if x in values:
                values[x] += 1
            else:
                values[x] = 1
        return values
    
    # ----- Получить минимальное значение столбца -----
    def get_min(self, column):
        items = self.get_column(column, False)
        return min(items)


    # ----- Получить максимальное значение столбца -----
    def get_max(self, column):
        items = self.get_column(column, False)
        return max(items)

    # ----- Получить среднее значение столбца (Столбец должен быть числовой) -----
    def get_mean(self, column):
        items = self.get_column(column, False)
        return mean(items)


    # ----- Получить медианное значение столбца -----
    def get_median(self, column):
        items = self.get_column(column, False)
        items.sort()
        p = int(len(items) / 2)
        return items[p]

    # ----- Получить значение заданного процентиля для столбца -----
    def get_procentile(self, column, procentile):
        items = self.get_column(column, False)
        items.sort()
        if procentile <= 0:
            return items[0]
        elif procentile >= 1:
            return items[-1]
        p = round(procentile * len(items))
        return items[p]


    # ----- Автоматическая конвертация данных -----
    def autoconvert_from_strings(
            self,
            columns = [],
            supposed_data_types = {DATA_TYPE__INTEGER, DATA_TYPE__FLOAT, DATA_TYPE__BOOLEAN, DATA_TYPE__DATETIME}
        ):
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for j in columns_indexes:
            x = self.data[0][j]
            if type(x) != type('abc'):
                # в столбце данные не являются строкой, поэтому нет необходимости в их автоматической конвертации
                continue
            possible_data_types = supposed_data_types.copy()
            for i in range(len(self.data)):
                x = self.data[i][j]
                if x is None:
                    # пропуск неопределённых значений
                    continue
                
                if DATA_TYPE__INTEGER in possible_data_types:
                    try:
                        x_int = int(x)
                    except:
                        possible_data_types.discard(DATA_TYPE__INTEGER)
                
                if DATA_TYPE__FLOAT in possible_data_types:
                    try:
                        x_float = float(x)
                    except:
                        possible_data_types.discard(DATA_TYPE__FLOAT)

                if DATA_TYPE__BOOLEAN in possible_data_types:
                    if x not in TRUE_VALUES and x not in FALSE_VALUES:
                        possible_data_types.discard(DATA_TYPE__BOOLEAN)

                if DATA_TYPE__DATETIME in possible_data_types:
                    x_dt = str_to_date_time(x)
                    if x_dt is None:
                        possible_data_types.discard(DATA_TYPE__DATETIME)

                if len(possible_data_types) == 0:
                    # все возможные типы данных для этого столбца исключены - не читать дальнейшие строки этого столбца
                    break
            if DATA_TYPE__BOOLEAN in possible_data_types:
                self.transform(TRANSFORMATION_KIND__TO_BOOLEAN, j)
            elif DATA_TYPE__INTEGER in possible_data_types:
                self.transform(TRANSFORMATION_KIND__TO_INTEGER, j)
            elif DATA_TYPE__FLOAT in possible_data_types:
                self.transform(TRANSFORMATION_KIND__TO_FLOAT, j)
            elif DATA_TYPE__DATETIME in possible_data_types:
                self.transform(TRANSFORMATION_KIND__TO_DATE_TIME, j)


    # ----- Вывести первые first_rows_count строк с данными -----
    def head(self, columns = [], first_rows_count = 5):
        self.print(columns, 0, first_rows_count)


    # ----- Вывести последние last_rows_count строк с данными -----
    def tail(self, columns = [], last_rows_count = 5):
        self.print(columns, len(self.data)-last_rows_count, len(self.data))



def sample1():
    cr1 = ColumnsReader()
    cr1.read(
        #'20230726-H/train.csv',
        'sample1.txt',
        csv__delimiter='\t',
        on_undefined_action=ON_UNDEFINED__KEEP,
        rows_max_count__file=25
    )
    print(cr1.header); pprint(cr1.data)

    # изменение в data1 приводит к изменению в исходном объекте
    #data1 = scr1.get_data(); data1[0][0] = 'replaced value'; print(scr1.header); pprint(scr1.data); exit(0)

    # изменение в data1 НЕ приводит к изменению в исходном объекте
    #data1 = scr1.copy_data(); data1[0][0] = 'replaced value'; print(scr1.header); pprint(scr1.data); pprint(data1); exit(0)

    #a1 = scr1.get_data(['height', 'weight']); pprint(a1); exit(0)
    cr2 = cr1.copy(['height', 'weight'], 2, 4)
    cr2.str_replace(',', '.')
    cr2.to_float()
    pprint(cr2.get_data())
    exit(0)
    cr1.one_hot_encoding('material')
    print(cr1.header); pprint(scr1.data)

    cr1.str_replace(',', '.')
    cr1.to_float()
    print(cr1.header); pprint(cr1.data)


if __name__ == "__main__":
    sample1(); exit(0)
    scr1.one_hot_encoding('C')
    scr1.to_float()
    print(scr1.get_header())
    pprint(scr1.data)
    print(len(scr1.data))
    #pprint(scr1.get_stat())
    print('-------------------')
    scr2 = scr1.copy(['A', 'C=+', 'C=-', 'target'])
    print('header2:', scr2.header)
    pprint(scr2.data)

    print('A:', scr2.get_column('A'))
