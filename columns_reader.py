
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
TRANSFORMATION_KIND__TO_INTEGER = 'to_float'
TRANSFORMATION_KIND__TO_STRING = 'to_float'
TRANSFORMATION_KIND__TO_BOOLEAN = 'to_boolean'
TRANSFORMATION_KIND__TO_DATE_TIME = 'to_date_time'

DIRECTION__VER = 'v'
DIRECTION__HOR = 'h'

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
                # искать по имени столбца без учёта регистра
                column__lower = column.lower()
                for j in range(len(header)):
                    if column__lower == header[j].lower():
                        column_index = j
                        break
        return column_index


    def get_columns_indexes(self, columns, header):
        if type(columns) != type([]) and type(columns) != type(()):
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


    def remove_column(self, column):
        ci = self.get_column_index(column, self.header)
        for i in range(len(self.data)):
            del self.data[i][ci]
        del self.header[ci]


    def remove_row(self, row_number):
        del self.data[row_number]


    def get_rows_count(self):
        return len(self.data)
        
    """
    def remove_rows_with_empty(self):
        removing_rows = []
        for i in range(len(self.data)):
            for j in range(len(self.data[i])):
                if self.data[i][j] == '':
                    removing_rows.append(i)
                    break
        if len(removing_rows) > 0:
            tmp_data = []
            for i in range(len(self.data)):
                if i not in removing_rows:
                    tmp_data.append(self.data[i])
            self.data.clear()
            self.data = tmp_data.deepcopy()
    """


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


    def one_hot_encoding(self, column, classes = {}, when_matched = 1.0, when_missed = 0.0, keep_src_column:bool = False):
        # classes: [
        #    ['animals',   'leopard', 'tiger', 'wolf', 'elephant'],
        #    ['plants'     'rye', 'wheat', 'apples', 'apple', 'corn']
        # ]

        ci = self.get_column_index(column, self.header)

        if len(classes.keys()) == 0:
            # классы не заданы - автоматически определить классы на основе имеющихся в столбце значений
            src_column_name = self.header[ci]
            items_uniq = []
            for i in range(len(self.data)):
                x = self.data[i][ci]
                if x is None:
                    continue
                if not x in items_uniq:
                    items_uniq.append(x)
            items_uniq.sort()
            for x in items_uniq:
                class_name = src_column_name + '=' + x
                classes[class_name] = [x]
        
        # если не для всех классов значения заданы в виде списка - сконвертировать скаляр в список
        #for j in range(len(classes)):
        for key in classes.keys():
            values = classes[key]
            if type(values) != type([]) and type(values) != type(()):
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
            self.remove_column(ci)


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



    def head(self, columns = [], first_rows_count = 5):
        self.print(columns, 0, first_rows_count)

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
