
import csv
from collections import Counter
from statistics import mean
from pprint import pprint
import datetime

ON_UNDEFINED__KEEP = 'keep'
ON_UNDEFINED__SKIP_ROW = 'skip_row'
ON_UNDEFINED__BREAK = 'break'
ON_UNDEFINED__EXCEPTION = 'exception'

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
# datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  =>  2022-09-24 10:18:32.926  (trim the last three digits of %f (microseconds))


#s1 = "abcdefg"; s2 = s1[:-3]; print(s2); exit(0)
#d1 = datetime.datetime.now(); s1 = d1.strftime(DEFAULT_DATETIME_FORMAT); print(s1); exit(0)
#print(str(True)); print(str(False)); exit(0)
#d1 = datetime.datetime.strptime("2025-11-20 11:46", "%Y-%m-%d %H:%M"); print(d1); print(type(d1)); exit(0)
#d1 = datetime.datetime.now(); s1 = d1.strftime("%Y-%m-%d %H:%M:%S"); print(d1, s1); exit(0)


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


def str2datetime():
    pass



class SimpleColumnsReader():

    def __init__(self) -> None:
        self.header = []
        self.data = []
        self.skipped_count = 0
        self.data_type = DATA_TYPE__STRING

    def get_data_type(self):
        return self.data_type

    def get_skipped_count(self):
        return self.skipped_count

    """
    def get_header(self, columns = []):
        if len(columns) == 0:
            return self.header.copy()
        header = []
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for ci in columns_indexes:
            header.append(self.header[ci])
        return header
    """

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
            only_header:bool = False,
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
            if clear_data:
                self.data.clear()
                self.skipped_count = 0
            file_row_number = 0
            appended_rows_count = 0
            for row in reader:
                #print(row)
                file_row_number += 1
                if len(self.header) == 0:
                    columns_indexes = self.get_columns_indexes(columns, row)
                    for ci in range(len(columns_indexes)):
                        self.header.append( row[ci] )
                    if only_header:
                        return
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
                            return
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

    def get_column(self, column):
        column_index = self.get_column_index(column, self.header)
        items = []
        for i in range(len(self.data)):
            items.append(self.data[i][column_index])
        return items
    

    def _convert_value(self, x, dest_data_type, true_values = TRUE_VALUES, false_values = FALSE_VALUES, datetime_format = ''):
        result = None
        if dest_data_type == DATA_TYPE__STRING:
            if isinstance(x, datetime.datetime):
                if datetime_format == '':
                    datetime_format = DEFAULT_DATETIME_FORMAT
                result = x.strftime(datetime_format) # если есть параметр %f - будут микросекунды (6 символов после запятной)
                #if datetime_format.find('.%f') >= 0:
                #    # используются миллисекунды
                #    result = result[:-3]
            else:               
                result = str(x)
        elif dest_data_type == DATA_TYPE__INTEGER:
            result = int(x)
        elif dest_data_type == DATA_TYPE__FLOAT:
            result = float(x)
        elif dest_data_type == DATA_TYPE__BOOLEAN:
            if x in true_values:
                result = True
            elif x in false_values:
                result = False
        elif dest_data_type == DATA_TYPE__DATETIME:
            result = str2datetime(x)
        return result


    def _convert(
            self,
            dest_data_type,
            columns,
            instead_of_undefined,
            true_values = TRUE_VALUES,
            false_values = FALSE_VALUES,
            datetime_format = ''
        ):
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for ci in columns_indexes:
            undefined_count = 0
            for i in range(len(self.data)):
                x = self.data[i][ci]
                if x is None:
                    undefined_count += 1
                    if not instead_of_undefined is None:
                        # явно указано на что заменить None
                        self.data[i][ci] = instead_of_undefined
                else:
                    self.data[i][ci] = self._convert_value(
                        x,
                        dest_data_type,
                        true_values=true_values,
                        false_values=false_values
                    )
            if undefined_count > 0 and instead_of_undefined is None:
                # есть неопределённые значения и не указано на что их заменить - заменить на медианное значение
                items = []
                for i in range(len(self.data)):
                    if not self.data[i][ci] is None:
                        items.append(self.data[i][ci])
                items.sort()
                p = int(len(items) / 2)
                median_value = items[p]
                for i in range(len(self.data)):
                    if self.data[i][ci] is None:
                        self.data[i][ci] = median_value

            
    def to_float(self, columns = [], instead_of_undefined = None):
        self._convert(DATA_TYPE__FLOAT, columns, instead_of_undefined)

    def to_integer(self, columns = [], instead_of_undefined = None):
        self._convert(DATA_TYPE__INTEGER, columns, instead_of_undefined)

    def to_boolean(self, columns = [], instead_of_undefined = None, true_values = TRUE_VALUES, false_values = FALSE_VALUES):
        self._convert(DATA_TYPE__INTEGER, columns, instead_of_undefined, true_values=true_values, false_values=false_values)

    def to_string(self, columns = [], instead_of_undefined = None):
        self._convert(DATA_TYPE__STRING, columns, instead_of_undefined)

    def to_datetime(self, columns = [], instead_of_undefined = None, datetime_format = ''):
        self._convert(DATA_TYPE__DATETIME, columns, instead_of_undefined, datetime_format=datetime_format)

    """
    def to_float1(self, columns = [], instead_of_undefined = None):
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for ci in columns_indexes:
            undefined_count = 0
            for i in range(len(self.data)):
                x = self.data[i][ci]
                if x is None:
                    undefined_count += 1
                    if not instead_of_undefined is None:
                        # явно указано на что заменить None
                        self.data[i][ci] = instead_of_undefined
                else:
                    self.data[i][ci] = float(x)
            if undefined_count > 0 and instead_of_undefined is None:
                # есть неопределённые значения и не указано на что их заменить - заменить на медианное значение
                items = []
                for i in range(len(self.data)):
                    if type(self.data[i][ci]) == type(123.456):
                        items.append(self.data[i][ci])
                items.sort()
                p = int(len(items) / 2)
                median_value = items[p]
                for i in range(len(self.data)):
                    if not type(self.data[i][ci]) == type(123.456):
                        self.data[i][ci] = median_value
    """

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

    def str_replace(self, seek, replace_to, columns = []):
        columns_indexes = self.get_columns_indexes(columns, self.header)
        for ci in columns_indexes:
            for i in range(len(self.data)):
                if type(self.data[i][ci]) == type("abc"):
                    self.data[i][ci] = self.data[i][ci].replace(seek, replace_to)

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
        csr = SimpleColumnsReader()
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

    def head(self, columns = [], first_rows_count = 5):
        self.print(columns, 0, first_rows_count)

    def tail(self, columns = [], last_rows_count = 5):
        self.print(columns, len(self.data)-last_rows_count, len(self.data))



def sample1():
    scr1 = SimpleColumnsReader()
    scr1.read(
        #'20230726-H/train.csv',
        'sample1.txt',
        csv__delimiter='\t',
        on_undefined_action=ON_UNDEFINED__KEEP,
        rows_max_count__file=25
    )
    print(scr1.header); pprint(scr1.data)

    # изменение в data1 приводит к изменению в исходном объекте
    #data1 = scr1.get_data(); data1[0][0] = 'replaced value'; print(scr1.header); pprint(scr1.data); exit(0)

    # изменение в data1 НЕ приводит к изменению в исходном объекте
    #data1 = scr1.copy_data(); data1[0][0] = 'replaced value'; print(scr1.header); pprint(scr1.data); pprint(data1); exit(0)

    #a1 = scr1.get_data(['height', 'weight']); pprint(a1); exit(0)
    scr2 = scr1.copy(['height', 'weight'], 2, 4)
    scr2.str_replace(',', '.')
    scr2.to_float()
    pprint(scr2.get_data())
    exit(0)
    scr1.one_hot_encoding('material')
    print(scr1.header); pprint(scr1.data)

    scr1.str_replace(',', '.')
    scr1.to_float()
    print(scr1.header); pprint(scr1.data)


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
