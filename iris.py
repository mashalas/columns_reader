#!/usr/bin/python3

import columns_reader as cr
from pprint import pprint

#print(min(1,-3,5)); exit(0)


if __name__ == '__main__':
    crIris = cr.ColumnsReader()
    crIris.read(
        'iris.csv'
    )

    '''items = []
    for i in range(150):
        if i % 5 == 0:
            items.append('0')
        else:
            items.append('1')
    crIris.add_column('flag', items)
    crIris.autoconvert_from_strings()
    data = crIris.get_data()
    pprint(data); exit(0)'''
    
    crIris.transform(cr.TRANSFORMATION_KIND__UPPER, 4)
    crIris.transform(cr.TRANSFORMATION_KIND__TO_FLOAT, [0,1,2,3])

    mn0 = crIris.get_min('Sepal_length')
    mx0 = crIris.get_max('Sepal_length')
    avg0 = crIris.get_mean('Sepal_length')
    m0 = crIris.get_procentile('Sepal_length', 0.5)
    print(mn0, mx0, avg0, m0)
    #crIris.transform(cr.TRANSFORMATION_KIND__NORMALIZE, [0,1,2,3],  on_error=cr.ON_ERROR__SET_NONE)

    crIris.one_hot_encoding('Species', keep_src_column=True)
    #pprint(data)

    labels_list = crIris.get_unique_values('Species', ignore_case=True)
    print(labels_list)
    labels_stat = crIris.count_unique_values('Species')
    print(labels_stat)

    crIris.drop_column(['species', 'sepal_length'])
    items = [(1+i)/10.0 for i in range(crIris.get_rows_count())]
    #print(items); exit(0)
    crIris.add_column('sn', items)

    crIris.update('sn', [-1, -5], None)
    crIris.delete_rows_with_empty()

    crIris.print()
