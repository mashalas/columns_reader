#!/usr/bin/python3

#x1 = 250.0; x2 = normalize(x1, 0.0, 100.0,  -1.0, +1.0); print(x1, x2); exit(0)

import columns_reader as cr
from pprint import pprint

#print(min(1,-3,5)); exit(0)


if __name__ == '__main__':
    crIris = cr.ColumnsReader()
    crIris.read(
        'iris.csv'
    )
    data = crIris.get_data()
    #crIris.to_float([0, 2, 3])
    crIris.transform(cr.TRANSFORMATION_KIND__UPPER, 4)
    crIris.transform(cr.TRANSFORMATION_KIND__REPLACE, 4, 'I', '!', 3)
    crIris.transform(cr.TRANSFORMATION_KIND__TO_FLOAT, [0,1,2,3], on_error=cr.ON_ERROR__SET_NONE)
    
    mn0 = crIris.get_min('Sepal_length')
    mx0 = crIris.get_max('Sepal_length')
    avg0 = crIris.get_mean('Sepal_length')
    m0 = crIris.get_procentile('Sepal_length', 0.5)
    print(mn0, mx0, avg0, m0)
    #crIris.transform(cr.TRANSFORMATION_KIND__NORMALIZE, [0,1,2,3],  on_error=cr.ON_ERROR__SET_NONE)
    #pprint(data)

    #labels = crIris.get_unique_values('Species', ignore_case=True)
    labels = crIris.count_unique_values('Species')
    print(labels)

