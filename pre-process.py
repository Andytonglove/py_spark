# -*- coding: utf-8 -*-
import sys
import imp
imp.reload(sys)
sys.setdefaultencoding("utf-8")

import findspark
findspark.init()

import pandas as pd

'''
文件预处理：
这里做了两种验证，第一个是验证是不是有数据为空，为?的数据视为不空。去除空行。
随后做第二步验证，就是用长if验证每一位的数据是否有效且字符串为纯数字。
如为字符串的，则在字典里找有没有对应的key，发现adult.data以及adult.test都没有错误。
当然，.test文件中结尾是50K.，这里直接用vscode替换全部成50K改过来了。
'''

data = pd.read_csv('adult/adult.data', header=None, sep=', ', engine='python')
print(data.shape)
# 第一步，判定含有空值的行
null_lines = data.isnull().T.any()
for index, value in null_lines.iteritems():  # items需要换成iteritems
    if value:
        print("{}行有空值".format(index + 1))
# 去除空值
data.dropna(axis=0, how='any')


# 第二步，判定不对劲的值
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


work_type = {'Private': 1,
             'Self-emp-not-inc': 2,
             'Self-emp-inc': 3,
             'Federal-gov': 4,
             'Local-gov': 5,
             'State-gov': 6,
             'Without-pay': 7,
             'Never-worked': 8,
             '?': -1}
education = {'Bachelors': 1,
             'Some-college': 2,
             '11th': 3,
             'HS-grad': 4,
             'Prof-school': 5,
             'Assoc-acdm': 6,
             'Assoc-voc': 7,
             '9th': 8,
             '7th-8th': 9,
             '12th': 10,
             'Masters': 11,
             '1st-4th': 12,
             '10th': 13,
             'Doctorate': 14,
             '5th-6th': 15,
             'Preschool': 16,
             '?': -1}
marital_status = {'Married-civ-spouse': 1,
                  'Divorced': 2,
                  'Never-married': 3,
                  'Separated': 4,
                  'Widowed': 5,
                  'Married-spouse-absent': 6,
                  'Married-AF-spouse': 7,
                  '?': -1}
occupation = {'Tech-support': 1,
              'Craft-repair': 2,
              'Other-service': 3,
              'Sales': 4,
              'Exec-managerial': 5,
              'Prof-specialty': 6,
              'Handlers-cleaners': 7,
              'Machine-op-inspct': 8,
              'Adm-clerical': 9,
              'Farming-fishing': 10,
              'Transport-moving': 11,
              'Priv-house-serv': 12,
              'Protective-serv': 13,
              'Armed-Forces': 14,
              '?': -1}
relationship = {'Wife': 1,
                'Own-child': 2,
                'Husband': 3,
                'Not-in-family': 4,
                'Other-relative': 5,
                'Unmarried': 6,
                '?': -1}
race = {'White': 1,
        'Asian-Pac-Islander': 2,
        'Amer-Indian-Eskimo': 3,
        'Other': 4,
        'Black': 5,
        '?': -1}
sex = {'Female': 1,
       'Male': 2,
       '?': -1}
native_country = {'United-States': 1,
                  'Cambodia': 2,
                  'England': 3,
                  'Puerto-Rico': 4,
                  'Canada': 5,
                  'Germany': 6,
                  'Outlying-US(Guam-USVI-etc)': 7,
                  'India': 8,
                  'Japan': 9,
                  'Greece': 10,
                  'South': 11,
                  'China': 12,
                  'Cuba': 13,
                  'Iran': 14,
                  'Honduras': 15,
                  'Philippines': 16,
                  'Italy': 17,
                  'Poland': 18,
                  'Jamaica': 19,
                  'Vietnam': 20,
                  'Mexico': 21,
                  'Portugal': 22,
                  'Ireland': 23,
                  'France': 24,
                  'Dominican-Republic': 25,
                  'Laos': 26,
                  'Ecuador': 27,
                  'Taiwan': 28,
                  'Haiti': 29,
                  'Columbia': 30,
                  'Hungary': 31,
                  'Guatemala': 32,
                  'Nicaragua': 33,
                  'Scotland': 34,
                  'Thailand': 35,
                  'Yugoslavia': 36,
                  'El-Salvador': 37,
                  'Trinadad&Tobago': 38,
                  'Peru': 39,
                  'Hong': 40,
                  'Holand-Netherlands': 41,
                  '?': -1}
for index, row in data.iterrows():
    if is_number(row[0]):
        if row[1] in work_type:
            if is_number(row[2]):
                if row[3] in education:
                    if is_number(row[4]):
                        if row[5] in marital_status:
                            if row[6] in occupation:
                                if row[7] in relationship:
                                    if row[8] in race:
                                        if row[9] in sex:
                                            if is_number(row[10]):
                                                if is_number(row[11]):
                                                    if is_number(row[12]):
                                                        if row[13] in native_country:
                                                            continue
    print("{}有错误".format(index + 1))