import pandas as  pd
import numpy as np
from googletrans import Translator
translator = Translator()
from iso3166 import countries
from geopy.geocoders import Nominatim
from functools import partial
import os
import regex as re
pd.set_option('display.max_columns', 50000)
pd.set_option('display.max_rows', 50000)
pd.set_option('display.width', 50000)
os.chdir(r'C:\Users\Gigi\PycharmProjects\Giraffe\Data Science Automotive Project')

    # 1 Step pre-processing
target_data = pd.read_excel('Target Data.xlsx')
target_data = target_data.fillna('null')

with open ("supplier_car.json", encoding = 'utf-8') as f:
    data = f.read()
    data1 = pd.read_json(data, lines=True, encoding = 'latin1')
    data1 = data1.drop_duplicates(subset = ['MakeText', 'TypeName', 'TypeNameFull', 'ModelText',
       'ModelTypeText', 'Attribute Names', 'Attribute Values'])
    data2 = data1.sort_values('ID')
    data2.drop('entity_id', inplace = True, axis=1)
    data2.drop(data2[data2['Attribute Names']=='FirstRegMonth'].index, inplace = True)
    data2.drop(data2[data2['Attribute Names']=='FirstRegYear'].index, inplace = True)
    data2.drop(data2[data2['Attribute Names']=='Co2EmissionText'].index, inplace = True)
    data2.drop(data2[data2['Attribute Names']=='Properties'].index, inplace = True)
    data2.drop(data2[data2['Attribute Names']=='ConsumptionRatingText'].index, inplace = True)
    data2.drop(data2[data2['Attribute Names']=='DriveTypeText'].index, inplace = True)
    data2.drop(data2[data2['Attribute Names']=='ConditionTypeText'].index, inplace = True)


    pp = pd.DataFrame(columns=['ID', 'carType', 'color', 'condition', 'currency', 'drive', 'city', 'country',
                                      'make', 'manufacture_year', 'mileage', 'mileage_unit', 'model',
                                      'model_variant', 'price_on_request', 'type', 'zip', 'manufacture_month',
                                      'fuel_consumption_unit'])

    pp['ID'] = data2['ID']
    pp['carType'] = np.where(data2['Attribute Names'] == 'BodyTypeText', data2['Attribute Values'], '')
    pp['city'] = np.where(data2['Attribute Names'] == 'City', data2['Attribute Values'], '')
    pp['country'] = ''
    pp['currency'] = ''
    pp['zip'] = ''
    pp['type'] = ''
    pp['drive'] = ''
    pp['condition'] = ''
    pp['manufacture_year'] = ''
    pp['manufacture_month'] = ''
    pp['price_on_request'] = ''
    pp['fuel_consumption_unit'] = np.where(data2['Attribute Names'] == 'ConsumptionTotalText', data2['Attribute Values'], '')
    pp['fuel_consumption_unit'] = np.where(pp['fuel_consumption_unit'].str.contains('km', regex = True), 'l_km_consumption', '')
    pp['color'] = np.where(data2['Attribute Names'] == 'BodyColorText', data2['Attribute Values'], '')
    pp['make'] = data2['MakeText']
    pp['model'] = data2['ModelText']
    pp['model'].fillna('null', inplace = True)
    pp['model_variant'] = data2['TypeName']
    pp['model_variant'] = np.where(pp['model_variant'] == 'null', '', pp['model_variant'])
    pp['mileage'] = np.where(data2['Attribute Names'] == 'Km', data2['Attribute Values'], '')
    pp['mileage_unit'] = np.where(pp['mileage'] != '', 'kilometer', '')
    pp['TransmissionTypeText'] = np.where(data2['Attribute Names'] == 'TransmissionTypeText', data2['Attribute Values'], '')
    pp['TransmissionTypeText'] = np.where(pp['TransmissionTypeText'] == 'null', '', pp['TransmissionTypeText'])
    pp['Doors'] = np.where(data2['Attribute Names'] == 'Doors', data2['Attribute Values'], '')
    pp['InteriorColorText'] = np.where(data2['Attribute Names'] == 'InteriorColorText', data2['Attribute Values'], '')
    pp['Hp'] = np.where(data2['Attribute Names'] == 'Hp', data2['Attribute Values'], '')
    pp['Ccm'] = np.where(data2['Attribute Names'] == 'Ccm', data2['Attribute Values'] , '')
    pp['Seats'] = np.where(data2['Attribute Names'] == 'Seats', data2['Attribute Values'], '')
    pp['Doors'] = np.where(pp['Doors'] != '', pp['Doors'] + ' doors', '')
    pp['Hp'] = np.where((pp['Hp'] != '') & (pp['Hp'] != '0'), pp['Hp'] + " hp", '')
    pp['InteriorColorText'] = np.where((pp['InteriorColorText']!= '') & (pp['InteriorColorText'] != 'null'),
                                       pp['InteriorColorText'] + " interior",'')
    pp['Ccm'] = np.where(pp['Ccm'] != '', pp['Ccm'] + " ccm", '')
    pp['Seats'] = np.where((pp['Seats'] != '') & (pp['Seats'] != '0'), pp['Seats'] + ' seats', '')
    pp2 = pp.groupby('ID')[['carType', 'color', 'condition', 'currency', 'drive', 'city', 'country', 'manufacture_year', 'mileage',
                            'mileage_unit', 'price_on_request', 'type', 'zip', 'manufacture_month', 'fuel_consumption_unit', 'Doors',
                            'TransmissionTypeText', 'InteriorColorText', 'Hp', 'Ccm', 'Seats']].sum()

    dict_make = dict(zip(pp.ID, pp.make))
    dict_model = dict(zip(pp.ID, pp.model))
    dict_model_variant = dict(zip(pp.ID, pp.model_variant))
    pp2['make'] = dict_make.values()
    pp2['model'] = dict_model.values()
    pp2['model_variant'] = dict_model_variant.values()
    pp2['merged_data'] = pp2['Doors']+ " " + pp2['Hp'] + " "+ pp2['InteriorColorText'] + " " \
                         + pp2['Ccm'] + ' '+ pp2['TransmissionTypeText'] + ' ' + pp2['Seats']
    pp2['model_variant'] = pp2['model_variant'] + ' ' + pp2['merged_data']
    #pp2 is our final dataframe for pre-processing dataframe


    # 2 Step normalisation
    norm = pp2.copy()
    norm.color.replace('', 'sonstiges', inplace = True, regex = True)
    norm.city.replace('', 'unknown', inplace = True, regex = True)

    list_german = norm.color.unique().tolist()
    list_english = []
    for eelement in list_german:
        list_english.append(translator.translate(eelement, src = 'de', dest = 'en').text)
    dict1 = dict(zip(list_german, list_english))
    norm.replace({"color":dict1}, inplace=True)

    geolocator = Nominatim(user_agent="google")
    geocode = partial(geolocator.geocode, language="en")
    list_cities = norm.city.unique().tolist()
    list_countries = []
    for x in list_cities:
        if x == 'unknown':
            continue
        list_countries.append(geocode(x)[-2])
    list_countries1 = []
    for p in list_countries:
        list_countries1.append(p.split(', ')[-1])
    list_countries1.insert(0, 'null')

    list_countries = []
    list_codes = []
    list_country_upper = [elem.upper() for elem in list_countries1]
    for q in list_country_upper:
        if q == 'NULL':
            continue
        list_codes.append(countries.get(q).alpha2)

    dict2 = dict(zip(list_cities, list_country_upper))
    norm['country'] = norm['city'].copy()
    norm.replace({"country":dict2}, inplace=True)

    list_codes.insert(0, 'NULL')
    dict_codes = dict(zip(list_country_upper, list_codes))
    norm.replace({'country':dict_codes}, inplace = True)
    norm.replace('', 'null', inplace = True)
    norm.city.replace('unknown', 'null', inplace = True)
    norm.country.replace('NULL', 'null', inplace = True)
    norm.make.replace('FORD (USA)', 'FORD', inplace = True)
    norm['make'] = norm['make'].str.title()
    norm.model_variant = norm.model_variant.replace(r'\s+', ' ', regex=True)
    # norm is our final dataframe for normalisation step

    # 3 Step integration
    integ = norm.copy()
    integ.drop(columns=['Doors', 'TransmissionTypeText', 'InteriorColorText', 'Hp', 'Ccm', 'Seats', 'merged_data'],
             inplace=True)
    integ = integ.reset_index()
    integ = integ[['carType', 'color', 'condition', 'currency', 'drive', 'city', 'country', 'make', 'manufacture_year',
                   'mileage', 'mileage_unit', 'model', 'model_variant', 'price_on_request', 'type', 'zip',
                   'manufacture_month', 'fuel_consumption_unit']]
    final_integ = pd.concat([integ, target_data], join = 'outer')
    #final_integ is our final dataframe for integration step

    with pd.ExcelWriter('Target data.xlsx') as writer:
        pp2.to_excel(writer, sheet_name = 'pre-processing', index = False)
        norm.to_excel(writer, sheet_name = 'normalisation', index = False)
        final_integ.to_excel(writer, sheet_name = 'integration', index = False)






