from selenium import webdriver 
from selenium.webdriver.common.by import By
import time 
import requests
import json
import pandas as pd

driver = webdriver.Chrome()

driver.get("https://new.land.naver.com/complexes?ms=37.4443103,127.1676532,16")


headers = {
        "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2NDk5NDc4OTYsImV4cCI6MTY0OTk1ODY5Nn0.IHj3PTF0ebjIftDUkUrUnbb4ntzIeqc14Ioouz7CbhE",
        "Connection": "keep-alive",
        "Referer": "https://new.land.naver.com/complexes/881?ms=37.4840693,127.0624236,16&a=APT:ABYG:JGC&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36"
    }

a_tags = driver.find_elements(By.CSS_SELECTOR, 'a.marker_complex--apart')
print(len(a_tags))
complex_ids = []

for a in a_tags :
    complex_ids.append(a.get_attribute('id').split('COMPLEX')[0].strip())

final_complex_detail_df_list = []

for complex_id in complex_ids:
        url = "https://new.land.naver.com/api/complexes/{}?sameAddressGroup=false".format(complex_id)
        res = requests.get("https://new.land.naver.com/api/complexes/{}?sameAddressGroup=false".format(complex_id),
                           headers=headers)
        data_dict = res.json()
        complex_detail_df = pd.Series(data_dict['complexDetail']).to_frame().T
        complex_detail_list_df = pd.DataFrame(data_dict['complexPyeongDetailList']) #complexdetail 정보 딕셔너리형태로 보여준다 

        # nexted 
        if "landPriceMaxByPtp" in complex_detail_list_df.columns:
            convert = complex_detail_list_df['landPriceMaxByPtp'].values.tolist()
            nested_df1 = pd.DataFrame([v for v in convert if pd.notna(v)])
            nested_df2 = pd.DataFrame(nested_df1['landPriceTax'].values.tolist())

            nested_df1 = pd.concat(
                [
                    nested_df1.drop("landPriceTax", axis=1),
                    nested_df2
                ],
                axis=1
            )
            complex_detail_list_df = pd.concat(
                [
                    complex_detail_list_df.drop(["supplyArea", "landPriceMaxByPtp"], axis=1),
                    nested_df1,
                ],
                axis=1
            )

        complex_detail_list_df = complex_detail_list_df.rename(columns={"realEstateTypeCode": "realEstateTypeCode2"})

        complex_detail_df = pd.concat([complex_detail_df] * len(complex_detail_list_df))
        complex_detail_df = complex_detail_df.reset_index()

        complex_detail_df = pd.concat(
            [
                complex_detail_df,
                complex_detail_list_df
            ],
            axis=1
        )
        final_complex_detail_df_list.append(complex_detail_df)

final_complex_detail_df = pd.concat(final_complex_detail_df_list)
final_complex_detail_df[['pyeongNo', 'complexNo']] = final_complex_detail_df[['pyeongNo', 'complexNo']].astype(int)

series_list = []
for _, row in final_complex_detail_df.iterrows(): #컬럼 인덱스를 보기쉽게 출력해봄
        complex_num = row['complexNo']
        area_num = row['pyeongNo']

        url = "https://new.land.naver.com/api/complexes/{}/prices?complexNo={}&tradeType=A1&year=5&priceChartChange=false&type=table&areaNo={}".format(
            complex_num, complex_num, area_num
        )
        res = requests.get(url, headers=headers)
        data_dict = res.json()

        try:
            series = pd.DataFrame(data_dict['marketPrices']).iloc[0] #가격 데이터값 정의 매매시세(실거래x)
            series['complexNo'] = complex_num
            series['pyeongNo'] = area_num
            series_list.append(series)
        except:
            continue
        time.sleep(0.5)

price_df = pd.concat(series_list, axis=1).T

merged_df = pd.merge(
        final_complex_detail_df,
        price_df,
        on=["complexNo", "pyeongNo"],
        how="left"
    )
merged_df.head()
merged_df.to_csv('naver_land.csv', encoding='euc-kr', index=False)
driver.close()
