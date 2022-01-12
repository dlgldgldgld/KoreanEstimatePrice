import csv
import requests
import json

numOfRows = 50

# prices 정보
url = 'http://apis.data.go.kr/1611000/nsdi/IndvdHousingPriceService/attr/getIndvdHousingPriceAttr'
params = { 'serviceKey' : 'EvZj5v2s4TOzNd2Bez7DuOkPc9LRqhB11HV79D6PU9Uh9TmLzoH6LPb7p6FWv77VDKP5IqkAm/dG9GaAeVkesw==', 
           'pnu' : '', 'stdrYear' : '2021', 'format' : 'json', 'numOfRows' : str(numOfRows), 'pageNo' : '1' }

def loadTotalCountOfContents ( ldCode )  :
    params['pnu'] = ldCode
    req = requests.get(url, params=params)
    record = json.loads(req.content)
    if record['indvdHousingPrices']['totalCount'] == '0' :
        maxPageNum = 0
    else :
        maxPageNum = int(record['indvdHousingPrices']['totalCount']) // numOfRows
        if int(record['indvdHousingPrices']['totalCount']) % numOfRows > 0.0 :
            maxPageNum += 1

    return maxPageNum

def loadContents ( ldCode, pageNum )  :
    params['pnu'] = ldCode
    params['pageNo'] = pageNum
    req = requests.get(url, params=params)
    record = json.loads(req.content)
    return record['indvdHousingPrices']

# 존재하는 법정동 Code를 모두 Search 함.
with open("ldcodeAll.csv", "r", encoding = "cp949") as ldcodeFile :
    ldcode_dic = csv.DictReader(ldcodeFile)
    # 명령어 만들기
    for row in ldcode_dic :
        # 법정동 코드 = row["법정동코드"]
        # 법정동 명 = row["법정동명"]
        # 폐지여부 = row["폐지여부"]
        if row["폐지여부"] == "폐지" :
            continue
        
        pageCount = loadTotalCountOfContents( row['법정동코드'] )
        if pageCount == 0 : 
            continue
        
        contents = []
        for i in range ( 1, pageCount + 1 ) :
            content = loadContents(row['법정동코드'], i)['field']
            # print ( '법정동 : {0}, Page {1}'.format(row["법정동명"], i ))
            contents.extend(content)

        fileName = "./srcFile/"+ row['법정동명'] + ".json"
        with open( fileName, "w") as json_file :
            json.dump(contents, json_file, indent=4)
        print(row['법정동명'], pageCount, len(contents))
        
        

        
        

    
