import requests
import pandas as pd
import re 

## FUNCTION TO GET THE LAST PAGE
def get_last_page(url):       
    r = requests.get(url)    
    url = pd.DataFrame(r.json()['links'])      
    try: 
        last_page = url['href'][3]    
        last_page_number = re.findall('[0-9]+', last_page) 
        last_page_number = last_page_number[3]   
    except:
        last_page_number = 1
        print('No last page')
    return last_page_number

## API URL to get deputies ID
url = 'https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome'
request = requests.get(url)
df_dep_info = pd.DataFrame(request.json()['dados'])

## lists to store the data and the ids
list = []
id_list = []
name_list = []
partido_list = []
uf_list = []
urlFoto_list = []
email_list = []

for year in range(2022, 2023+1):
    page = 1

    ## counter to select index of rows
    count = 0

    ## request the data for each id
    for id in df_dep_info['id']:          
        url = 'https://dadosabertos.camara.leg.br/api/v2/deputados/'+ str(id) + '/despesas?ano=' + str(year) + '&pagina=' + str(page) + '&itens=100&ordem=ASC&ordenarPor=ano'   
        print(id)
        last_page = get_last_page(url) 
        count += 1  
        

    ## Pagination
        for page in range(1, int(last_page)+1):    
            url = 'https://dadosabertos.camara.leg.br/api/v2/deputados/'+ str(id) + '/despesas?ano=' + str(year) + '&pagina=' + str(page) + '&itens=100&ordem=ASC&ordenarPor=ano' 
            r2 = requests.get(url)
            df_expenses = pd.DataFrame(r2.json()['dados'])        
            list.append(df_expenses)

            ## counting how many rows appears
            count_rows = len(r2.json()['dados'])

            for i in range(count_rows):
                id_list.append(id)
                name_list.append(df_dep_info['nome'][count-1])
                partido_list.append(df_dep_info['siglaPartido'][count-1])
                uf_list.append(df_dep_info['siglaUf'][count-1])
                urlFoto_list.append(df_dep_info['urlFoto'][count-1])
                email_list.append(df_dep_info['email'][count-1])
        

## The line below will allow us to transform our list into a dataframe
df_expenses = pd.concat(list)

## add the columns information to the dataframe
df_expenses['id'] = id_list
df_expenses['nome'] = name_list
df_expenses['siglaPartido'] = partido_list
df_expenses['siglaUf'] = uf_list
df_expenses['urlFoto'] = urlFoto_list
df_expenses['email'] = email_list

## Organize the dataframe
df_expenses = df_expenses[['ano','mes', 'id', 'nome', 'siglaPartido', 'siglaUf', 'urlFoto', 'email', 'tipoDespesa', 'dataDocumento', 'valorDocumento', 'urlDocumento', 'nomeFornecedor', 'cnpjCpfFornecedor']].copy()

## Save the dataframe in a csv file
df_expenses.to_csv('expenses.csv', index=False, encoding='utf-8-sig')
