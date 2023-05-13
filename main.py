import requests
import pandas as pd
import re 
import cx_Oracle
import pandas as pd

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
    print(year)

    ## counter to select index of rows
    count = 0

    ## request the data for each id
    for id in df_dep_info['id']:          
        url = 'https://dadosabertos.camara.leg.br/api/v2/deputados/'+ str(id) + '/despesas?ano=' + str(year) + '&pagina=' + str(page) + '&itens=100&ordem=ASC&ordenarPor=ano'          
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

# patch to instantclient_21_3
cx_Oracle.init_oracle_client(lib_dir=r"C:\\oracle\\instantclient_21_9")

# Variables to connect to Oracle
username = "ADMIN"
password = "Q!w2e3r4t5y6u7I*"
dsn = '(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=g2db51f405a4261_projetoimpacta_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'

# Connect to Oracle
connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)

# Create a cursor
cursor = connection.cursor()

# Execute a statement
cursor.execute("""begin
			        execute immediate 'drop table data_tab';
			        exception when others then if sqlcode <> -942 then raise; end if;
		            end;""")

#Create table
cursor.execute("""create table data_tab(
                                        ano VARCHAR(255), 
                                        mes VARCHAR(255), 
                                        id VARCHAR(255), 
                                        nome VARCHAR(255), 
                                        siglaPartido VARCHAR(255), 
                                        siglaUf VARCHAR(5), 
                                        urlFoto VARCHAR(255), 
                                        email VARCHAR(255), 
                                        tipoDespesa VARCHAR(255), 
                                        dataDocumento VARCHAR(255), 
                                        valorDocumento VARCHAR(255), 
                                        urlDocumento VARCHAR(255), 
                                        nomeFornecedor VARCHAR(255), 
                                        cnpjCpfFornecedor VARCHAR(255))""")
print("Table created")



df_expenses['ano'] = df_expenses['ano'].astype(str)
df_expenses['mes'] = df_expenses['mes'].astype(str)
df_expenses['id'] = df_expenses['id'].astype(str)
df_expenses['nome'] = df_expenses['nome'].astype(str)
df_expenses['siglaPartido'] = df_expenses['siglaPartido'].astype(str)
df_expenses['siglaUf'] = df_expenses['siglaUf'].astype(str)
df_expenses['urlFoto'] = df_expenses['urlFoto'].astype(str)
df_expenses['email'] = df_expenses['email'].astype(str)
df_expenses['tipoDespesa'] = df_expenses['tipoDespesa'].astype(str)
df_expenses['dataDocumento'] = df_expenses['dataDocumento'].astype(str)
df_expenses['valorDocumento'] = df_expenses['valorDocumento'].astype(str)
df_expenses['urlDocumento'] = df_expenses['urlDocumento'].astype(str)
df_expenses['nomeFornecedor'] = df_expenses['nomeFornecedor'].astype(str)
df_expenses['cnpjCpfFornecedor'] = df_expenses['cnpjCpfFornecedor'].astype(str)

count = 0
for index, row in df_expenses.iterrows():
    sql = "INSERT INTO data_tab (ano, mes, id, nome, siglaPartido, siglaUf, urlFoto, email, tipoDespesa, dataDocumento, valorDocumento, urlDocumento, nomeFornecedor, cnpjCpfFornecedor) VALUES (:ano, :mes, :id, :nome, :siglaPartido, :siglaUf, :urlFoto, :email, :tipoDespesa, :dataDocumento, :valorDocumento, :urlDocumento, :nomeFornecedor, :cnpjCpfFornecedor)"
    values = (row['ano'], row['mes'], row['id'], row['nome'], row['siglaPartido'], row['siglaUf'], row['urlFoto'], row['email'], row['tipoDespesa'], row['dataDocumento'], row['valorDocumento'], row['urlDocumento'], row['nomeFornecedor'], row['cnpjCpfFornecedor'])
    cursor.execute(sql, values)
    count += 1
    print(count)

connection.commit()