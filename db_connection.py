import cx_Oracle
import pandas as pd

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

main = pd.read_csv('expenses.csv' , sep=',')
print("File read")

main['ano'] = main['ano'].astype(str)
main['mes'] = main['mes'].astype(str)
main['id'] = main['id'].astype(str)
main['nome'] = main['nome'].astype(str)
main['siglaPartido'] = main['siglaPartido'].astype(str)
main['siglaUf'] = main['siglaUf'].astype(str)
main['urlFoto'] = main['urlFoto'].astype(str)
main['email'] = main['email'].astype(str)
main['tipoDespesa'] = main['tipoDespesa'].astype(str)
main['dataDocumento'] = main['dataDocumento'].astype(str)
main['valorDocumento'] = main['valorDocumento'].astype(str)
main['urlDocumento'] = main['urlDocumento'].astype(str)
main['nomeFornecedor'] = main['nomeFornecedor'].astype(str)
main['cnpjCpfFornecedor'] = main['cnpjCpfFornecedor'].astype(str)

count = 0
for index, row in main.iterrows():
    sql = "INSERT INTO data_tab (ano, mes, id, nome, siglaPartido, siglaUf, urlFoto, email, tipoDespesa, dataDocumento, valorDocumento, urlDocumento, nomeFornecedor, cnpjCpfFornecedor) VALUES (:ano, :mes, :id, :nome, :siglaPartido, :siglaUf, :urlFoto, :email, :tipoDespesa, :dataDocumento, :valorDocumento, :urlDocumento, :nomeFornecedor, :cnpjCpfFornecedor)"
    values = (row['ano'], row['mes'], row['id'], row['nome'], row['siglaPartido'], row['siglaUf'], row['urlFoto'], row['email'], row['tipoDespesa'], row['dataDocumento'], row['valorDocumento'], row['urlDocumento'], row['nomeFornecedor'], row['cnpjCpfFornecedor'])
    cursor.execute(sql, values)
    count += 1
    print(count)

connection.commit()