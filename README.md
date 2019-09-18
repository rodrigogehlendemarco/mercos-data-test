# Mercos Data Engineer Test

Boa tarde pessoal, após uma semana de intenso aprendizado aqui está o compilado de tudo que eu consegui fazer, segui as orientações do documento, utilizei scripts em Python, tomei uma surra de SQL (confesso que achei que eu era melhor nessa linguagem), e pra datawarehouse e relatórios eu escolhi a stack do Google Cloud Platform por ser a stack utilizada pela Mercos, em termos de conhecimento eu era igualmente ignorante para essa e todas as outras stacks então decidi aprender essa do zero.

Durante essa semana, dediquei todo meu tempo nesse trabalho e devo admitir que me diverti mais do que esperava, por restrições de tempo (e da minha competência) eu não consegui fazer tudo que queria, então ao final do documento vou citar algumas ações que poderiam ser tomadas na sequência para melhorar a atual data pipeline. Então sem mais delongas, vamos ao que interessa.

### Extração dos dados

O teste propunha dois formatos de arquivos, como fonte de dados: CSV e JSON então resolvi começar pelo arquivo em CSV. O primeiro passo foi carregá-lo manualmente para um intervalo no Google Storage, esse processo pode ser facilmente automatizado mas resolvi pular esse passo para economizar tempo.

Após carregar o arquivo para o Storage, deve-se criar um dataset e uma tabela no Bigquery para receber os dados transformados. Eu optei por criá-los usando Python, porém pode-se fazer o mesmo processo através da interface web do Bigquery sem problemas.

Função para criar o dataset:

```python

def create_dataset():
    '''
    Função para criação do dataset de pagamentos
    '''
    dataset_name = 'payments'
    dataset_ref = client.dataset(dataset_name)
    dataset = bigquery.Dataset(dataset_ref)
    try:
        dataset = client.create_dataset(dataset)
    except Exception as e:
        raise e

```

Função para criar a tabela:

```python

def create_payments_table():
    '''
    Função para criação da tabela de pagamentos dentro do dataset
    '''
    schema = [
        bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("payment_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("plan", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("months_paid", "INTEGER", mode="REQUIRED"),
    ]

    # TODO: definir campos de particionamento e clusterização

    table_id = "mercos-de-test.payments.payments_raw"

    table = bigquery.Table(table_id, schema=schema)

    try:
        table = client.create_table(table)  # API request
    except Exception as e:
        raise e

    print(
        "Criada tabela {}.{}.{}".format(
            table.project, table.dataset_id, table.table_id)
    )

```

Como comentado no código, é possível implementar particionamento e clusterização das tabelas para melhora na eficiência das queries, por restrições de tempo eu acabei não me preocupando com isso.

O próximo passo foi fazer a transformação no arquivo CSV hospedado no Storage, para isso utilizei a ferramenta Google Dataprep, que permite que eu crie uma receita de transformação que pode ser automatizada para cargas posteriores de arquivos, inclusive, ao fim do processo de transformação é possível fazer a carga dos dados para o Bigquery diretamente para a tabela criada no código anterior.

OBS: Preciso de um email de algum responsável da Mercos para que eu possa compartilhar o acesso ao projeto no Google Cloud Platform para que vocês possam visualizar a receita criada no Dataprep.

Feito isso, vamos para o arquivo JSON, este também foi disponibilizado via API e sua extração para o Storage pode ser facilmente automatizada, além disso, é preciso transformar o JSON para um formato aceito pelo Bigquery, onde cada objeto deve estar em uma nova linha dentro do arquivo. Para tal, criei um script que baixa o arquivo da API e o transforma para o formato adequado em seguida:

```python

def format_json_for_bigquery():
    '''
    Função que formata um arquivo JSON de linha única para o formato aceito pelo Bigquery (um objeto por linha).
    O arquivo é recebido via API como manda o exercício.
    '''
    import requests
    import json

    url = 'https://demo4417994.mockable.io/clientes/'

    request = requests.get(url)

    data = request.json()

    # Converte para json separado por linha
    result = [json.dumps(record, ensure_ascii=False) for record in data]

    # Escreve para arquivo
    with open('clientes_nl.json', 'w', encoding='utf8') as outfile:
        outfile.writelines("%s\n" % line for line in result)

```
Após feito isso, podemos imediatamente exportar o arquivo JSON do Storage para o Bigquery, inclusive não é necessário criar o schema da tabela antecipadamente pois o Bigquery é inteligente o suficiente para detectar o schema automaticamente, vamos ao código:

```python

def load_json_to_new_table():
    '''
    Função para carga de arquivo JSON (formatado corretamente) do Storage para nova tabela no Bigquery.
    '''

    dataset_id = 'payments'
    table_name = 'customers_raw'
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()

    job_config.autodetect = True # Detecta schema automaticamente.

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    uri = "gs://mercos-intervalo/clientes_nl.json"

    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table_name),
        job_config=job_config,
    )

    print("Iniciando job {}".format(load_job.job_id))

    load_job.result()
    print("Job concluído.")

    destination_table = client.get_table(dataset_ref.table(table_name))
    print("Carregadas {} linhas".format(destination_table.num_rows))

```

### Dentro do Bigquery

Finalmente estamos com os arquivos devidamente transformados e carregados no Bigquery, já temos duas tabelas, uma de clientes e outra de pagamentos, vamos criar nossa primeira View para fazer um join das duas tabelas:

```sql

SELECT * 
FROM `mercos-de-test.payments.customers_raw` AS customers 
JOIN `mercos-de-test.payments.payments_raw` AS payments 
ON customers.id = payments.customer_id

```

OBS: Utilizei o sufixo "raw" nas tabelas para indicar que representam a forma dos dados originais no Storage mas confesso que não sei se é uma boa prática.

Mesmo depois do JOIN, a tabela ainda não está pronta para receber consultas mais complexas, ainda falta um passo:

Eis o formato da tabela:

|Linha|segmento|estado|cidade|nome|id|customer_id|payment_date|price|plan|months_paid|
|---|---|---|---|---|---|---|---|---|---|---|
|1  |Outro|AC|Porto Acre|Empresa 752|752|752|2018-05-13|555,0|Prata|3|

O campo "months_paid", quando maior que 1, significa que aquela linha contém o pagamento de mais de uma mensalidade, portanto, precisamos quebrar essas linhas em uma linha por pagamento.

E eu não gosto de admitir mas essa query foi o exercício que mais tomou meu tempo em todo o teste: Mais de 8 horas batendo cabeça até chegar à solução, mas vendo pelo lado positivo, aprendi várias coisas na linguagem as quais eu sequer tinha conhecimento, como as funções OLAP (window frames), Commom Table Expressions (CTEs) e várias sacadas para consultas no Bigquery.

Então, pra variar um pouco e usar a biblioteca do bigquery, criei a query em Python Script:

```python

def query_all_payments():
    '''
    Query que retorna todos os pagamentos feitos por todos os clientes
    '''
    query_job = client.query("""
    SELECT 
    tb.id, 
    tb.nome as name, 
    tb.segmento as segment, 
    tb.cidade as city, 
    tb.estado as state, 
    tb.plan as plan, 
    (tb.price/tb.months_paid) as monthly_price, 
    tb.months_paid as months_paid_upfront, 
    DATE_ADD(tb.payment_date, INTERVAL (ar - 1) MONTH) as payment_date
    FROM `mercos-de-test.payments.customers_payments_join` as tb
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, tb.months_paid)) AS ar
    ORDER BY tb.id asc, payment_date asc
    """)

    print("Iniciando job {}".format(query_job.job_id))

    results = query_job.result()  # Waits for job to complete.
    print("Job concluído.")
    return results

```

Depois eu ainda aproveitei essa query e criei uma View e uma Tabela dentro no Bigquery.

### Visualização

Finalmente, vamos para as Views que darão origem aos indicadores SaaS solicitados no documento. Essa é outra parte do trabalho que sofri bastante, mas fiquei feliz com o quanto aprendi.

Sem delongas vou listá-las aqui:

#### MRR:
```sql

SELECT EXTRACT(ISOYEAR FROM tb.payment_date) AS year, 
       EXTRACT(MONTH FROM tb.payment_date) AS month,
       DATE_TRUNC(tb.payment_date, MONTH) AS year_month,
       SUM(tb.monthly_price) as MRR
FROM `mercos-de-test.reports.all_payments` AS tb 
WHERE DATE_DIFF(CURRENT_DATE(), tb.payment_date, DAY) > 0
GROUP BY year, month, year_month
ORDER BY year ASC, month asc

```

#### New MRR:
```sql

SELECT first_payment_year, 
first_payment_month, 
year_month,
sum(new_MRR) as new_MRR

FROM (

  SELECT 
  tb.name as empresa,
  EXTRACT(ISOYEAR FROM tb.payment_date) AS year, 
  EXTRACT(MONTH FROM tb.payment_date) AS month,
  DATE_TRUNC(tb.payment_date, MONTH) AS year_month,
  SUM(tb.monthly_price) as new_MRR,
  FIRST_VALUE(EXTRACT(MONTH FROM tb.payment_date)) OVER (PARTITION BY tb.name ORDER BY tb.payment_date asc) as first_payment_month,
  FIRST_VALUE(EXTRACT(ISOYEAR FROM tb.payment_date)) OVER (PARTITION BY tb.name ORDER BY tb.payment_date asc) as first_payment_year
  FROM `mercos-de-test.reports.all_payments` AS tb 
  WHERE DATE_DIFF(CURRENT_DATE(), tb.payment_date, DAY) > 0
  GROUP BY tb.name, year, month, tb.payment_date
  ORDER BY year ASC, month asc
)

WHERE month = first_payment_month AND year = first_payment_year
GROUP BY first_payment_year, first_payment_month, year_month
ORDER BY first_payment_year asc, first_payment_month asc

```

#### Expansion MRR:
```sql

SELECT
a.payment_year as payment_year,
a.payment_month as payment_month,
DATE_TRUNC(a.payment_date, MONTH) AS year_month,
sum(a.monthly_price - b.monthly_price) as expansion_MRR
FROM `mercos-de-test.reports.all_payments` as a
JOIN `mercos-de-test.reports.all_payments` as b
ON a.current_month = b.current_month + 1 and a.id = b.id
WHERE (a.monthly_price - b.monthly_price) > 0
GROUP BY payment_year, payment_month, year_month
ORDER BY payment_year asc, payment_month asc

```

#### Contraction MRR:
```sql

SELECT
a.payment_year as payment_year,
a.payment_month as payment_month,
DATE_TRUNC(a.payment_date, MONTH) AS year_month,
sum(a.monthly_price - b.monthly_price) as contraction_MRR
FROM `mercos-de-test.reports.all_payments` as a
JOIN `mercos-de-test.reports.all_payments` as b
ON a.current_month = b.current_month + 1 and a.id = b.id
WHERE (a.monthly_price - b.monthly_price) < 0
GROUP BY payment_year, payment_month, year_month
ORDER BY payment_year asc, payment_month asc

```

#### Cancelled MRR:
```sql

SELECT 
table.cancelled_year as cancelled_year,
table.cancelled_month as cancelled_month,
table.year_month AS year_month,
SUM(table.cancelled_MRR) as cancelled_MRR
FROM (
SELECT
a.payment_year as cancelled_year,
a.payment_month as cancelled_month,
a.current_month as current_month,
DATE_TRUNC(a.payment_date, MONTH) AS year_month,
b.monthly_price as cancelled_MRR,
LEAD(a.current_month) OVER (PARTITION BY a.id ORDER BY a.current_month asc) as next_month
FROM `mercos-de-test.reports.all_payments` as a
JOIN `mercos-de-test.reports.all_payments` as b
ON a.current_month = b.current_month + 1 and a.id = b.id
ORDER BY cancelled_year asc, cancelled_month asc
) as table
WHERE next_month <> (current_month + 1)
GROUP BY cancelled_year, cancelled_month, year_month
ORDER BY cancelled_year asc, cancelled_month asc

```

#### Resurrected MRR:
```sql

SELECT 
table.ressurected_year as ressurected_year,
table.ressurected_month as ressurected_month,
table.year_month as year_month,
SUM(table.ressurected_MRR) as ressurected_MRR
FROM (
SELECT
a.id,
a.payment_year as ressurected_year,
a.payment_month as ressurected_month,
a.current_month as current_month,
DATE_TRUNC(a.payment_date, MONTH) AS year_month,
a.monthly_price as ressurected_MRR,
LAG(a.current_month) OVER (PARTITION BY a.id ORDER BY a.current_month asc) as previous_month
FROM `mercos-de-test.reports.all_payments` as a
JOIN `mercos-de-test.reports.all_payments` as b
ON a.current_month = b.current_month + 1 and a.id = b.id
ORDER BY ressurected_year asc, ressurected_month asc
) as table
WHERE previous_month <> (current_month - 1)
GROUP BY ressurected_year, ressurected_month, year_month
ORDER BY ressurected_year asc, ressurected_month asc

```

E é isso, como eu disse anteriomente, o tempo ficou apertado, só sobrou tempo para fazer o dashboard com essas métricas, espero que vocês gostem, aprender a usar o Data Studio foi um pouquinho chato também hehehe.

Link para o relatório: https://datastudio.google.com/open/1mFzP72efoAhduik9ytza3FGRSaGfi87x


### Concluindo

Tenho ciência de que toda parte de análise de segmentos e regiões acabou não sendo feita e esse seria meu próximo passo, é nesse tipo de análise, inclusive, que eu acredito que podem sair os melhores insights e drivers de decisões estratégicas dentro da empresa, cito alguns exemplos: Quais regiões tem o maior número de vendas e devem ser priorizadas? Qual segmento tem os clientes com maior ticket médio para direcionarmos a força de vendas para esse nicho? E assim por diante, Gosto muito de fazer esse tipo de análise, inclusive fez parte do meu dia-a-dia nos últimos dois anos em que estava empreendendo. 

Finalizo pontuando algumas oportunidades de melhorias no pipeline de dados:

- Estudar a melhor modelagem das tabelas (star schema etc.) a fim de encontrar o equilíbrio entre custo benefício e facilidade e agilidade de uso.
- Particionar e clusterizar as tabelas para tornar as consultas mais rápidas e baratas.
- Implementar Google Composer para automatizar as variadas etapas de extração, transformação e carga de dados.
- Estudar a oportunidade/vantagem/viabilidade de se implementar uma streaming pipeline usando Google Dataflow para captação de dados em tempo real.
- Implementar um dashboard com indicadores de auditoria (gerados pelo próprio Bigquery) para monitorar performance das consultas realizadas e encontrar oportunidades de redução de custos e aumento de eficiência.

É isso pessoal, espero que tenham gostado, eu com certeza aprendi muito e fiquei ainda mais empolgado pra agarrar essa vaga na Mercos. Aguardo os próximos passos para continuarmos um diálogo em caso positivo e, em caso negativo, eu gostaria muito de receber um feedback.

Abraço a todos,

Rodrigo Gehlen De Marco
