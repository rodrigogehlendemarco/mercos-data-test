from google.cloud import bigquery

credentials = 'Omitida por razões de segurança'
client = bigquery.Client.from_service_account_json(credentials)


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


def load_json_to_new_table():
    '''
    Função para carga de arquivo JSON (formatado corretamente) do Storage para nova tabela no Bigquery.
    '''

    dataset_id = 'payments'
    table_name = 'customers_raw'
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()

    job_config.autodetect = True

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
