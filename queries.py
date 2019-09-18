from google.cloud import bigquery
from .process import credentials, client


def query_all_payments():
    '''
    Query que retorna todos os pagamentos feitos por todos os clientes
    '''
    query_job = client.query("""
    SELECT tb.id, tb.nome as name, tb.segmento as segment, tb.cidade as city, tb.estado as state, tb.plan as plan, (tb.price/tb.months_paid) as monthly_price, tb.months_paid as months_paid_upfront, 
       DATE_ADD(tb.payment_date, INTERVAL (ar - 1) MONTH) as payment_date
    FROM `mercos-de-test.payments.customers_payments_join` as tb
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, tb.months_paid)) AS ar
    ORDER BY tb.id asc, payment_date asc
    """)

    print("Iniciando job {}".format(query_job.job_id))

    results = query_job.result()  # Waits for job to complete.
    print("Job concluído.")
    return results


def query_mrr():
    '''
    Query que retorna todos os pagamentos feitos por todos os clientes
    '''
    query_job = client.query("""
    SELECT EXTRACT(ISOYEAR FROM tb.payment_date) AS year, 
       EXTRACT(MONTH FROM tb.payment_date) AS month,
       DATE_TRUNC(tb.payment_date, MONTH) AS year_month,
       SUM(tb.monthly_price) as MRR
    FROM `mercos-de-test.reports.all_payments` AS tb 
    WHERE DATE_DIFF(CURRENT_DATE(), tb.payment_date, DAY) > 0
    GROUP BY year, month, year_month
    ORDER BY year ASC, month asc
    """)

    print("Iniciando job {}".format(query_job.job_id))

    results = query_job.result()  # Waits for job to complete.
    print("Job concluído.")
    return results


def query_new_mrr():
    '''
    Query que retorna todos os pagamentos feitos por todos os clientes
    '''
    query_job = client.query("""
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
    """)

    print("Iniciando job {}".format(query_job.job_id))

    results = query_job.result()  # Waits for job to complete.
    print("Job concluído.")
    return results


def query_expansion_mrr():
    '''
    Query que retorna todos os pagamentos feitos por todos os clientes
    '''
    query_job = client.query("""
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
    """)

    print("Iniciando job {}".format(query_job.job_id))

    results = query_job.result()  # Waits for job to complete.
    print("Job concluído.")
    return results


def query_contraction_mrr():
    '''
    Query que retorna todos os pagamentos feitos por todos os clientes
    '''
    query_job = client.query("""
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
    """)

    print("Iniciando job {}".format(query_job.job_id))

    results = query_job.result()  # Waits for job to complete.
    print("Job concluído.")
    return results


def query_cancelled_mrr():
    '''
    Query que retorna todos os pagamentos feitos por todos os clientes
    '''
    query_job = client.query("""
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
    """)

    print("Iniciando job {}".format(query_job.job_id))

    results = query_job.result()  # Waits for job to complete.
    print("Job concluído.")
    return results


def query_resurrected_mrr():
    '''
    Query que retorna todos os pagamentos feitos por todos os clientes
    '''
    query_job = client.query("""
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
    """)

    print("Iniciando job {}".format(query_job.job_id))

    results = query_job.result()  # Waits for job to complete.
    print("Job concluído.")
    return results
