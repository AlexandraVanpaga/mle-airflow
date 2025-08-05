import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime, Float, UniqueConstraint

def create_table():
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()

    metadata = MetaData()
    alt_users_churn = Table(
        'alt_users_churn',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String, nullable=False),
        Column('begin_date', DateTime),
        Column('end_date', DateTime),
        Column('type', String),
        Column('paperless_billing', String),
        Column('payment_method', String),
        Column('monthly_charges', Float),
        Column('total_charges', Float),
        Column('internet_service', String),
        Column('online_security', String),
        Column('online_backup', String),
        Column('device_protection', String),
        Column('tech_support', String),
        Column('streaming_tv', String),
        Column('streaming_movies', String),
        Column('gender', String),
        Column('senior_citizen', Integer),
        Column('partner', String),
        Column('dependents', String),
        Column('multiple_lines', String),
        Column('target', Integer),
        UniqueConstraint('customer_id', name='uq_customer_id_alt')
    )
    metadata.create_all(engine)


def extract(**kwargs):
    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = """
    select
        c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
        i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
        p.gender, p.senior_citizen, p.partner, p.dependents,
        ph.multiple_lines
    from contracts as c
    left join internet as i on i.customer_id = c.customer_id
    left join personal as p on p.customer_id = c.customer_id
    left join phone as ph on ph.customer_id = c.customer_id
    """
    data = pd.read_sql(sql, conn)
    conn.close()
    return data


def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')
    data['target'] = (data['end_date'] != 'No').astype(int)
    data['end_date'].replace({'No': None}, inplace=True)
    return data


def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="alt_users_churn",
        rows=data.values.tolist(),
        target_fields=data.columns.tolist(),
        replace=True,
        replace_index=['customer_id']
    )
