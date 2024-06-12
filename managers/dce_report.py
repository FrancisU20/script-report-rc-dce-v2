from sqlalchemy import Table, Column, String, MetaData, TIMESTAMP, select, ForeignKey, func, or_
from sqlalchemy.dialects.postgresql import UUID
import pandas as pd

def fetch_report_dce(engine, start_date, end_date):
    metadata = MetaData()

    # Definición de las tablas
    customer = Table('customer', metadata,
        Column('uuid', UUID, primary_key=True),
        Column('identification_number', String(50), nullable=False)
    )

    process = Table('process', metadata,
        Column('uuid', UUID, primary_key=True),
        Column('name', String(50), nullable=False),
        Column('area_uuid', UUID, ForeignKey('area.uuid'), nullable=False)
    )

    registration_sheet = Table('registration_sheet', metadata,
        Column('uuid', UUID, primary_key=True),
        Column('user_request_email', String(50), nullable=False)
    )

    area = Table('area', metadata,
        Column('uuid', UUID, primary_key=True),
        Column('name', String(50), nullable=False),
        Column('created_on', TIMESTAMP, nullable=True),
        Column('updated_on', TIMESTAMP, nullable=True),
        Column('status', String(50), nullable=False)
    )

    smart_form = Table('smart_form', metadata,
        Column('code', String(50), nullable=False)
    )

    binary_report = Table('binary_report', metadata,
        Column('customer_uuid', UUID, ForeignKey('customer.uuid'), nullable=False),
        Column('process_uuid', UUID, ForeignKey('process.uuid'), nullable=False),
        Column('registration_sheet_uuid', UUID, ForeignKey('registration_sheet.uuid'), nullable=False),
        Column('report_name', String(50), nullable=False),
        Column('name_customer', String(50), nullable=False),
        Column('last_name_customer', String(50), nullable=False),
        Column('variable_name', String(50), nullable=False),
        Column('process_number_operation', String(50), nullable=False),
        Column('updated_on', TIMESTAMP, nullable=True),
        Column('created_on', TIMESTAMP, nullable=True),  # Agregando la columna created_on
        Column('request_uuid', UUID, primary_key=True)
    )

    # Construcción de la consulta
    query = select(
        customer.c.identification_number.label('cedula_socio'),
        binary_report.c.request_uuid.label('process_id'),
        process.c.name.label('nombre_del_proceso'),
        binary_report.c.process_number_operation.label('numero_caso'),
        (binary_report.c.name_customer + ' ' + binary_report.c.last_name_customer).label('nombre_del_cliente'),
        registration_sheet.c.user_request_email.label('generador'),
        area.c.name.label('area'),
        binary_report.c.variable_name.label('variables')
    ).select_from(
        binary_report.join(customer, binary_report.c.customer_uuid == customer.c.uuid)
                     .join(process, binary_report.c.process_uuid == process.c.uuid)
                     .join(registration_sheet, binary_report.c.registration_sheet_uuid == registration_sheet.c.uuid)
                     .join(area, process.c.area_uuid == area.c.uuid)
                     .join(smart_form, func.upper(binary_report.c.report_name) == func.upper(smart_form.c.code))
    ).where(
        or_(
            binary_report.c.updated_on.between(start_date, end_date),
            binary_report.c.created_on.between(start_date, end_date)
        )
    )

    # Ejecución de la consulta y conversión a DataFrame
    df = pd.read_sql(query, engine)

    return df
