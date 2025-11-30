import os

DB_CONFiG = {
    'host': 'data-enable-db.ch4cwsua2n2h.eu-north-1.rds.amazonaws.com',
    'port': 5432,
    'database': 'data_enablement_project',
    'user': 'postgres',
    'password': 'Rappygod123'
}

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "cms_applications")

