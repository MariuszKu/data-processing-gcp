import pandas as pd
import os


PROJECT = os.environ['PROJECT']
SILVER = os.environ['SILVER']

def extract_trans():
    sql  = f"SELECT * FROM  `{PROJECT}.demodevdwh.transactions` where transaction_amount > 15000 LIMIT 1000"

    df = pd.read_gbq(sql,
                    project_id=PROJECT,
                    dialect="standard"
                    )  

    df.to_csv(f"gs://{SILVER}/reports/trans_report.csv",index=False)

if __name__ == "__main__":
    extract_trans()