import sdmx
import time
import datetime
import pendulum
import re

from airflow.sdk import dag, task

@dag(
    dag_id="tuik_ydufe_edo_v1",
    schedule=datetime.timedelta(seconds=30),
    start_date=pendulum.datetime(2026, 4, 20, 3, 10, 15, tz="Europe/Istanbul"),
    tags=["tuik"]
)
def process():
    @task
    def get_summary():
        sdmx.add_source({
            "id": "TUIK",
            "url": "https://nsiws.tuik.gov.tr/rest",
            "name": "Türkiye İstatistik Kurumu (TÜİK)"
        }, override=True)

        tuik = sdmx.Client('TUIK')
        df = 'DF_YDUFE_EDO_V1'
        filepath = "/opt/airflow/files"

        print("START DATA", df)
        start_d = time.perf_counter()
        data = tuik.data(df)
        end_d = time.perf_counter()
        elapsed = "%.3f"%(end_d - start_d) + " sec"
        print("END DATA", elapsed)
        timenow = pendulum.now(tz='Europe/Istanbul')
        pd_data = sdmx.to_pandas(data)
        cleanfname = re.sub(r"^DF_", "", df)
        cleandtime = timenow.strftime("%y%m%d%H%M%S")
        filename = f"{cleanfname}_{cleandtime}.csv"
        # pd_data.to_csv(f"{filepath}/{filename}")
        with open(f"{filepath}/LOG_{cleanfname}.txt", "a") as f:
            times = list(pd_data.index.get_level_values("TIME_PERIOD"))
            times.sort(reverse=True)
            f.write(f"\ntime: {timenow}, elapsed: {elapsed}, row count: {pd_data.shape[0]}, max date: {times[0]}")
    
    get_summary()
dag = process()