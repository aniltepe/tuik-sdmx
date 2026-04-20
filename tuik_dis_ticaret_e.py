import sdmx
import time
import datetime
import pendulum
import re

from airflow.sdk import dag, task

@dag(
    dag_id="dis_ticaret_endeksleri",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    tags=["tuik"]
)
def process():
    @task
    def get_data():
        sdmx.add_source({
            "id": "TUIK",
            "url": "https://nsiws.tuik.gov.tr/rest",
            "name": "Türkiye İstatistik Kurumu (TÜİK)"
        }, override=True)

        df = 'DF_ALTINSIZ_DTH'
        tuik = sdmx.Client('TUIK')
        print("START META", df)
        start_m = time.perf_counter()
        flow = tuik.dataflow(df, agency_id='TR', detail='full', references='descendants')
        end_m = time.perf_counter()
        print("END META", "elapsed", "%.3f"%(end_m-start_m), "sec")
        dimensions = [comp.concept_identity.id 
                    for struct in flow.structure.items()
                    for comp in struct[1].dimensions.components]
        measures = [msr.id for struct in flow.structure.items()
                    for msr in struct[1].measures]
        codelists = {comp.concept_identity.id: {k: v.name["tr"] for k, v in comp.local_representation.enumerated.items.items()}
                    for struct in flow.structure.items()
                    for comp in struct[1].dimensions.components if comp.local_representation.enumerated}
        concepts = {concept.id: concept.name["tr"]
                    for csch in flow.concept_scheme.values()
                    for concept in csch.items.values() if concept.id in dimensions or concept.id in measures}
        print("START SLEEP", "10 sec")
        time.sleep(10)
        print("END SLEEP")
        print("START DATA", df)
        start_d = time.perf_counter()
        data = tuik.data(df)
        end_d = time.perf_counter()
        print("END DATA", "elapsed", "%.3f"%(end_d-start_d), "sec")
        pd_data = sdmx.to_pandas(data)
        for cl in codelists.keys():
            pd_data.index = pd_data.index.set_levels([codelists[vals.name][val] 
                for vals in pd_data.index.levels 
                for val in list(vals) if vals.name == cl
            ], level=cl)
        pd_data.rename(concepts["OBS_VALUE"], inplace=True)
        pd_data.index.set_names([concepts[name] for name in pd_data.index.names], inplace=True)
        pd_data.to_csv(f'/opt/airflow/files/{re.sub(r"^DF_", "", df)}.csv')

    get_data()

dag = process()