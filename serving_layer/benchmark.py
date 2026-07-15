"""
serving_layer/benchmark.py

Dublin Bus Analytics Performance Benchmark Suite

Benchmarks:
1. Kinesis ingestion throughput
2. Serving API latency under load
3. EMR Spark batch parallel speedup

Outputs:
benchmark_results/
"""


import os
import sys
import time
import csv
import json
import statistics
import argparse


from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed



# -------------------------------------------------------
# FIX PROJECT IMPORTS
# -------------------------------------------------------

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        ".."
    )
)


sys.path.insert(
    0,
    PROJECT_ROOT
)



import boto3
import requests


import matplotlib

matplotlib.use("Agg")


import matplotlib.pyplot as plt



from config.config import (

    AWS_REGION,

    KINESIS_STREAM_NAME,

    S3_BUCKET_NAME,

    EMR_CLUSTER_ID,

    EMR_BATCH_JOB_SCRIPT_S3_PATH,

    S3_RAW_PREFIX,

    S3_BATCH_OUTPUT_PREFIX

)



# -------------------------------------------------------
# AWS CLIENTS
# -------------------------------------------------------


kinesis_client = boto3.client(

    "kinesis",

    region_name=AWS_REGION

)



emr_client = boto3.client(

    "emr",

    region_name=AWS_REGION

)



RESULTS_DIR = "benchmark_results"





def ensure_results_dir():

    os.makedirs(

        RESULTS_DIR,

        exist_ok=True

    )






def write_csv(filename, rows):

    if not rows:

        return



    ensure_results_dir()



    path = os.path.join(

        RESULTS_DIR,

        filename

    )



    with open(

        path,

        "w",

        newline=""

    ) as f:



        writer = csv.DictWriter(

            f,

            fieldnames=list(rows[0].keys())

        )



        writer.writeheader()

        writer.writerows(rows)



    print(

        f"Saved {path}"

    )





# ======================================================
# 1. KINESIS THROUGHPUT
# ======================================================



def send_record(index):


    payload = {


        "benchmark": True,


        "record_id": index,


        "timestamp":

            datetime.utcnow().isoformat()

    }



    try:


        kinesis_client.put_record(


            StreamName=KINESIS_STREAM_NAME,


            Data=json.dumps(payload).encode(),


            PartitionKey=str(index)


        )


        return True



    except Exception as e:


        print(

            f"Kinesis error: {e}"

        )


        return False





def run_throughput_benchmark():


    print(

        "\n=== THROUGHPUT BENCHMARK ==="

    )


    rates = [

        10,

        50,

        100,

        200,

        500

    ]



    results = []



    for rate in rates:



        total = rate * 5



        print(

            f"Testing {rate} records/sec"

        )



        start = time.perf_counter()



        success = 0



        with ThreadPoolExecutor(

            max_workers=100

        ) as executor:



            futures = [


                executor.submit(

                    send_record,

                    i

                )


                for i in range(total)

            ]



            for future in as_completed(futures):


                if future.result():

                    success += 1




        elapsed = time.perf_counter() - start



        result = {


            "target_rate":

                rate,


            "total_records":

                total,


            "success":

                success,


            "success_rate":

                round(

                    success / total * 100,

                    2

                ),


            "actual_rps":

                round(

                    success / elapsed,

                    2

                )

        }



        results.append(result)



        print(result)




    write_csv(

        "throughput.csv",

        results

    )



    return results
# ======================================================
# 2. SERVING API LATENCY UNDER LOAD BENCHMARK
# ======================================================


def timed_api_request(api_url):


    start = time.perf_counter()


    try:


        response = requests.get(

            f"{api_url}/routes/top-delayed",

            timeout=10

        )



        elapsed = (

            time.perf_counter() - start

        ) * 1000



        return {


            "latency_ms":

                round(

                    elapsed,

                    2

                ),



            "status":

                response.status_code,



            "success":

                response.status_code == 200

        }



    except Exception as e:



        print(

            f"❌ API REQUEST FAILED: {e}"

        )



        return {


            "latency_ms":

                None,



            "status":

                str(e),



            "success":

                False

        }






def percentile(values, percent):


    values = sorted(values)



    index = int(

        len(values)

        *

        percent

        /

        100

    )



    return values[

        min(

            index,

            len(values)-1

        )

    ]






def run_latency_benchmark(api_url):


    print(

        "\n=== API LATENCY UNDER LOAD BENCHMARK ==="

    )



    concurrency_levels = [

        1,

        5,

        10,

        20

    ]



    requests_count = 100



    csv_results = []



    chart_values = []





    for concurrency in concurrency_levels:



        print(

            f"\nTesting concurrency={concurrency}"

        )



        results = []



        with ThreadPoolExecutor(

            max_workers=concurrency

        ) as executor:



            futures = [


                executor.submit(

                    timed_api_request,

                    api_url

                )


                for _ in range(requests_count)

            ]



            for future in as_completed(futures):


                results.append(

                    future.result()

                )





        successful_latencies = [


            r["latency_ms"]


            for r in results


            if r["success"]

            and r["latency_ms"] is not None


        ]




        failed = (

            requests_count

            -

            len(successful_latencies)

        )





        if successful_latencies:



            avg_latency = round(

                statistics.mean(

                    successful_latencies

                ),

                2

            )



            p50 = round(

                statistics.median(

                    successful_latencies

                ),

                2

            )



            p95 = round(

                percentile(

                    successful_latencies,

                    95

                ),

                2

            )



            p99 = round(

                percentile(

                    successful_latencies,

                    99

                ),

                2

            )



            chart_values.append(

                (

                    concurrency,

                    avg_latency

                )

            )



        else:



            avg_latency = None

            p50 = None

            p95 = None

            p99 = None





        result = {


            "concurrency":

                concurrency,



            "requests":

                requests_count,



            "successful":

                len(successful_latencies),



            "failed":

                failed,



            "avg_latency_ms":

                avg_latency,



            "p50_ms":

                p50,



            "p95_ms":

                p95,



            "p99_ms":

                p99

        }



        print(result)



        csv_results.append(result)





    write_csv(

        "latency_under_load.csv",

        csv_results

    )



    plot_latency_under_load(

        chart_values

    )



    return csv_results





def plot_latency_under_load(values):


    if not values:


        print(

            "No successful API requests. Chart not created."

        )


        return





    ensure_results_dir()



    x = [

        item[0]

        for item in values

    ]



    y = [

        item[1]

        for item in values

    ]





    plt.figure(

        figsize=(8,5)

    )



    plt.plot(

        x,

        y,

        marker="o"

    )



    plt.xlabel(

        "Concurrency Level"

    )



    plt.ylabel(

        "Average Latency (ms)"

    )



    plt.title(

        "Serving API Latency Under Load"

    )



    plt.grid(True)



    plt.tight_layout()



    plt.savefig(

        f"{RESULTS_DIR}/latency_chart.png",

        dpi=150

    )



    plt.close()
# ======================================================
# 3. EMR SPARK SPEEDUP BENCHMARK
# ======================================================


def submit_spark_job(partitions):


    print(

        f"Submitting Spark job with {partitions} partitions"

    )



    response = emr_client.add_job_flow_steps(


        JobFlowId=EMR_CLUSTER_ID,



        Steps=[


            {


                "Name":

                    f"spark-benchmark-{partitions}",



                "ActionOnFailure":

                    "CONTINUE",



                "HadoopJarStep": {


                    "Jar":

                        "command-runner.jar",



                    "Args": [


                        "spark-submit",



                        "--deploy-mode",

                        "cluster",



                        "--master",

                        "yarn",



                        "--conf",

                        "spark.yarn.submit.waitAppCompletion=true",



                        "--conf",

                        f"spark.default.parallelism={partitions}",



                        f"s3://{S3_BUCKET_NAME}/{EMR_BATCH_JOB_SCRIPT_S3_PATH}",



                        "--input",

                        f"s3://{S3_BUCKET_NAME}/{S3_RAW_PREFIX}",



                        "--output",

                        f"s3://{S3_BUCKET_NAME}/{S3_BATCH_OUTPUT_PREFIX}benchmark-{partitions}/",



                        "--partitions",

                        str(partitions)

                    ]

                }

            }

        ]

    )



    return response["StepIds"][0]






def wait_for_step(step_id):


    start = time.perf_counter()



    while True:



        response = emr_client.describe_step(


            ClusterId=EMR_CLUSTER_ID,


            StepId=step_id


        )



        state = response["Step"]["Status"]["State"]



        print(

            f"Step status: {state}"

        )



        if state in [


            "COMPLETED",


            "FAILED",


            "CANCELLED",


            "INTERRUPTED"


        ]:


            break



        time.sleep(15)



    return state, time.perf_counter() - start






def run_speedup_benchmark():



    print(

        "\n=== EMR SPEEDUP BENCHMARK ==="

    )



    partitions = [

        1,

        2,

        4,

        8

    ]



    results = []



    baseline = None



    for p in partitions:



        step_id = submit_spark_job(p)



        state, elapsed = wait_for_step(step_id)



        if baseline is None:


            baseline = elapsed




        speedup = (

            baseline / elapsed

            if elapsed > 0

            else 0

        )



        efficiency = (

            speedup / p

        )





        result = {


            "partitions":

                p,



            "state":

                state,



            "time_seconds":

                round(

                    elapsed,

                    2

                ),



            "speedup":

                round(

                    speedup,

                    3

                ),



            "efficiency":

                round(

                    efficiency,

                    3

                )

        }



        results.append(result)



        print(result)




    write_csv(

        "speedup.csv",

        results

    )



    return results





# ======================================================
# MAIN
# ======================================================


def main():



    parser = argparse.ArgumentParser(

        description=

        "Dublin Bus Analytics Benchmark"

    )



    parser.add_argument(

        "--api-url",

        default=

        "http://localhost:8080"

    )



    parser.add_argument(

        "--throughput",

        action="store_true"

    )



    parser.add_argument(

        "--latency",

        action="store_true"

    )



    parser.add_argument(

        "--speedup",

        action="store_true"

    )



    parser.add_argument(

        "--all",

        action="store_true"

    )



    args = parser.parse_args()



    ensure_results_dir()




    run_all = (


        args.all


        or


        not (

            args.throughput

            or

            args.latency

            or

            args.speedup

        )


    )





    if run_all or args.throughput:


        run_throughput_benchmark()





    if run_all or args.latency:


        run_latency_benchmark(

            args.api_url

        )





    if run_all or args.speedup:


        run_speedup_benchmark()





    print()



    print(

        "=" * 40

    )



    print(

        "Benchmark completed successfully"

    )



    print(

        "Results saved in benchmark_results/"

    )



    print(

        "=" * 40

    )






if __name__ == "__main__":


    main()