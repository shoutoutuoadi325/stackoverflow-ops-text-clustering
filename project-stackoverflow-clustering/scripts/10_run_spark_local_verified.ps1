param(
    [string]$JavaHome = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot",
    [string]$Python = ".\.venv-spark\Scripts\python.exe",
    [string]$InputJson = "..\StackOverFlow_Oracle_Database\oracle_database_questions.json",
    [string]$OutputRoot = "output\spark_local_verified"
)

$ErrorActionPreference = "Stop"
$ProjectHome = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $ProjectHome

$env:JAVA_HOME = $JavaHome
$env:HADOOP_HOME = (Resolve-Path "tools\hadoop").Path
$env:hadoop_home_dir = $env:HADOOP_HOME
$env:PATH = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_SUBMIT_ARGS = "--master local[4] --driver-memory 8g pyspark-shell"

& $Python src\json_to_jsonl.py $InputJson output\questions.jsonl
& $Python src\eda.py --input output\questions.jsonl --input-format jsonl --output "$OutputRoot\eda" --shuffle-partitions 48
& $Python src\preprocess.py --input output\questions.jsonl --output "$OutputRoot\questions" --top-answers 3 --shuffle-partitions 48
& $Python src\feature_engineering.py --input "$OutputRoot\questions" --output "$OutputRoot\features" --num-features 262144 --shuffle-partitions 48

$env:PYSPARK_SUBMIT_ARGS = "--master local[4] --driver-memory 10g pyspark-shell"
& $Python src\cluster_lsh.py --features "$OutputRoot\features" --output "$OutputRoot\similar_pairs_sim075_ht4_b300" --distance-threshold 0.25 --similarity-threshold 0.75 --num-hash-tables 4 --max-bucket-size 300 --join-strategy approx --top-n-per-doc 10 --shuffle-partitions 48
& $Python src\cluster_lsh.py --features "$OutputRoot\features" --output "$OutputRoot\similar_pairs_sim085_ht4_b300" --distance-threshold 0.15 --similarity-threshold 0.85 --num-hash-tables 4 --max-bucket-size 300 --join-strategy approx --top-n-per-doc 10 --shuffle-partitions 48

$env:PYSPARK_SUBMIT_ARGS = "--master local[4] --driver-memory 6g pyspark-shell"
& $Python src\connected_components.py --questions "$OutputRoot\questions" --pairs "$OutputRoot\similar_pairs_sim075_ht4_b300" --output "$OutputRoot\clusters_sim075_ht4_b300" --iterations 8 --shuffle-partitions 48
& $Python src\connected_components.py --questions "$OutputRoot\questions" --pairs "$OutputRoot\similar_pairs_sim085_ht4_b300" --output "$OutputRoot\clusters_sim085_ht4_b300" --iterations 8 --shuffle-partitions 48

python src\summarize_spark_verified_outputs.py --pairs "$OutputRoot\similar_pairs_sim075_ht4_b300" --clusters "$OutputRoot\clusters_sim075_ht4_b300" --output-run-dir output\evaluation\sweep\spark_local_sim075_ht4_b300 --similarity-threshold 0.75 --distance-threshold 0.25 --num-hash-tables 4 --max-bucket-size 300
python src\summarize_spark_verified_outputs.py --pairs "$OutputRoot\similar_pairs_sim085_ht4_b300" --clusters "$OutputRoot\clusters_sim085_ht4_b300" --output-run-dir output\evaluation\sweep\spark_local_sim085_ht4_b300 --similarity-threshold 0.85 --distance-threshold 0.15 --num-hash-tables 4 --max-bucket-size 300
python src\summarize_sweep_results.py
python src\build_review_candidates.py
python src\evaluate_review_labels.py
python src\build_demo_assets.py
