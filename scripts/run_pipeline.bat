@echo off
setlocal
set PYTHONPATH=%~dp0..\src
cd /d "%~dp0.."
echo === Banking Fraud Detection Pipeline ===
python -m fraud_detection.data_generation.generator
python -m fraud_detection.etl.pipeline --skip-postgres %*
echo === Pipeline complete ===
