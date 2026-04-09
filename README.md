# Prediction Market Calibration

## Project Goal
This project studies whether prediction-market-implied probabilities are well calibrated using public market data APIs, with Kalshi as the initial data source.

## Main Questions
- When a market implies 70% probability, does that outcome happen about 70% of the time?
- Which market categories appear more or less calibrated?
- Does calibration improve as markets approach resolution?

## Tech Stack
- Python
- requests
- pandas
- matplotlib

## Project Structure
- `src/`: code for data collection and analysis
- `data/raw/`: raw downloaded data
- `data/processed/`: cleaned datasets
- `results/`: charts and outputs
